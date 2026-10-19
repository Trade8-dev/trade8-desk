// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Desk DB persistence.
//!
//! Every order mutation writes `desk."order"` + `desk.order_event` + a
//! `desk.outbox` row **in the same transaction** (outbox-first,
//! architecture-principles.md §6.7: the shell's boot-step-8b relay drains
//! the local outbox to Bus `POST /v1/produce`; no shared inbox).
//!
//! Catalog bootstrap loads the reference tables into the arc-swap
//! snapshot at startup (desk §4.3). RLS: every query runs on the same
//! connection the tenant context was set on, with explicit
//! `WHERE env_id = $N` defense-in-depth from the token claims.

use sqlx::PgConnection;
use trade8::contracts::{events, Order, OrderState, OrderSummary, Side, TimeInForce};
use trade8::fixed::{Price, Qty};
use uuid::Uuid;

use crate::catalog::{DestinationRow, SymbolRow, SymbolRowRaw, TickBand};
use crate::engine::DeskEngine;

/// Wire/DB encoding helpers - the serde renames are the stored values
/// (enums.rs keeps the codes byte-true).
fn side_str(s: Side) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "b".into())
}

fn side_from(s: &str) -> Option<Side> {
    match s {
        "b" => Some(Side::Buy),
        "s" => Some(Side::Sell),
        "t" => Some(Side::Trade),
        _ => None,
    }
}

fn state_str(s: OrderState) -> String {
    serde_json::to_value(s)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "pending".into())
}

fn state_from(s: &str) -> OrderState {
    match s {
        "accepted" => OrderState::Accepted,
        "routed" => OrderState::Routed,
        "partially_filled" => OrderState::PartiallyFilled,
        "filled" => OrderState::Filled,
        "cancelled" => OrderState::Cancelled,
        "replaced" => OrderState::Replaced,
        "rejected" => OrderState::Rejected,
        "expired" => OrderState::Expired,
        "overnight_parked" => OrderState::OvernightParked,
        _ => OrderState::Pending,
    }
}

fn tif_str(t: TimeInForce) -> String {
    serde_json::to_value(t)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "0".into())
}

fn tif_from(s: &str) -> TimeInForce {
    match s {
        "0" => TimeInForce::Day,
        "1" => TimeInForce::Gtc,
        "3" => TimeInForce::Ioc,
        "4" => TimeInForce::Fok,
        "6" => TimeInForce::Gtd,
        "ON" => TimeInForce::Overnight,
        "OC" => TimeInForce::OnClose,
        _ => TimeInForce::Day,
    }
}

// ---------------------------------------------------------------------------
// Bootstrap: load reference tables into the catalog snapshot.
// Runs at startup on a fresh connection with the service's own env id
// (boot context - the M2M token's env; runs once before serving).
// ---------------------------------------------------------------------------

/// Load catalogs for one env into the engine's arc-swap snapshot.
/// Failure is logged, not fatal - the service still serves health and
/// admin (a reload event can repair the cache later).
pub async fn bootstrap_catalog(conn: &mut PgConnection, env_id: Uuid, engine: &DeskEngine) {
    if let Err(e) = bootstrap_catalog_inner(conn, env_id, engine).await {
        tracing::warn!(error = %e, "trade8:desk: catalog bootstrap failed (empty catalog)");
    }
}

async fn bootstrap_catalog_inner(
    conn: &mut PgConnection,
    env_id: Uuid,
    engine: &DeskEngine,
) -> anyhow::Result<()> {
    sentin8::shell::set_tenant_context(conn, env_id).await;

    // Symbols (+ future price scale when present). The SQL row is a tuple;
    // the catalog's `SymbolRow` struct is built from it inline.
    let symbols: Vec<SymbolRowRaw> = sqlx::query_as(
        r#"SELECT s.symbol_id, s.market_id, s.name,
                  COALESCE(f.price_scale, 4)::int,
                  s.tick_rule_id, s.feed_state, s.instrument_state, s.lot_size
           FROM desk.symbol s
           LEFT JOIN desk.future f ON f.symbol_id = s.symbol_id
           WHERE s.env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    engine
        .catalog
        .load_symbols(symbols.into_iter().map(SymbolRow::from).collect());

    // Destinations.
    let dests: Vec<(Uuid, String, i64, bool)> = sqlx::query_as(
        r#"SELECT destination_id, name, min_size, enable
           FROM desk.destination WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    engine.catalog.load_destinations(
        dests
            .into_iter()
            .map(|(destination_id, name, min_size, enable)| DestinationRow {
                destination_id,
                name,
                min_size,
                enable,
            })
            .collect(),
    );

    // Tick rules (bands JSONB → Vec<TickBand>).
    let rules: Vec<(Uuid, serde_json::Value)> =
        sqlx::query_as(r#"SELECT tick_rule_id, bands FROM desk.tick_rule WHERE env_id = $1"#)
            .bind(env_id)
            .fetch_all(&mut *conn)
            .await?;
    for (rule_id, bands) in rules {
        let parsed: Vec<TickBand> = bands
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|b| {
                        Some(TickBand {
                            price_lt: b.get("price_lt")?.as_i64()? as i128,
                            tick_size: b.get("tick_size")?.as_i64()? as i128,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        engine.catalog.load_tick_rules(rule_id, parsed);
    }

    // Trader→office (trader_profile).
    let offices: Vec<(String, String)> =
        sqlx::query_as(r#"SELECT usr_id, office_id FROM desk.trader_profile WHERE env_id = $1"#)
            .bind(env_id)
            .fetch_all(&mut *conn)
            .await?;
    engine.catalog.load_trader_offices(offices);

    Ok(())
}

// ---------------------------------------------------------------------------
// Order persistence: order + event + outbox event in ONE transaction.
// ---------------------------------------------------------------------------

/// Persist a newly accepted order: INSERT order row, INSERT order_event,
/// INSERT outbox event row (`trade8:desk:order:accepted`) - same tx.
pub async fn persist_order_accepted(
    conn: &mut PgConnection,
    env_id: Uuid,
    o: &Order,
    actor: &str,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    insert_order_row(conn, env_id, o).await?;
    insert_event_row(
        conn,
        env_id,
        o.order_id,
        OrderState::Pending,
        OrderState::Accepted,
        actor,
        "submit",
    )
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::ORDER_ACCEPTED,
        serde_json::to_value(o).unwrap_or_default(),
    )
    .await
}

/// Persist a state transition (cancel/replace/park/restore): UPDATE order,
/// INSERT event, INSERT outbox - same tx.
pub async fn persist_transition(
    conn: &mut PgConnection,
    env_id: Uuid,
    o: &Order,
    prev: OrderState,
    reason: &str,
    actor: &str,
    event_type: &str,
) -> Result<(), sqlx::Error> {
    update_order_row(conn, env_id, o).await?;
    insert_event_row(conn, env_id, o.order_id, prev, o.state, actor, reason).await?;
    insert_outbox_event(
        conn,
        env_id,
        event_type,
        serde_json::to_value(o).unwrap_or_default(),
    )
    .await
}

async fn insert_order_row(
    conn: &mut PgConnection,
    env_id: Uuid,
    o: &Order,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO desk."order"
           (order_id, env_id, client_order_id, chain_root_id, replaces_order_id,
            usr_id, office_id, symbol_id, destination_id, side, order_type, tif,
            qty, price_px, stop_px, price_scale, qty_scale, leaves_qty, filled_qty,
            flags, on_behalf_of, state, submitted_at, terminal_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)"#,
    )
    .bind(o.order_id)
    .bind(env_id)
    .bind(&o.client_order_id)
    .bind(o.chain_root_id)
    .bind(o.replaces_order_id)
    .bind(&o.usr_id)
    .bind(&o.office_id)
    .bind(o.symbol_id)
    .bind(o.destination_id)
    .bind(side_str(o.side))
    .bind(trade8::contracts::OrderType::serde_str(o.order_type))
    .bind(tif_str(o.tif))
    .bind(o.qty.raw as i64)
    .bind(o.price.map(|p| p.raw as i64))
    .bind(o.stop_price.map(|p| p.raw as i64))
    .bind(o.qty.scale as i16)
    .bind(o.qty.scale as i16)
    .bind(o.leaves_qty.raw as i64)
    .bind(o.filled_qty.raw as i64)
    .bind(&o.flags)
    .bind(&o.on_behalf_of)
    .bind(state_str(o.state))
    .bind(o.submitted_at)
    .bind(o.terminal_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn update_order_row(
    conn: &mut PgConnection,
    env_id: Uuid,
    o: &Order,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE desk."order"
           SET state = $3, leaves_qty = $4, filled_qty = $5, terminal_at = $6
           WHERE order_id = $1 AND env_id = $2"#,
    )
    .bind(o.order_id)
    .bind(env_id)
    .bind(state_str(o.state))
    .bind(o.leaves_qty.raw as i64)
    .bind(o.filled_qty.raw as i64)
    .bind(o.terminal_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn insert_event_row(
    conn: &mut PgConnection,
    env_id: Uuid,
    order_id: Uuid,
    prev: OrderState,
    new: OrderState,
    actor: &str,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO desk.order_event
           (env_id, order_id, prev_state, new_state, actor, reason)
           VALUES ($1,$2,$3,$4,$5,$6)"#,
    )
    .bind(env_id)
    .bind(order_id)
    .bind(state_str(prev))
    .bind(state_str(new))
    .bind(actor)
    .bind(reason)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Append a desk event to the local outbox (`desk.outbox`) - same tx as
/// the domain change (outbox-first, architecture-principles §6.7). The
/// shell's boot-step-8b relay drains it to Bus `POST /v1/produce` (M2M,
/// `sentin8:bus:produce`, idempotency key = the outbox row id); a Bus
/// outage leaves the row unrelayed and retries - never silently dropped.
pub async fn insert_outbox_event(
    conn: &mut PgConnection,
    env_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let row_id =
        sentin8::bus::outbox::insert_outbox_row(&mut *conn, env_id, "desk", event_type, &payload)
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(other.to_string().into()),
            })?;
    tracing::debug!(outbox_id = %row_id, event_type, "trade8:desk: outbox row appended");
    Ok(())
}

// ---------------------------------------------------------------------------
// Session-close sweep + overnight restore
// ---------------------------------------------------------------------------

/// Persist one swept order (cancel or park) + its journal row. Called
/// from the sweep loop with the already-mutated engine order.
pub async fn persist_swept(
    conn: &mut PgConnection,
    env_id: Uuid,
    o: &Order,
    prev: OrderState,
    reason: &str,
    event_type: &str,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    update_order_row(conn, env_id, o).await?;
    insert_event_row(conn, env_id, o.order_id, prev, o.state, "system", reason).await?;
    if o.state == OrderState::OvernightParked {
        // Park snapshot: full order snapshot into overnight_order.
        sqlx::query(
            r#"INSERT INTO desk.overnight_order
               (order_id, env_id, source_order_id, parked_date, snapshot)
               VALUES ($1,$2,$3, CURRENT_DATE, $4)
               ON CONFLICT (order_id) DO NOTHING"#,
        )
        .bind(o.order_id)
        .bind(env_id)
        .bind(o.replaces_order_id.unwrap_or(o.order_id))
        .bind(serde_json::to_value(o).unwrap_or_default())
        .execute(&mut *conn)
        .await?;
    }
    insert_outbox_event(
        conn,
        env_id,
        event_type,
        serde_json::to_value(o).unwrap_or_default(),
    )
    .await
}

/// Load parked overnight orders for restore: rows for this env
/// parked today or earlier with no restored_order_id yet.
pub async fn load_overnight_orders(
    conn: &mut PgConnection,
    env_id: Uuid,
) -> Result<Vec<Order>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
        r#"SELECT snapshot FROM desk.overnight_order
           WHERE env_id = $1 AND restored_order_id IS NULL"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(snap,)| serde_json::from_value(snap).ok())
        .collect())
}

/// Mark a parked row restored + `overnight:restored` event.
pub async fn persist_restored(
    conn: &mut PgConnection,
    env_id: Uuid,
    parked_id: Uuid,
    restored: &Order,
) -> Result<(), sqlx::Error> {
    // context is transaction-local: set it on the executor this fn receives (DESIGN.md §5).
    sentin8::shell::set_tenant_context(conn, env_id).await;
    sqlx::query(
        r#"UPDATE desk.overnight_order
           SET restored_order_id = $3 WHERE order_id = $1 AND env_id = $2"#,
    )
    .bind(parked_id)
    .bind(env_id)
    .bind(restored.order_id)
    .execute(&mut *conn)
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::ORDER_OVERNIGHT_RESTORED,
        serde_json::json!({
            "parked_order_id": parked_id,
            "restored_order_id": restored.order_id,
            "usr_id": restored.usr_id,
        }),
    )
    .await
}

// ---------------------------------------------------------------------------
// Borrow pool bootstrap + CRUD
// ---------------------------------------------------------------------------

/// Load the borrow pool from the reference tables into the engine.
/// Short-locate is on the accept hot path - the pool must be resident.
pub async fn bootstrap_borrow(
    conn: &mut PgConnection,
    env_id: Uuid,
    engine: &DeskEngine,
) -> anyhow::Result<()> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let pools: Vec<(Uuid, i64, i64)> = sqlx::query_as(
        r#"SELECT symbol_id, total_shares, available_shares
           FROM desk.short_pool WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    for (symbol_id, total, _available) in pools {
        engine.borrow.set_pool(symbol_id, total);
    }
    let blocks: Vec<(Uuid, String, Option<i64>)> = sqlx::query_as(
        r#"SELECT symbol_id, office_id, total_shares
           FROM desk.short_pool_office_block WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *conn)
    .await?;
    for (symbol_id, office_id, total) in blocks {
        engine
            .borrow
            .set_office_block(symbol_id, &office_id, total.unwrap_or(0));
    }
    let lists: Vec<(Uuid, String)> =
        sqlx::query_as(r#"SELECT symbol_id, category FROM desk.borrow_list WHERE env_id = $1"#)
            .bind(env_id)
            .fetch_all(&mut *conn)
            .await?;
    for (symbol_id, category) in lists {
        let class = match category.as_str() {
            "etb" => crate::borrow::BorrowClass::Easy,
            _ => crate::borrow::BorrowClass::Hard,
        };
        engine.borrow.classify(symbol_id, class);
    }
    let exempt: Vec<(Uuid,)> =
        sqlx::query_as(r#"SELECT symbol_id FROM desk.short_exempt WHERE env_id = $1"#)
            .bind(env_id)
            .fetch_all(&mut *conn)
            .await?;
    for (symbol_id,) in exempt {
        engine
            .borrow
            .classify(symbol_id, crate::borrow::BorrowClass::Exempt);
    }
    Ok(())
}

/// Borrow CRUD row (admin + risk actions).
#[derive(serde::Deserialize)]
pub struct BorrowMutation {
    pub symbol_id: Uuid,
    pub total_shares: Option<i64>,
    pub office_id: Option<String>,
    pub category: Option<String>,
    pub exempt: Option<bool>,
}

/// Apply one borrow mutation: DB write + engine swap + `borrow:updated`
/// outbox event - same tx.
pub async fn persist_borrow_mutation(
    conn: &mut PgConnection,
    env_id: Uuid,
    engine: &DeskEngine,
    m: &BorrowMutation,
    actor: &str,
) -> Result<(), sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    if let Some(total) = m.total_shares {
        // Office-scoped block when office_id present, else the env pool.
        if let Some(office) = m.office_id.as_deref() {
            sqlx::query(
                r#"INSERT INTO desk.short_pool_office_block
                   (env_id, symbol_id, office_id, total_shares, available_shares)
                   VALUES ($1,$2,$3,$4,$4)
                   ON CONFLICT (env_id, symbol_id, office_id)
                   DO UPDATE SET total_shares = EXCLUDED.total_shares,
                                 available_shares = EXCLUDED.available_shares"#,
            )
            .bind(env_id)
            .bind(m.symbol_id)
            .bind(office)
            .bind(total)
            .execute(&mut *conn)
            .await?;
            engine.borrow.set_office_block(m.symbol_id, office, total);
        } else {
            sqlx::query(
                r#"INSERT INTO desk.short_pool
                   (env_id, symbol_id, total_shares, available_shares)
                   VALUES ($1,$2,$3,$3)
                   ON CONFLICT (env_id, symbol_id)
                   DO UPDATE SET total_shares = EXCLUDED.total_shares,
                                 available_shares = EXCLUDED.available_shares"#,
            )
            .bind(env_id)
            .bind(m.symbol_id)
            .bind(total)
            .execute(&mut *conn)
            .await?;
            engine.borrow.set_pool(m.symbol_id, total);
        }
    }
    if let Some(category) = m.category.as_deref() {
        let cat = if category == "etb" { "etb" } else { "htb" };
        sqlx::query(
            r#"INSERT INTO desk.borrow_list (env_id, symbol_id, category)
               VALUES ($1,$2,$3)
               ON CONFLICT (env_id, symbol_id, category) DO NOTHING"#,
        )
        .bind(env_id)
        .bind(m.symbol_id)
        .bind(cat)
        .execute(&mut *conn)
        .await?;
        let class = if cat == "etb" {
            crate::borrow::BorrowClass::Easy
        } else {
            crate::borrow::BorrowClass::Hard
        };
        engine.borrow.classify(m.symbol_id, class);
    }
    if let Some(exempt) = m.exempt {
        if exempt {
            sqlx::query(
                r#"INSERT INTO desk.short_exempt (env_id, symbol_id)
                   VALUES ($1,$2) ON CONFLICT (env_id, symbol_id) DO NOTHING"#,
            )
            .bind(env_id)
            .bind(m.symbol_id)
            .execute(&mut *conn)
            .await?;
            engine
                .borrow
                .classify(m.symbol_id, crate::borrow::BorrowClass::Exempt);
        } else {
            sqlx::query(
                r#"DELETE FROM desk.short_exempt
                   WHERE env_id = $1 AND symbol_id = $2"#,
            )
            .bind(env_id)
            .bind(m.symbol_id)
            .execute(&mut *conn)
            .await?;
        }
    }
    insert_outbox_event(
        conn,
        env_id,
        events::BORROW_UPDATED,
        serde_json::json!({
            "symbol_id": m.symbol_id, "actor": actor,
            "total_shares": m.total_shares, "category": m.category, "exempt": m.exempt,
        }),
    )
    .await
}

// ---------------------------------------------------------------------------
// Reads: DB-backed blotter + order detail with explicit env filter.
// ---------------------------------------------------------------------------

/// Row shape for the read paths (order + symbol name join).
#[derive(sqlx::FromRow)]
struct OrderReadRow {
    order_id: Uuid,
    client_order_id: String,
    symbol_name: Option<String>,
    usr_id: String,
    office_id: String,
    side: String,
    state: String,
    qty: i64,
    leaves_qty: i64,
    filled_qty: i64,
    price_px: Option<i64>,
    price_scale: i16,
    tif: String,
    submitted_at: chrono::DateTime<chrono::Utc>,
}

fn row_to_summary(r: OrderReadRow) -> OrderSummary {
    let side = side_from(&r.side).unwrap_or(Side::Buy);
    OrderSummary {
        order_id: r.order_id,
        client_order_id: r.client_order_id,
        symbol_name: r.symbol_name.unwrap_or_default(),
        side,
        state: state_from(&r.state),
        qty: Qty {
            raw: r.qty.max(0) as u64,
            scale: 0,
        },
        leaves_qty: Qty {
            raw: r.leaves_qty.max(0) as u64,
            scale: 0,
        },
        filled_qty: Qty {
            raw: r.filled_qty.max(0) as u64,
            scale: 0,
        },
        price: r
            .price_px
            .map(|p| Price::from_scaled(p as i128, r.price_scale.max(0) as u8)),
        usr_id: r.usr_id,
        office_id: r.office_id,
        submitted_at: r.submitted_at,
    }
}

/// DB blotter: own rows (self scope) or office rows (manager scope).
/// Explicit `WHERE env_id = $1 AND (usr_id = $2 OR office_id = $3)` -
/// defense-in-depth on top of RLS.
pub async fn blotter_rows(
    conn: &mut PgConnection,
    env_id: Uuid,
    usr_id: &str,
    office_id: &str,
    limit: i64,
) -> Result<Vec<OrderSummary>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let rows: Vec<OrderReadRow> = sqlx::query_as(
        r#"SELECT o.order_id, o.client_order_id, s.name AS symbol_name,
                  o.usr_id, o.office_id, o.side, o.state,
                  o.qty, o.leaves_qty, o.filled_qty, o.price_px, o.price_scale,
                  o.tif, o.submitted_at
           FROM desk."order" o
           JOIN desk.symbol s ON s.symbol_id = o.symbol_id
           WHERE o.env_id = $1 AND (o.usr_id = $2 OR o.office_id = $3)
           ORDER BY o.submitted_at DESC
           LIMIT $4"#,
    )
    .bind(env_id)
    .bind(usr_id)
    .bind(office_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(row_to_summary).collect())
}

/// Full order read row (detail + replay) - an explicit struct because the
/// 22-column shape exceeds sqlx's tuple FromRow limit (16).
#[derive(sqlx::FromRow)]
struct OrderFullRow {
    order_id: Uuid,
    client_order_id: String,
    chain_root_id: Uuid,
    replaces_order_id: Option<Uuid>,
    usr_id: String,
    office_id: String,
    symbol_id: Uuid,
    destination_id: Uuid,
    side: String,
    order_type: String,
    tif: String,
    qty: i64,
    price_px: Option<i64>,
    stop_px: Option<i64>,
    price_scale: i16,
    leaves_qty: i64,
    filled_qty: i64,
    flags: serde_json::Value,
    on_behalf_of: Option<String>,
    state: String,
    submitted_at: chrono::DateTime<chrono::Utc>,
    terminal_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Load an order's full row as the engine `Order` shape (detail + replay).
pub async fn load_order(
    conn: &mut PgConnection,
    env_id: Uuid,
    order_id: Uuid,
) -> Result<Option<Order>, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let row: Option<OrderFullRow> = sqlx::query_as(
        r#"SELECT o.order_id, o.client_order_id, o.chain_root_id, o.replaces_order_id,
                  o.usr_id, o.office_id, o.symbol_id, o.destination_id, o.side,
                  o.order_type, o.tif, o.qty, o.price_px, o.stop_px, o.price_scale,
                  o.leaves_qty, o.filled_qty, o.flags, o.on_behalf_of, o.state,
                  o.submitted_at, o.terminal_at
           FROM desk."order" o
           WHERE o.order_id = $1 AND o.env_id = $2"#,
    )
    .bind(order_id)
    .bind(env_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(r) = row else {
        return Ok(None);
    };
    let scale = r.price_scale.max(0) as u8;
    Ok(Some(Order {
        order_id: r.order_id,
        client_order_id: r.client_order_id,
        chain_root_id: r.chain_root_id,
        replaces_order_id: r.replaces_order_id,
        usr_id: r.usr_id,
        office_id: r.office_id,
        symbol_id: r.symbol_id,
        symbol_name: String::new(), // detail joins symbol server-side when needed
        market_id: Uuid::nil(),
        destination_id: r.destination_id,
        side: side_from(&r.side).unwrap_or(Side::Buy),
        order_type: trade8::contracts::OrderType::parse_wire(&r.order_type)
            .unwrap_or(trade8::contracts::OrderType::Limit),
        tif: tif_from(&r.tif),
        qty: Qty {
            raw: r.qty.max(0) as u64,
            scale: 0,
        },
        price: r.price_px.map(|p| Price::from_scaled(p as i128, scale)),
        stop_price: r.stop_px.map(|p| Price::from_scaled(p as i128, scale)),
        state: state_from(&r.state),
        leaves_qty: Qty {
            raw: r.leaves_qty.max(0) as u64,
            scale: 0,
        },
        filled_qty: Qty {
            raw: r.filled_qty.max(0) as u64,
            scale: 0,
        },
        flags: r.flags,
        on_behalf_of: r.on_behalf_of,
        submitted_at: r.submitted_at,
        terminal_at: r.terminal_at,
    }))
}

/// Working-order count for a trader (Risk max_open_orders join input).
pub async fn open_order_count(
    conn: &mut PgConnection,
    env_id: Uuid,
    usr_id: &str,
) -> Result<i64, sqlx::Error> {
    sentin8::shell::set_tenant_context(conn, env_id).await;
    let (n,): (i64,) = sqlx::query_as(
        r#"SELECT count(*) FROM desk."order"
           WHERE env_id = $1 AND usr_id = $2
             AND state IN ('accepted','routed','partially_filled')"#,
    )
    .bind(env_id)
    .bind(usr_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(n)
}

// ---------------------------------------------------------------------------
// Keystroke audit
// ---------------------------------------------------------------------------

/// Append a keystroke audit row + `keysroke:logged` outbox event (same tx).
pub async fn log_keystroke(
    conn: &mut PgConnection,
    env_id: Uuid,
    usr_id: &str,
    action: &str,
    order_id: Option<Uuid>,
    requested_by: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"INSERT INTO desk.keystroke_log
           (env_id, usr_id, action, order_id, requested_by)
           VALUES ($1,$2,$3,$4,$5)"#,
    )
    .bind(env_id)
    .bind(usr_id)
    .bind(action)
    .bind(order_id)
    .bind(requested_by)
    .execute(&mut *conn)
    .await?;
    insert_outbox_event(
        conn,
        env_id,
        events::KEYSTROKE_LOGGED,
        serde_json::json!({"usr_id": usr_id, "action": action, "order_id": order_id}),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_wire_strings() {
        assert_eq!(side_str(Side::Buy), "b");
        assert_eq!(side_str(Side::Sell), "s");
        assert_eq!(side_str(Side::Trade), "t");
        assert_eq!(state_str(OrderState::PartiallyFilled), "partially_filled");
        assert_eq!(state_from("overnight_parked"), OrderState::OvernightParked);
        assert_eq!(tif_str(TimeInForce::Overnight), "ON");
        assert_eq!(tif_from("ON"), TimeInForce::Overnight);
        assert_eq!(tif_from("3"), TimeInForce::Ioc);
    }
}
