// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Desk SQL integration tests (test-architecture-design §5.1).
//!
//! Runs against the live platform PostgreSQL (18) as the **real service
//! role** `trade8_desk_service` (NOBYPASSRLS — what the service uses once
//! per-service DSNs are enabled) plus `trade8_probe` for the no-context
//! RLS leg. Covers the store-layer contract unit tests cannot exercise:
//!   1. no tenant context → 0 rows (silent RLS enforcement)
//!   2. cross-env context → 0 rows
//!   3. `persist_order_accepted` writes order + event + outbox event in
//!      ONE explicit transaction (outbox-first, architecture-principles §6.7)
//!   4. overnight park → load → restore round-trip through the DB
//!
//! Context is transaction-local (`set_config(..., true)`) — every test
//! sets context inside a `conn.begin()` transaction and runs all
//! statements on that same tx (DESIGN.md §5, architecture-principles
//! §4.3d). Skips silently when the DB is unreachable so `cargo test`
//! stays hermetic offline (exchange8 `sql_matrix.rs` pattern).
//!
//! The desk crate is a binary (no lib target), so its modules are pulled
//! in via `#[path]` — the same pattern as bank8's payments tests.

#[path = "../src/borrow.rs"]
#[allow(dead_code)]
mod borrow;
#[path = "../src/catalog.rs"]
#[allow(dead_code)]
mod catalog;
#[path = "../src/engine.rs"]
#[allow(dead_code)]
mod engine;
#[path = "../src/session.rs"]
#[allow(dead_code)]
mod session;
#[path = "../src/store.rs"]
#[allow(dead_code)]
mod store;

use catalog::{DestinationRow, SymbolRow};
use engine::DeskEngine;
use sqlx::postgres::PgPoolOptions;
use sqlx::Acquire;
use trade8::contracts::{events, NewOrderRequest, Order, OrderState, OrderType, Side, TimeInForce};
use trade8::fixed::{Price, Qty};
use uuid::Uuid;

/// Real service role (NOBYPASSRLS, password from `vertical enroll`).
const DESK_URL: &str = "postgresql://trade8_desk_service@localhost:5432/trade8";
/// The probe role for the no-context leg (independent of service-role
/// grant drift).
const PROBE_URL: &str = "postgresql://trade8_probe@localhost:5432/trade8";

/// Well-known test envs (same convention as exchange8's matrix).
fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}
fn env_b() -> Uuid {
    Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
}

/// ONE connection per test — tenant context is connection-local;
/// context-set and data queries must ride the same connection
/// (architecture-principles.md §4.3d). `None` → skip (hermetic offline).
async fn desk_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(DESK_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

async fn probe_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(PROBE_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn catalog_fixture() -> (SymbolRow, DestinationRow) {
    // Name uniqueness per CALL (not per process): parallel tests each
    // seed their own symbol, and `catalog.symbol(name)` would otherwise
    // resolve an arbitrary duplicate.
    let run = Uuid::now_v7();
    (
        SymbolRow {
            symbol_id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            name: format!("APL{run}"),
            price_scale: 4,
            tick_rule_id: Uuid::new_v4(),
            feed_state: "act".into(),
            instrument_state: "active".into(),
            lot_size: 1,
        },
        DestinationRow {
            destination_id: Uuid::new_v4(),
            name: "BRK1".into(),
            min_size: 1,
            enable: true,
        },
    )
}

/// An engine-shaped `Order` in the Accepted state (the shape
/// `submit_with_actor` returns — persisted via `persist_order_accepted`).
fn accepted_order(client_order_id: &str, sym: &SymbolRow, dest: &DestinationRow) -> Order {
    static RUN: std::sync::OnceLock<Uuid> = std::sync::OnceLock::new();
    let run = *RUN.get_or_init(Uuid::now_v7);
    let client_order_id = format!("{client_order_id}-{run}");
    let qty = Qty::shares(100);
    Order {
        order_id: Uuid::now_v7(),
        client_order_id,
        chain_root_id: Uuid::now_v7(),
        replaces_order_id: None,
        usr_id: "u-sql".into(),
        office_id: "HO".into(),
        symbol_id: sym.symbol_id,
        symbol_name: sym.name.clone(),
        market_id: sym.market_id,
        destination_id: dest.destination_id,
        side: Side::Buy,
        order_type: OrderType::Limit,
        tif: TimeInForce::Day,
        qty,
        price: Some(Price::equity(100_500)), // 10.05 ×10⁴ — scale pinned in tests
        stop_price: None,
        state: OrderState::Accepted,
        leaves_qty: qty,
        filled_qty: Qty::shares(0),
        flags: serde_json::Value::Null,
        on_behalf_of: None,
        submitted_at: chrono::Utc::now(),
        terminal_at: None,
    }
}

fn new_req(
    client_order_id: &str,
    qty: &str,
    price: Option<&str>,
    dest_id: Uuid,
) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: client_order_id.into(),
        symbol_name: "AAPL".into(),
        destination_id: dest_id,
        side: Side::Buy,
        order_type: OrderType::Limit,
        tif: TimeInForce::Day,
        qty: qty.into(),
        price: price.map(str::to_string),
        stop_price: None,
        flags: None,
        on_behalf_of: None,
    }
}

/// Count `desk."order"` rows for a client_order_id under tenant context
/// (own transaction per the §8b contract).
async fn count_orders(conn: &mut sqlx::PgConnection, env: Uuid, client_order_id: &str) -> i64 {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let n: i64 =
        sqlx::query_scalar(r#"SELECT count(*) FROM desk."order" WHERE client_order_id = $1"#)
            .bind(client_order_id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    n
}

/// Delete the test's own rows (order + event + outbox event for one order id).
async fn cleanup_order(conn: &mut sqlx::PgConnection, env: Uuid, o: &Order) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let _ = sqlx::query(
        r#"DELETE FROM desk.outbox
           WHERE env_id = $1 AND payload->>'order_id' = $2"#,
    )
    .bind(env)
    .bind(o.order_id.to_string())
    .execute(&mut *tx)
    .await;
    let _ = sqlx::query(r#"DELETE FROM desk.order_event WHERE env_id = $1 AND order_id = $2"#)
        .bind(env)
        .bind(o.order_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.overnight_order WHERE env_id = $1 AND order_id = $2"#)
        .bind(env)
        .bind(o.order_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk."order" WHERE env_id = $1 AND order_id = $2"#)
        .bind(env)
        .bind(o.order_id)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
}

/// The FK chain behind `desk."order"`: office → trader_profile, and
/// market → symbol (+ destination). Seeded per test (fresh UUIDs),
/// removed in cleanup. (The live DB enforces FKs — the engine-only
/// fixtures never needed the referents; this is exactly what the
/// sql_matrix is for.)
async fn seed_referents(
    conn: &mut sqlx::PgConnection,
    env: Uuid,
    sym: &SymbolRow,
    dest: &DestinationRow,
    usr: &str,
    office: &str,
) {
    let broker_id = Uuid::new_v4();
    let gateway_id = Uuid::new_v4();
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    sqlx::query(
        r#"INSERT INTO desk.market (market_id, env_id, code, asset_class, timezone)
           VALUES ($1,$2,'MKT-' || $1::text,'equity','UTC') ON CONFLICT (market_id) DO NOTHING"#,
    )
    .bind(sym.market_id)
    .bind(env)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.symbol
           (symbol_id, env_id, market_id, name, currency_code, asset_class,
            lot_size, tick_rule_id, feed_state, instrument_state)
           VALUES ($1,$2,$3,$4,'USD','equity',1,$5,'act','active')
           ON CONFLICT (symbol_id) DO NOTHING"#,
    )
    .bind(sym.symbol_id)
    .bind(env)
    .bind(sym.market_id)
    .bind(&sym.name)
    .bind(sym.tick_rule_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.broker (broker_id, env_id, name)
           VALUES ($1,$2,'brk-' || $1::text) ON CONFLICT (broker_id) DO NOTHING"#,
    )
    .bind(broker_id)
    .bind(env)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.gateway (gateway_id, env_id, name)
           VALUES ($1,$2,'gw-' || $1::text) ON CONFLICT (gateway_id) DO NOTHING"#,
    )
    .bind(gateway_id)
    .bind(env)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.destination
           (destination_id, env_id, gateway_id, broker_id, name, protocol,
            endpoint_url, credentials_ref, currency_code, min_size, enable)
           VALUES ($1,$2,$3,$4,$5,'fix44','tcp://test:0','ref://t','USD',1,true)
           ON CONFLICT (destination_id) DO NOTHING"#,
    )
    .bind(dest.destination_id)
    .bind(env)
    .bind(gateway_id)
    .bind(broker_id)
    .bind(format!("{}-{}", dest.name, dest.destination_id.simple()))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.office (office_id, env_id, name)
           VALUES ($1,$2,$3) ON CONFLICT (office_id) DO NOTHING"#,
    )
    .bind(office)
    .bind(env)
    .bind(format!("Office {office}"))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.trader_profile (usr_id, env_id, office_id, display_name)
           VALUES ($1,$2,$3,'SQL Test Trader') ON CONFLICT (usr_id) DO NOTHING"#,
    )
    .bind(usr)
    .bind(env)
    .bind(office)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// A throwaway destination row for helpers that only need the symbol
/// referent (borrow seeding) — never persisted.
fn dest_placeholder() -> DestinationRow {
    DestinationRow {
        destination_id: Uuid::new_v4(),
        name: "unused".into(),
        min_size: 1,
        enable: true,
    }
}

/// Remove the referents seeded by `seed_referents` (idempotent).
async fn cleanup_referents(
    conn: &mut sqlx::PgConnection,
    env: Uuid,
    sym: &SymbolRow,
    dest: &DestinationRow,
    usr: &str,
    office: &str,
) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let _ = sqlx::query(r#"DELETE FROM desk.trader_profile WHERE env_id=$1 AND usr_id=$2"#)
        .bind(env)
        .bind(usr)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.office WHERE env_id=$1 AND office_id=$2"#)
        .bind(env)
        .bind(office)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.symbol WHERE env_id=$1 AND symbol_id=$2"#)
        .bind(env)
        .bind(sym.symbol_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.market WHERE env_id=$1 AND market_id=$2"#)
        .bind(env)
        .bind(sym.market_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.destination WHERE env_id=$1 AND destination_id=$2"#)
        .bind(env)
        .bind(dest.destination_id)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// RLS matrix (design §5.1)
// ---------------------------------------------------------------------------

/// RLS: no tenant context → 0 rows (silent enforcement, FORCE RLS).
#[tokio::test]
async fn rls_matrix_no_context_returns_zero() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let n: i64 = sqlx::query_scalar(r#"SELECT count(*) FROM desk."order""#)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(n, 0, "no tenant context must see zero rows");
}

/// RLS: cross-env context cannot read env-a's rows; env-a sees its own.
#[tokio::test]
async fn rls_matrix_cross_env_returns_zero() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let o = accepted_order("sql-x-env", &sym, &dest);
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o, "u-seed")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let foreign = count_orders(&mut conn, env_b(), &o.client_order_id).await;
    assert_eq!(foreign, 0, "env-b must not see env-a's order rows");
    let own = count_orders(&mut conn, env_a(), &o.client_order_id).await;
    assert_eq!(own, 1, "env-a must see its own row");
    cleanup_order(&mut conn, env_a(), &o).await;
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

// ---------------------------------------------------------------------------
// Outbox-first atomicity: persist_order_accepted
// ---------------------------------------------------------------------------

/// One `persist_order_accepted` call → order row (×10⁴ price), event row
/// (pending→accepted), outbox event row (`stream_family='desk'`) — all
/// three from one explicit transaction.
#[tokio::test]
async fn persist_order_accepted_triple_write() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let o = accepted_order("sql-triple", &sym, &dest);
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o, "u1")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    // Order row — the ×10⁴ price scale is pinned (design §6).
    let (state, price_px, qty, leaves): (String, Option<i64>, i64, i64) = sqlx::query_as(
        r#"SELECT state, price_px, qty, leaves_qty FROM desk."order"
           WHERE order_id = $1 AND env_id = $2"#,
    )
    .bind(o.order_id)
    .bind(env_a())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(state, "accepted");
    assert_eq!(price_px, Some(100_500), "price must persist at ×10⁴ scale");
    assert_eq!(qty, 100);
    assert_eq!(leaves, 100);
    // Event row: pending → accepted, actor + reason recorded.
    let (prev, new, actor, reason): (String, String, String, Option<String>) = sqlx::query_as(
        r#"SELECT prev_state, new_state, actor, reason FROM desk.order_event
           WHERE env_id = $1 AND order_id = $2"#,
    )
    .bind(env_a())
    .bind(o.order_id)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!((prev.as_str(), new.as_str()), ("pending", "accepted"));
    assert_eq!(actor, "u1");
    assert_eq!(reason.as_deref(), Some("submit"));
    // Inbox row: desk stream family + event type + full-order payload
    // (payload-completeness — a trigger must run from the payload alone).
    let (family, payload): (String, serde_json::Value) = sqlx::query_as(
        r#"SELECT stream_family, payload FROM desk.outbox
           WHERE env_id = $1 AND event_type = $2
             AND payload->>'order_id' = $3
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(env_a())
    .bind(events::ORDER_ACCEPTED)
    .bind(o.order_id.to_string())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        family, "desk",
        "outbox row must carry the desk stream family"
    );
    assert_eq!(payload["client_order_id"], o.client_order_id);
    assert_eq!(payload["usr_id"], "u-sql");
    tx.commit().await.unwrap();
    cleanup_order(&mut conn, env_a(), &o).await;
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

/// `persist_transition` (cancel): order row updated + journal appended +
/// outbox event row — all in one tx.
#[tokio::test]
async fn persist_transition_updates_and_journals() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let mut o = accepted_order("sql-transition", &sym, &dest);
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o, "u1")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    o.state = OrderState::Cancelled;
    o.leaves_qty = Qty::shares(0);
    o.terminal_at = Some(chrono::Utc::now());
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_transition(
            &mut tx,
            env_a(),
            &o,
            OrderState::Accepted,
            "cancel",
            "u1",
            events::ORDER_CANCELLED,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let (state, leaves): (String, i64) = sqlx::query_as(
        r#"SELECT state, leaves_qty FROM desk."order"
           WHERE order_id = $1 AND env_id = $2"#,
    )
    .bind(o.order_id)
    .bind(env_a())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(state, "cancelled");
    assert_eq!(leaves, 0);
    let journal: Vec<(String, String)> = sqlx::query_as(
        r#"SELECT prev_state, new_state FROM desk.order_event
           WHERE env_id = $1 AND order_id = $2 ORDER BY seq"#,
    )
    .bind(env_a())
    .bind(o.order_id)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(
        journal,
        vec![
            ("pending".to_string(), "accepted".to_string()),
            ("accepted".to_string(), "cancelled".to_string()),
        ],
        "journal must record both transitions in order"
    );
    let _ = new_req("unused", "1", None, dest.destination_id); // builder kept for parity with engine tests
    tx.commit().await.unwrap();
    cleanup_order(&mut conn, env_a(), &o).await;
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

// ---------------------------------------------------------------------------
// Overnight park → load → restore (design §5.1 #4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn overnight_park_restore_roundtrip() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let mut o = accepted_order("sql-overnight", &sym, &dest);
    o.tif = TimeInForce::Overnight;
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o, "u1")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // Sweep parks it (session-close path).
    o.state = OrderState::OvernightParked;
    o.terminal_at = Some(chrono::Utc::now());
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_swept(
            &mut tx,
            env_a(),
            &o,
            OrderState::Accepted,
            "session_close_park",
            events::ORDER_OVERNIGHT,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    // Load: the parked snapshot deserializes back to the engine Order.
    let parked: Vec<Order> = {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let rows = store::load_overnight_orders(&mut tx, env_a())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        rows.into_iter()
            .filter(|p| p.order_id == o.order_id)
            .collect()
    };
    assert_eq!(parked.len(), 1, "parked snapshot must round-trip");
    assert_eq!(parked[0].tif, TimeInForce::Overnight);
    assert_eq!(parked[0].qty.raw, 100);
    // Restore: stamp restored_order_id; a second load finds nothing.
    let restored = Order {
        order_id: Uuid::now_v7(),
        replaces_order_id: Some(o.order_id),
        state: OrderState::Pending,
        terminal_at: None,
        ..parked[0].clone()
    };
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_restored(&mut tx, env_a(), o.order_id, &restored)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let again: Vec<Order> = {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let rows = store::load_overnight_orders(&mut tx, env_a())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        rows.into_iter()
            .filter(|p| p.order_id == o.order_id)
            .collect()
    };
    assert!(again.is_empty(), "restored row must not load again");
    cleanup_order(&mut conn, env_a(), &o).await;
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

// ---------------------------------------------------------------------------
// Blotter scope filter + open-order count (design §5.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blotter_scope_filter() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let o1 = accepted_order("sql-bltr-1", &sym, &dest);
    let mut o2 = accepted_order("sql-bltr-2", &sym, &dest);
    o2.usr_id = "u-other".into();
    // u-other needs its own trader_profile row (order_usr_id_fkey).
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        sqlx::query(
            r#"INSERT INTO desk.trader_profile (usr_id, env_id, office_id, display_name)
               VALUES ('u-other',$1,'HO','Other Trader') ON CONFLICT (usr_id) DO NOTHING"#,
        )
        .bind(env_a())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o1, "u1")
            .await
            .unwrap();
        store::persist_order_accepted(&mut tx, env_a(), &o2, "u2")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // Office scope: both traders of office HO are visible.
    let rows = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::blotter_rows(&mut tx, env_a(), "u-sql", "HO", 50)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert!(rows.iter().any(|r| r.order_id == o1.order_id));
    assert!(rows.iter().any(|r| r.usr_id == "u-other"));
    // Cross-env: env-b sees neither.
    let none = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::blotter_rows(&mut tx, env_b(), "u-sql", "HO", 50)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert!(none.is_empty(), "cross-env blotter must be empty");
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        let _ =
            sqlx::query(r#"DELETE FROM desk.outbox WHERE env_id=$1 AND payload->>'order_id'=$2"#)
                .bind(env_a())
                .bind(o1.order_id.to_string())
                .execute(&mut *tx)
                .await;
        let _ = sqlx::query(r#"DELETE FROM desk.order_event WHERE env_id=$1 AND order_id=$2"#)
            .bind(env_a())
            .bind(o1.order_id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM desk."order" WHERE env_id=$1 AND order_id=$2"#)
            .bind(env_a())
            .bind(o1.order_id)
            .execute(&mut *tx)
            .await;
        let _ =
            sqlx::query(r#"DELETE FROM desk.outbox WHERE env_id=$1 AND payload->>'order_id'=$2"#)
                .bind(env_a())
                .bind(o2.order_id.to_string())
                .execute(&mut *tx)
                .await;
        let _ = sqlx::query(r#"DELETE FROM desk.order_event WHERE env_id=$1 AND order_id=$2"#)
            .bind(env_a())
            .bind(o2.order_id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM desk."order" WHERE env_id=$1 AND order_id=$2"#)
            .bind(env_a())
            .bind(o2.order_id)
            .execute(&mut *tx)
            .await;
        let _ =
            sqlx::query(r#"DELETE FROM desk.trader_profile WHERE env_id=$1 AND usr_id='u-other'"#)
                .bind(env_a())
                .execute(&mut *tx)
                .await;
        tx.commit().await.unwrap();
    }
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

#[tokio::test]
async fn open_order_count_by_state() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    let o = accepted_order("sql-count", &sym, &dest);
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &o, "u1")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let n = {
        let mut tx = conn.begin().await.unwrap();
        let v = store::open_order_count(&mut tx, env_a(), "u-sql")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        v
    };
    assert!(n >= 1, "accepted orders count as working");
    cleanup_order(&mut conn, env_a(), &o).await;
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

// ---------------------------------------------------------------------------
// Catalog + borrow bootstrap (design §5.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_bootstrap_loads_engine() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    // seed_referents covers market (symbol's FK parent) + symbol + destination.
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
    // Seed the tick-rule bands under env-a.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    // Tick bands JSONB [{price_lt, tick_size}] — parsed by the loader.
    sqlx::query(
        r#"INSERT INTO desk.tick_rule (tick_rule_id, env_id, description, bands)
           VALUES ($1,$2,'sqltest-' || $1::text,'[{"price_lt":1000000000,"tick_size":100}]'::jsonb)
           ON CONFLICT (tick_rule_id) DO NOTHING"#,
    )
    .bind(sym.tick_rule_id)
    .bind(env_a())
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    // Bootstrap → engine populated. The loaders set tenant context —
    // transaction-local, so they MUST run inside a tx to see seeded rows
    // (design §8b — the autocommit call-site bug this suite documents).
    let engine = DeskEngine::new();
    {
        let mut tx = conn.begin().await.unwrap();
        store::bootstrap_catalog(&mut tx, env_a(), &engine).await;
        tx.commit().await.unwrap();
    }
    let loaded = engine.catalog.symbol(&sym.name).expect("symbol loaded");
    assert_eq!(loaded.symbol_id, sym.symbol_id);
    assert_eq!(loaded.price_scale, 4);
    let d = engine
        .catalog
        .destination(dest.destination_id)
        .expect("destination loaded");
    assert!(d.enable);

    // Cleanup the reference rows.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let _ = sqlx::query(r#"DELETE FROM desk.tick_rule WHERE env_id=$1 AND tick_rule_id=$2"#)
        .bind(env_a())
        .bind(sym.tick_rule_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.destination WHERE env_id=$1 AND destination_id=$2"#)
        .bind(env_a())
        .bind(dest.destination_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.symbol WHERE env_id=$1 AND symbol_id=$2"#)
        .bind(env_a())
        .bind(sym.symbol_id)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
    cleanup_referents(&mut conn, env_a(), &sym, &dest, "u-sql", "HO").await;
}

#[tokio::test]
async fn borrow_bootstrap_and_mutation() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, _dest) = catalog_fixture();
    // Seed market + symbol first (short_pool FKs the symbol).
    seed_referents(&mut conn, env_a(), &sym, &dest_placeholder(), "u-sql", "HO").await;
    // Seed pool + ETB classification under env-a.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    sqlx::query(
        r#"INSERT INTO desk.short_pool (env_id, symbol_id, total_shares, available_shares)
           VALUES ($1,$2,500,500)
           ON CONFLICT (env_id, symbol_id) DO UPDATE SET total_shares = 500, available_shares = 500"#,
    )
    .bind(env_a())
    .bind(sym.symbol_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.borrow_list (env_id, symbol_id, category)
           VALUES ($1,$2,'etb') ON CONFLICT (env_id, symbol_id, category) DO NOTHING"#,
    )
    .bind(env_a())
    .bind(sym.symbol_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let engine = DeskEngine::new();
    {
        // Context is transaction-local — run the loader inside a tx (§8b).
        let mut tx = conn.begin().await.unwrap();
        store::bootstrap_borrow(&mut tx, env_a(), &engine)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(engine.borrow.available(sym.symbol_id), 500);
    assert!(engine.borrow.is_etb(sym.symbol_id));

    // Mutation: pool top-up + `borrow:updated` outbox event in one tx.
    let m = store::BorrowMutation {
        symbol_id: sym.symbol_id,
        total_shares: Some(700),
        office_id: None,
        category: None,
        exempt: None,
    };
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_borrow_mutation(&mut tx, env_a(), &engine, &m, "admin")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(engine.borrow.available(sym.symbol_id), 700);
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let events_: i64 = sqlx::query_scalar(
        r#"SELECT count(*) FROM desk.outbox
           WHERE env_id = $1 AND event_type = $2"#,
    )
    .bind(env_a())
    .bind(events::BORROW_UPDATED)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(events_ >= 1, "borrow mutation must emit the outbox event");
    tx.commit().await.unwrap();

    // Cleanup.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let _ = sqlx::query(
        r#"DELETE FROM desk.outbox WHERE env_id=$1 AND event_type=$2 AND payload->>'symbol_id'=$3"#,
    )
    .bind(env_a())
    .bind(events::BORROW_UPDATED)
    .bind(sym.symbol_id.to_string())
    .execute(&mut *tx)
    .await;
    let _ = sqlx::query(r#"DELETE FROM desk.short_pool WHERE env_id=$1 AND symbol_id=$2"#)
        .bind(env_a())
        .bind(sym.symbol_id)
        .execute(&mut *tx)
        .await;
    let _ = sqlx::query(r#"DELETE FROM desk.borrow_list WHERE env_id=$1 AND symbol_id=$2"#)
        .bind(env_a())
        .bind(sym.symbol_id)
        .execute(&mut *tx)
        .await;
    tx.commit().await.unwrap();
    cleanup_referents(&mut conn, env_a(), &sym, &dest_placeholder(), "u-sql", "HO").await;
}
