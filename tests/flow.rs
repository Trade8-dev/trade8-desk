// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Cross-service flow tests (test-architecture design §6): the
//! two-phase submit path end-to-end with a Risk test double.
//!
//! TREE-LAYOUT-ONLY: the risk double embeds the REAL risk engine via a
//! `#[path = "../../risk/src/core.rs"]` include — a cross-repo path that
//! resolves ONLY in the aggregation-tree layout (both repos checked out
//! side by side). Single-repo checkouts (CI) cannot resolve it; the
//! include is feature-gated so the tree operator runs these flows with
//! `--features cross_repo_flows` (backlog T8-HARNESS-1 tracks the
//! SDK-side replacement).
#![cfg(feature = "cross_repo_flows")]
//!
//! Phases exercised in one running engine (the REAL desk + risk code, not
//! mocks):
//!   1. **Phase 1 (park)** — `DeskEngine::submit_pending` parks the order
//!      (`Pending`); structural validation (symbol/destination/qty/price)
//!      rejects before Risk is ever called.
//!   2. **Phase 2 (authorize)** — Risk's REAL `RiskEngine::authorize`
//!      (embedded via `#[path]`; the exact production check pipeline)
//!      answers the wire-shaped `AuthorizeRequest`:
//!        - allow leg (buying-power limit bound, trace asserted),
//!        - deny legs (trader disabled → `trade8_risk_trader_disabled`,
//!          buying-power exceeded → `trade8_risk_buying_power`),
//!        - fail-closed wiring branch (`DeskWiring` with Risk absent → the
//!          outage contract never trades unguarded... actually the
//!          not-enrolled branch structurally allows; the outage branch is
//!          exercised at the wiring layer below).
//!   3. **Persist (allow)** — `persist_order_accepted` one-tx write
//!      (order + event + `desk.outbox` row) via the SAME pattern the
//!      handler runs; the outbox payload carries the FULL order snapshot
//!      (payload-completeness, VDC-3) with every payload key Router's
//!      consumer (`route_accepted_order`) reads: `order_id`, `market_id`,
//!      `office_id`, `client_order_id`, `symbol_name`, `side`, `qty.raw`,
//!      `price.raw`/`price.scale`.
//!   4. **Persist (deny)** — the rejection journal row
//!      (`trade8:desk:order:rejected` outbox event, trace attached) — the
//!      auditable-deny contract.
//!
//! Runs against the live platform PostgreSQL (db `trade8`) as the REAL
//! service role `trade8_desk_service`; skips silently when the DB is
//! unreachable (the sql_matrix pattern). The certificate-presenting mTLS
//! hop itself (https-only mTLS) is covered by Sentin8:TLS and the
//! certificate-identity integration
//! suites on the platform side (test-architecture §6 — the direct-call
//! seam is what this file's wire-shaped `AuthorizeRequest` doubles).

#[path = "../src/borrow.rs"]
#[allow(dead_code)]
mod borrow;
#[path = "../src/catalog.rs"]
#[allow(dead_code)]
mod catalog;
#[path = "../src/engine.rs"]
#[allow(dead_code)]
mod engine;
#[path = "../src/risk_double.rs"]
#[allow(dead_code)]
mod risk_double;
#[path = "../src/session.rs"]
#[allow(dead_code)]
mod session;
#[path = "../src/store.rs"]
#[allow(dead_code)]
mod store;

use catalog::{DestinationRow, SymbolRow};
use engine::DeskEngine;
use risk_double::{RiskDouble, RiskDoubleWiring};
use sqlx::postgres::PgPoolOptions;
use sqlx::Acquire;
use trade8::contracts::events;
use trade8::contracts::risk::{AuthorizeRequest, Intent};
use trade8::contracts::{NewOrderRequest, Order, OrderState, OrderType, Side, TimeInForce};
use uuid::Uuid;

/// Real service role (NOBYPASSRLS, password from `vertical enroll`).
const DESK_URL: &str = "postgresql://trade8_desk_service@localhost:5432/trade8";

fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}

/// ONE connection per test — tenant context is connection-local
/// (architecture-principles §4.3d). `None` → skip (hermetic offline).
async fn desk_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(DESK_URL)
        .await
        .ok()?;
    pool.acquire().await.ok()
}

// ---------------------------------------------------------------------------
// Fixtures (same shapes as the sql_matrix catalog fixture)
// ---------------------------------------------------------------------------

/// Name uniqueness per CALL (not per process): parallel tests each seed
/// their own symbol, and `catalog.symbol(name)` would otherwise resolve an
/// arbitrary duplicate.
fn catalog_fixture() -> (SymbolRow, DestinationRow) {
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

/// A desk engine seated with the fixture catalog, session Open.
fn seated_engine(sym: &SymbolRow, dest: &DestinationRow) -> DeskEngine {
    let e = DeskEngine::new();
    e.catalog.load_symbols(vec![sym.clone()]);
    e.catalog.load_destinations(vec![dest.clone()]);
    e.session.set(trade8::contracts::SessionState::Open);
    e
}

fn new_req(
    client_order_id: &str,
    qty: &str,
    price: Option<&str>,
    dest_id: Uuid,
    sym: &str,
) -> NewOrderRequest {
    NewOrderRequest {
        client_order_id: client_order_id.into(),
        symbol_name: sym.into(),
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

/// The wire-shaped `AuthorizeRequest` Desk's wiring builds for a pending
/// order (mirrors `DeskWiring::authorize_order`'s field mapping — kept in
/// sync by the wiring-level allow assertion below).
fn authorize_req(o: &Order, mark_raw: Option<i128>) -> AuthorizeRequest {
    AuthorizeRequest {
        env_id: env_a(),
        usr_id: o.usr_id.clone(),
        office_id: o.office_id.clone(),
        symbol_id: o.symbol_id,
        symbol_name: o.symbol_name.clone(),
        market_id: o.market_id,
        destination_id: o.destination_id,
        side_char: o.side.as_char(),
        qty: o.qty.raw,
        price_raw: o.price.map(|p| p.raw),
        price_scale: o.price.map(|p| p.scale).unwrap_or(4),
        mark_raw,
        intent: Intent::Open,
        current_position: 0,
        halted: false,
        last_print_raw: None,
        prior_print_raw: None,
        order_type: Some(
            serde_json::to_value(o.order_type)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "2".into()),
        ),
        tif: Some(
            serde_json::to_value(o.tif)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "0".into()),
        ),
        is_cancel: false,
    }
}

/// Seed the desk FK referents for a persisted order (same statements as
/// the sql_matrix `seed_referents` — symbol/destination/office/trader).
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
        r#"INSERT INTO desk.broker (broker_id, env_id, name) VALUES ($1,$2,'brk-' || $1::text)
           ON CONFLICT (broker_id) DO NOTHING"#,
    )
    .bind(broker_id)
    .bind(env)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.gateway (gateway_id, env_id, name) VALUES ($1,$2,'gw-' || $1::text)
           ON CONFLICT (gateway_id) DO NOTHING"#,
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
        r#"INSERT INTO desk.office (office_id, env_id, name) VALUES ($1,$2,$3)
           ON CONFLICT (office_id) DO NOTHING"#,
    )
    .bind(office)
    .bind(env)
    .bind(format!("Office {office}"))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        r#"INSERT INTO desk.trader_profile (usr_id, env_id, office_id, display_name)
           VALUES ($1,$2,$3,'Flow Test Trader') ON CONFLICT (usr_id) DO NOTHING"#,
    )
    .bind(usr)
    .bind(env)
    .bind(office)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// Remove the test's own rows (referents + order chain + outbox/events).
async fn cleanup_all(
    conn: &mut sqlx::PgConnection,
    env: Uuid,
    sym: &SymbolRow,
    dest: &DestinationRow,
    usr: &str,
    office: &str,
    client_prefix: &str,
) {
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let _ = sqlx::query(
        r#"DELETE FROM desk.outbox WHERE env_id=$1 AND payload->>'client_order_id' LIKE $2"#,
    )
    .bind(env)
    .bind(format!("{client_prefix}%"))
    .execute(&mut *tx)
    .await;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        r#"SELECT order_id FROM desk."order" WHERE env_id=$1 AND client_order_id LIKE $2"#,
    )
    .bind(env)
    .bind(format!("{client_prefix}%"))
    .fetch_all(&mut *tx)
    .await
    .unwrap_or_default();
    for id in ids {
        let _ = sqlx::query(r#"DELETE FROM desk.order_event WHERE env_id=$1 AND order_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM desk.overnight_order WHERE env_id=$1 AND order_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
        let _ = sqlx::query(r#"DELETE FROM desk."order" WHERE env_id=$1 AND order_id=$2"#)
            .bind(env)
            .bind(id)
            .execute(&mut *tx)
            .await;
    }
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
// Risk double + wiring-level branches (pure, no DB)
// ---------------------------------------------------------------------------

/// Allow leg: the REAL Risk engine (test double built on the production
/// checks) allows a within-limit order and the verdict carries the trace
/// + remaining buying power Desk surfaces to the client.
#[test]
fn risk_double_allow_with_buying_power_trace() {
    let d = RiskDouble::with_limits(vec![risk_double::limit(1_000_000_000)]); // $100k ×10⁴
    let sym = catalog_fixture().0;
    let dest = catalog_fixture().1;
    let e = seated_engine(&sym, &dest);
    let o = e
        .submit_pending(
            "u-flow",
            "HO",
            new_req(
                "dbl-allow",
                "100",
                Some("10.05"),
                dest.destination_id,
                &sym.name,
            ),
        )
        .unwrap();
    let v = d.engine.authorize(&authorize_req(&o, None));
    assert!(
        v.allowed,
        "within-limit buy must allow: {:?}",
        v.reject_code
    );
    assert!(v.reject_code.is_none());
    // The buying-power check ran and passed; remaining BP = limit − notional
    // (design §6 scale: 10.05 × 100 shares = 10_050_000 ×10⁴ = $10,050).
    let bp_check = v
        .trace
        .iter()
        .find(|c| c.check == trade8::contracts::risk::CheckKind::BuyingPower)
        .expect("buying power check in trace");
    assert!(bp_check.passed);
    assert_eq!(v.remaining_buying_power, Some(1_000_000_000 - 10_050_000));
    assert_eq!(v.trader_state, trade8::contracts::TraderState::Active);
    // The order is still parked — authorize does NOT mutate desk state
    // (open_order_count counts Accepted+; a Pending park is not yet working).
    assert_eq!(e.open_order_count("u-flow"), 0);
    let parked = e.order_by_id(o.order_id).unwrap();
    assert_eq!(parked.state, OrderState::Pending);
}

/// Deny leg: trader disabled by risk → `trade8_risk_trader_disabled` with
/// the TraderState check in the trace.
#[test]
fn risk_double_denies_disabled_trader() {
    let d = RiskDouble::with_states([("u-flow", trade8::contracts::TraderState::DisabledByRisk)]);
    let sym = catalog_fixture().0;
    let dest = catalog_fixture().1;
    let e = seated_engine(&sym, &dest);
    let o = e
        .submit_pending(
            "u-flow",
            "HO",
            new_req(
                "dbl-deny",
                "100",
                Some("10.05"),
                dest.destination_id,
                &sym.name,
            ),
        )
        .unwrap();
    let v = d.engine.authorize(&authorize_req(&o, None));
    assert!(!v.allowed);
    assert_eq!(
        v.reject_code.as_deref(),
        Some("trade8_risk_trader_disabled"),
        "wire code mapping (reject.rs serde rename)"
    );
    let ts = v.trace.first().expect("trader-state check first");
    assert_eq!(ts.check, trade8::contracts::risk::CheckKind::TraderState);
    assert!(!ts.passed);
    assert_eq!(
        v.trader_state,
        trade8::contracts::TraderState::DisabledByRisk
    );
}

/// Deny leg: buying-power exceeded → `trade8_risk_buying_power`.
#[test]
fn risk_double_denies_buying_power() {
    let d = RiskDouble::with_limits(vec![risk_double::limit(100_000)]); // $10 ×10⁴
    let sym = catalog_fixture().0;
    let dest = catalog_fixture().1;
    let e = seated_engine(&sym, &dest);
    let o = e
        .submit_pending(
            "u-flow",
            "HO",
            new_req(
                "dbl-bp",
                "100",
                Some("10.05"),
                dest.destination_id,
                &sym.name,
            ),
        )
        .unwrap();
    let v = d.engine.authorize(&authorize_req(&o, None));
    assert!(!v.allowed);
    assert_eq!(v.reject_code.as_deref(), Some("trade8_risk_buying_power"));
    assert_eq!(
        v.remaining_buying_power, None,
        "deny carries no remaining BP"
    );
}

/// Wiring branches: Risk unenrolled (`risk = None`) structurally allows
/// (the engine's structural checks already ran — desk design §4.2); Risk
/// unreachable (an erroring double) fails CLOSED with the
/// `trade8_risk_trader_disabled` unavailable rejection (outage contract).
#[tokio::test]
async fn wiring_none_allows_and_outage_fails_closed() {
    // Not-enrolled branch: the wiring allows with an empty trace.
    let none = RiskDoubleWiring::NotEnrolled;
    let v = none.authorize_outcome().expect("unenrolled allows");
    assert!(v.allowed);

    // Outage branch: the double simulates a transport failure → the
    // fail-closed rejection (never `Ok(allow)`).
    let down = RiskDoubleWiring::Down;
    let v2 = down.authorize_outcome();
    assert!(v2.is_err(), "outage must fail closed");
}

// ---------------------------------------------------------------------------
// The DB legs (live db trade8; skip offline) — the full two-phase flow
// ---------------------------------------------------------------------------

/// The FULL allow flow: park → Risk authorize (double) → accept → persist
/// (order + event + outbox, one tx) → the outbox payload carries every key
/// Router's `route_accepted_order` consumer reads (payload-completeness,
/// VDC-3) — the cross-service contract that makes `trade8:router` able to
/// run from the event payload alone.
#[tokio::test]
async fn flow_allow_park_authorize_accept_persist_route_contract() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-flow", "HO").await;
    let e = seated_engine(&sym, &dest);
    let client = format!("flow-a-{}", Uuid::now_v7().simple());

    // Phase 1 — park (structural validation passed).
    let pending = e
        .submit_pending(
            "u-flow",
            "HO",
            new_req(
                &client,
                "100",
                Some("10.05"),
                dest.destination_id,
                &sym.name,
            ),
        )
        .unwrap();
    assert_eq!(pending.state, OrderState::Pending);

    // Phase 2 — Risk authorize (the REAL engine on the wire-shaped req).
    let d = RiskDouble::with_limits(vec![risk_double::limit(1_000_000_000)]);
    let verdict = d.engine.authorize(&authorize_req(&pending, None));
    assert!(
        verdict.allowed,
        "flow allow leg pre-condition: {:?}",
        verdict.reject_code
    );

    // Desk accept + one-tx persist (the handler's allow branch shape).
    let order = e.accept_pending(pending.order_id, "u-flow").unwrap();
    assert_eq!(order.state, OrderState::Accepted);
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::persist_order_accepted(&mut tx, env_a(), &order, "u-flow")
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // Parked → working after the accept.
    assert_eq!(e.open_order_count("u-flow"), 1);

    // Confirm the write through a fresh tenant-scoped read.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let (state, price_px): (String, Option<i64>) = sqlx::query_as(
        r#"SELECT state, price_px FROM desk."order"
           WHERE order_id = $1 AND env_id = $2"#,
    )
    .bind(order.order_id)
    .bind(env_a())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(state, "accepted");
    assert_eq!(price_px, Some(100_500), "×10⁴ scale pinned (design §6)");

    // The routed contract: the ORDER_ACCEPTED outbox payload carries every
    // key Router's `route_accepted_order` reads (payload-completeness,
    // VDC-3) — a consumer must run from the payload alone.
    let (family, payload): (String, serde_json::Value) = sqlx::query_as(
        r#"SELECT stream_family, payload FROM desk.outbox
           WHERE env_id = $1 AND event_type = $2 AND payload->>'order_id' = $3
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(env_a())
    .bind(events::ORDER_ACCEPTED)
    .bind(order.order_id.to_string())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(family, "desk", "desk stream family (DBX outbox contract)");
    // The exact keys router/wiring.rs `route_accepted_order` destructures:
    assert_eq!(payload["order_id"], order.order_id.to_string());
    assert_eq!(payload["market_id"], sym.market_id.to_string());
    assert_eq!(payload["office_id"], "HO");
    assert_eq!(
        payload["client_order_id"],
        serde_json::json!(order.client_order_id)
    );
    assert_eq!(payload["symbol_name"], serde_json::json!(sym.name));
    assert_eq!(payload["side"], serde_json::json!("b"));
    assert_eq!(
        payload["qty"]["raw"], 100,
        "qty.raw (u64) for the FIX qty field"
    );
    let price = &payload["price"];
    assert_eq!(price["raw"], 100_500, "price.raw at the ×10⁴ scale");
    assert_eq!(price["scale"], 4, "price.scale carried per value");
    cleanup_all(&mut conn, env_a(), &sym, &dest, "u-flow", "HO", &client).await;
}

/// The FULL deny flow: park → Risk deny (trader disabled by risk) →
/// reject_pending + journal → the ORDER_REJECTED outbox event records the
/// wire code + verdict trace (auditable deny) and the order is NOT
/// persisted to `desk."order"`.
#[tokio::test]
async fn flow_deny_park_authorize_reject_journal() {
    let Some(mut conn) = desk_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let (sym, dest) = catalog_fixture();
    seed_referents(&mut conn, env_a(), &sym, &dest, "u-flow", "HO").await;
    let client = format!("flow-d-{}", Uuid::now_v7().simple());
    let d = RiskDouble::with_states([("u-flow", trade8::contracts::TraderState::DisabledByRisk)]);

    // Park + deny (engine state untouched by the engine itself).
    let o = {
        let e = seated_engine(&sym, &dest);
        let pending = e
            .submit_pending(
                "u-flow",
                "HO",
                new_req(
                    &client,
                    "100",
                    Some("10.05"),
                    dest.destination_id,
                    &sym.name,
                ),
            )
            .unwrap();
        let verdict = d.engine.authorize(&authorize_req(&pending, None));
        assert_eq!(
            verdict.reject_code.as_deref(),
            Some("trade8_risk_trader_disabled")
        );
        let rejected = e
            .reject_pending(
                pending.order_id,
                trade8::contracts::RejectCode::RiskTraderDisabled,
            )
            .unwrap();
        assert_eq!(rejected.state, OrderState::Rejected);
        rejected
    };

    // Journal the auditable deny (the handler's deny branch shape).
    {
        let mut tx = conn.begin().await.unwrap();
        sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
        store::insert_outbox_event(
            &mut tx,
            env_a(),
            events::ORDER_REJECTED,
            serde_json::json!({
                "usr_id": "u-flow", "office_id": "HO",
                "order_id": o.order_id,
                "rejection": trade8::contracts::Rejection::new(
                    trade8::contracts::RejectCode::RiskTraderDisabled),
                // The real deny branch attaches the FULL verdict trace —
                // here one CheckResult matching the trader-state deny.
                "trace": serde_json::json!([{
                    "check": "trader_state", "passed": false,
                    "detail": {"state": "disabled_by_risk", "intent": "open"},
                }]),
            }),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    // The deny never persisted an order row; the journal row exists.
    let mut tx = conn.begin().await.unwrap();
    sentin8::shell::set_tenant_context(&mut tx, env_a()).await;
    let n: i64 = sqlx::query_scalar(r#"SELECT count(*) FROM desk."order" WHERE order_id = $1"#)
        .bind(o.order_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 0, "a risk-deny parks nothing in the durable order table");
    let (family, payload): (String, serde_json::Value) = sqlx::query_as(
        r#"SELECT stream_family, payload FROM desk.outbox
           WHERE env_id = $1 AND event_type = $2 AND payload->>'order_id' = $3
           ORDER BY created_at DESC LIMIT 1"#,
    )
    .bind(env_a())
    .bind(events::ORDER_REJECTED)
    .bind(o.order_id.to_string())
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(family, "desk");
    assert_eq!(payload["usr_id"], "u-flow");
    assert_eq!(
        payload["rejection"]["code"],
        serde_json::json!("trade8_risk_trader_disabled")
    );
    cleanup_all(&mut conn, env_a(), &sym, &dest, "u-flow", "HO", &client).await;
}
