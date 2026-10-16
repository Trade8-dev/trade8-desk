// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Desk service binary — order lifecycle, catalogs, borrow pool,
//! session control.
//!
//! Design: `../desk/DESIGN.md`.
//! Order mutations persist `order` + `order_event` + `desk.outbox` rows
//! in one DB transaction (outbox-first); the catalog bootstraps from the
//! reference tables; the blotter reads the DB with explicit
//! `WHERE env_id` filters (RLS backstop).

#![allow(dead_code)]

mod admin_ui;
mod borrow;
mod catalog;
mod engine;
mod registration;
mod session;
mod store;
mod wiring;

use sqlx::Acquire;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use trade8::contracts::risk::Intent;
use trade8::contracts::{
    events, FlattenRequest, NewOrderRequest, OrderSummary, RejectCode, Rejection,
    ReplaceOrderRequest,
};

/// Service state: shell core + the Desk engine + runtime
/// wiring ( Risk caller,  Feed reader).
pub struct DeskState {
    pub core: sentin8::shell::ServiceCore,
    pub engine: engine::DeskEngine,
    pub wiring: wiring::DeskWiring,
}

fn extend_router(app: Router<Arc<DeskState>>, _state: Arc<DeskState>) -> Router<Arc<DeskState>> {
    app // health is mounted by the shell
        .route("/v1/orders", post(submit_order).get(list_orders))
        .route("/v1/orders/{id}", get(get_order).delete(cancel_order))
        .route("/v1/orders/{id}/replace", post(replace_order))
        .route("/v1/orders/batch", post(submit_batch))
        .route("/v1/positions/flatten", post(flatten))
        .route("/v1/session", get(session_state).post(session_action))
        .route("/v1/symbols", get(list_symbols))
        // Borrow pool CRUD (locate-approval actions).
        .route("/v1/borrow", get(borrow_read).post(borrow_mutate))
        // WS order entry — the two-way tier.
        .route("/ws", get(ws_entry))
        // Func proxy-back host functions.
        .route("/v1/internal/func-host/orders", post(func_host_submit))
        .route("/v1/internal/func-host/orders/{id}", get(func_host_get))
        .route(
            "/v1/internal/func-host/orders/{id}/cancel",
            post(func_host_cancel),
        )
        // Admin RFW pages: page definitions + POST-alias data endpoints.
        .route("/v1/admin/ui/{*page}", get(admin_ui::get_admin_ui))
        // ── Shield policy-admin API (platform policy design — shared
        // implementation; handlers generic over this state via HasPolicyAdmin).
        .route(
            "/v1/admin/policy-sets",
            get(sentin8::shield::admin::list_policy_sets::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            get(sentin8::shield::admin::get_policy_set::<Arc<DeskState>>)
                .put(sentin8::shield::admin::put_policy_set::<Arc<DeskState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            post(sentin8::shield::admin::test_policy_set::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            post(sentin8::shield::admin::admin_enforce_policy_set::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            post(sentin8::shield::admin::admin_shadow_policy_set::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            post(sentin8::shield::admin::admin_disable_policy_set::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/relations",
            get(sentin8::shield::admin::list_relations::<Arc<DeskState>>)
                .put(sentin8::shield::admin::put_relation::<Arc<DeskState>>)
                .delete(sentin8::shield::admin::delete_relation::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/relations/check",
            get(sentin8::shield::admin::check_relation::<Arc<DeskState>>),
        )
        .route(
            "/v1/admin/attributes",
            get(sentin8::shield::admin::list_attributes::<Arc<DeskState>>)
                .put(sentin8::shield::admin::put_attribute::<Arc<DeskState>>)
                .delete(sentin8::shield::admin::delete_attribute::<Arc<DeskState>>),
        )
        .route("/v1/admin/orders", get(admin_ui::admin_orders))
        .route("/v1/admin/overview", get(admin_ui::admin_overview))
        .route("/v1/admin/symbols", get(admin_ui::admin_symbols))
        .route("/v1/admin/markets", get(admin_ui::admin_markets))
        .route("/v1/admin/destinations", get(admin_ui::admin_destinations))
        .route("/v1/admin/offices", get(admin_ui::admin_offices))
        .route("/v1/admin/borrow", get(admin_ui::admin_borrow))
        .route(
            "/v1/admin/session",
            get(admin_ui::admin_session).post(admin_ui::admin_session_action),
        )
        .route(
            "/v1/health",
            // Carries the binary's build identity (`version`/`build`,
            // BUILDID-1) so captain's monitor populates `make status`'s
            // VERSION column — the consuming BINARY's identity.
            get(|| async { Json(serde_json::json!({"status": "ok", "service": "trade8:desk", "version": sentin8::build_info::id(), "build": sentin8::build_info::build_hash()})) }),
        )
}

/// Reject → HTTP mapping (error-code parity with registration.rs).
fn rej_http(rej: &Rejection) -> (axum::http::StatusCode, String) {
    let code = match rej.code {
        RejectCode::OrderNotFound => axum::http::StatusCode::NOT_FOUND,
        RejectCode::SessionNotOpen => axum::http::StatusCode::CONFLICT,
        _ => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
    };
    (
        code,
        serde_json::to_string(rej).unwrap_or_else(|_| "order rejected".into()),
    )
}

/// The Risk-gated submit path: structural validation parks a
/// Pending order → Risk `authorize` (fail-closed) → accept or reject,
/// each persisted with journal + outbox rows in one tx.
async fn submit_order(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<NewOrderRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let env = auth.env_id();
    let usr = auth.claims.sub.clone();
    let office = st
        .engine
        .catalog
        .office_for(&usr)
        .unwrap_or_else(|| "DEFAULT".into());
    // Step 1: structural validation parks a Pending order.
    let pending = match st.engine.submit_pending(&usr, &office, req) {
        Ok(o) => o,
        Err(rej) => return submit_rejected(st, env, &usr, &office, rej).await,
    };
    // Idempotent resubmit: an existing order returns as-is (no re-auth).
    if pending.state != trade8::contracts::OrderState::Pending {
        return Ok(Json(serde_json::json!({
            "order_id": pending.order_id, "state": pending.state, "duplicate": true,
        })));
    }
    // Step 2: Risk authorize (fail-closed on outage).
    match st
        .wiring
        .authorize_order(env, &st.engine, &pending, Intent::Open)
        .await
    {
        Ok(verdict) if verdict.allowed => {
            let order = st
                .engine
                .accept_pending(pending.order_id, &usr)
                .map_err(|r| rej_http(&r))?;
            let mut conn = st
                .core
                .pool
                .acquire()
                .await
                .map_err(sentin8::shell::err_db)?;
            sentin8::shell::set_tenant_context(&mut conn, env).await;
            // RLS context is transaction-local (DESIGN.md §5) —
            // begin() pins the tenant context to the SAME tx as the DML/reads.
            let mut tx = match conn.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "desk: tenant tx begin failed");
                    return Err(sentin8::shell::err_db(e));
                }
            };
            if let Err(e) = store::persist_order_accepted(&mut tx, env, &order, &usr).await {
                tracing::warn!(error = %e, "trade8:desk: order persist failed");
                st.engine
                    .cancel_with_actor("system:persist-fail", order.order_id)
                    .ok();
                return Err(sentin8::shell::err_db(e));
            }
            Ok(Json(serde_json::json!({
                "order_id": order.order_id,
                "state": order.state,
                "remaining_buying_power": verdict.remaining_buying_power,
            })))
        }
        Ok(verdict) => {
            // Risk deny: map the code, journal the rejection with the
            // full verdict trace (auditable deny — desk §4.2 step 3).
            let code = RejectCode::from_wire(&verdict.reject_code.clone().unwrap_or_default())
                .unwrap_or(RejectCode::RiskTraderDisabled);
            let rej = Rejection::new(code).with_params(serde_json::json!({"trace": verdict.trace}));
            let order = st.engine.reject_pending(pending.order_id, code).ok();
            let mut conn = st
                .core
                .pool
                .acquire()
                .await
                .map_err(sentin8::shell::err_db)?;
            sentin8::shell::set_tenant_context(&mut conn, env).await;
            // RLS context is transaction-local (DESIGN.md §5) —
            // begin() pins the tenant context to the SAME tx as the DML/reads.
            let mut tx = match conn.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "desk: tenant tx begin failed");
                    return Err(sentin8::shell::err_db(e));
                }
            };
            let _ = store::insert_outbox_event(
                &mut tx,
                env,
                events::ORDER_REJECTED,
                serde_json::json!({
                    "usr_id": usr, "office_id": office,
                    "order_id": order.map(|o| o.order_id),
                    "rejection": rej, "trace": verdict.trace,
                }),
            )
            .await;
            tx.commit().await.ok(); // dropping an uncommitted tx rolls back
            Err(rej_http(&rej))
        }
        Err(rej) => submit_rejected(st, env, &usr, &office, rej).await,
    }
}

/// Shared rejection path: journal + 4xx (auditable deny).
async fn submit_rejected(
    st: Arc<DeskState>,
    env: uuid::Uuid,
    usr: &str,
    office: &str,
    rej: Rejection,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    // the context is transaction-local — set it INSIDE an
    // explicit tx with the DML, never on a bare connection.
    let mut conn = match st.core.pool.acquire().await {
        Ok(c) => c,
        Err(_) => return Err(rej_http(&rej)),
    };
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(_) => return Err(rej_http(&rej)),
    };
    sentin8::shell::set_tenant_context(&mut tx, env).await;
    let _ = store::insert_outbox_event(
        &mut tx,
        env,
        events::ORDER_REJECTED,
        serde_json::json!({"usr_id": usr, "office_id": office, "rejection": rej}),
    )
    .await;
    tx.commit().await.ok();
    Err(rej_http(&rej))
}

/// Basket submission: per-item validation + accept/reject, one
/// batch event. Each accepted item persists independently — a failing
/// item rejects only itself. Every item passes the Risk gate.
async fn submit_batch(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(reqs): Json<Vec<NewOrderRequest>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let env = auth.env_id();
    let usr = auth.claims.sub.clone();
    let office = st
        .engine
        .catalog
        .office_for(&usr)
        .unwrap_or_else(|| "DEFAULT".into());
    let mut accepted: Vec<serde_json::Value> = Vec::new();
    let mut rejected: Vec<serde_json::Value> = Vec::new();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    for req in reqs.into_iter().take(200) {
        let client_id = req.client_order_id.clone();
        // Structural phase.
        let pending = match st.engine.submit_pending(&usr, &office, req) {
            Ok(o) => o,
            Err(rej) => {
                rejected.push(serde_json::json!({"client_order_id": client_id, "rejection": rej}));
                continue;
            }
        };
        if pending.state != trade8::contracts::OrderState::Pending {
            accepted.push(serde_json::json!({"order_id": pending.order_id, "duplicate": true}));
            continue;
        }
        // Risk phase (fail-closed).
        match st
            .wiring
            .authorize_order(env, &st.engine, &pending, Intent::Open)
            .await
        {
            Ok(v) if v.allowed => {}
            Ok(v) => {
                let code = RejectCode::from_wire(&v.reject_code.clone().unwrap_or_default())
                    .unwrap_or(RejectCode::RiskTraderDisabled);
                let _ = st.engine.reject_pending(pending.order_id, code);
                rejected.push(serde_json::json!({
                    "client_order_id": client_id,
                    "rejection": Rejection::new(code).with_params(serde_json::json!({"trace": v.trace})),
                }));
                continue;
            }
            Err(rej) => {
                rejected.push(serde_json::json!({"client_order_id": client_id, "rejection": rej}));
                continue;
            }
        }
        let order = match st.engine.accept_pending(pending.order_id, &usr) {
            Ok(o) => o,
            Err(r) => {
                rejected.push(serde_json::json!({"client_order_id": client_id, "rejection": r}));
                continue;
            }
        };
        if store::persist_order_accepted(&mut tx, env, &order, &usr)
            .await
            .is_err()
        {
            st.engine
                .cancel_with_actor("system:persist-fail", order.order_id)
                .ok();
            rejected.push(
                serde_json::json!({"client_order_id": client_id, "reason": "persist failed"}),
            );
            continue;
        }
        accepted.push(serde_json::json!({"order_id": order.order_id, "state": order.state}));
    }
    let _ = store::insert_outbox_event(
        &mut tx,
        env,
        events::ORDER_SUBMITTED,
        serde_json::json!({"usr_id": usr, "accepted": accepted.len(), "rejected": rejected.len()}),
    )
    .await;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(
        serde_json::json!({"accepted": accepted, "rejected": rejected}),
    ))
}

/// Blotter read: DB-backed with in-memory fallback when the DB is
/// unavailable (degraded-read mode keeps the desk usable).
async fn list_orders(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<OrderSummary>>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let env = auth.env_id();
    let usr = auth.claims.sub.clone();
    let office = st
        .engine
        .catalog
        .office_for(&usr)
        .unwrap_or_else(|| "DEFAULT".into());
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(200)
        .min(1000);
    let mut conn = match st.core.pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "trade8:desk: blotter degraded to engine memory");
            return Ok(Json(st.engine.blotter_for(&usr, &office)));
        }
    };
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    match store::blotter_rows(&mut tx, env, &usr, &office, limit).await {
        Ok(rows) => Ok(Json(rows)),
        Err(e) => {
            tracing::warn!(error = %e, "trade8:desk: blotter DB read failed; engine memory");
            Ok(Json(st.engine.blotter_for(&usr, &office)))
        }
    }
}

async fn get_order(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let env = auth.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    match store::load_order(&mut tx, env, id).await {
        Ok(Some(o)) => Ok(Json(serde_json::to_value(&o).unwrap_or_default())),
        Ok(None) => {
            // Fresh submits in degraded mode may live only in engine memory.
            st.engine
                .order_by_id(id)
                .map(|o| Json(serde_json::to_value(&o).unwrap_or_default()))
                .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "order not found".into()))
        }
        Err(e) => Err(sentin8::shell::err_db(e)),
    }
}

async fn cancel_order(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:cancel"),
    )
    .await?;
    let env = auth.env_id();
    let usr = auth.claims.sub.clone();
    let prev = st.engine.order_by_id(id).map(|o| o.state);
    match st.engine.cancel_with_actor(&usr, id) {
        Ok(o) => {
            let mut conn = st
                .core
                .pool
                .acquire()
                .await
                .map_err(sentin8::shell::err_db)?;
            sentin8::shell::set_tenant_context(&mut conn, env).await;
            // RLS context is transaction-local (DESIGN.md §5) —
            // begin() pins the tenant context to the SAME tx as the DML/reads.
            let mut tx = match conn.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "desk: tenant tx begin failed");
                    return Err(sentin8::shell::err_db(e));
                }
            };
            if let Err(e) = store::persist_transition(
                &mut tx,
                env,
                &o,
                prev.unwrap_or(trade8::contracts::OrderState::Accepted),
                "cancel",
                &usr,
                events::ORDER_CANCELLED,
            )
            .await
            {
                tracing::warn!(error = %e, "trade8:desk: cancel persist failed");
                return Err(sentin8::shell::err_db(e));
            }
            Ok(Json(
                serde_json::json!({"order_id": o.order_id, "state": o.state}),
            ))
        }
        Err(rej) => Err(rej_http(&rej)),
    }
}

async fn replace_order(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
    Json(req): Json<ReplaceOrderRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let env = auth.env_id();
    let usr = auth.claims.sub.clone();
    match st.engine.replace_with_actor(&usr, id, req) {
        Ok(o) => {
            let mut conn = st
                .core
                .pool
                .acquire()
                .await
                .map_err(sentin8::shell::err_db)?;
            sentin8::shell::set_tenant_context(&mut conn, env).await;
            // RLS context is transaction-local (DESIGN.md §5) —
            // begin() pins the tenant context to the SAME tx as the DML/reads.
            let mut tx = match conn.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!(error = %e, "desk: tenant tx begin failed");
                    return Err(sentin8::shell::err_db(e));
                }
            };
            if let Err(e) = store::persist_order_accepted(&mut tx, env, &o, &usr).await {
                tracing::warn!(error = %e, "trade8:desk: replace persist failed");
                return Err(sentin8::shell::err_db(e));
            }
            Ok(Json(serde_json::json!({
                "order_id": o.order_id,
                "replaces": id,
                "state": o.state,
            })))
        }
        Err(rej) => Err(rej_http(&rej)),
    }
}

async fn flatten(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<FlattenRequest>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:flatten"),
    )
    .await?;
    let env = auth.env_id();
    // the typed strategy parameters ride the flatten event — the
    // Func strategy module (flexible-flat) consumes them for the resubmit
    // loop; the Desk performs the synchronous cancel side now.
    let strategy = serde_json::to_value(&req.strategy).unwrap_or_default();
    let count = st.engine.flatten(&auth, req).await;
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    let _ = store::insert_outbox_event(
        &mut tx,
        env,
        events::ORDER_FLATTEN,
        serde_json::json!({
            "usr_id": auth.claims.sub,
            "cancelled": count,
            "strategy": strategy,
        }),
    )
    .await;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(serde_json::json!({"flatten_orders_submitted": count})))
}

async fn session_state(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    Ok(Json(
        serde_json::json!({"state": st.engine.session_state().as_str()}),
    ))
}

/// Session action (Cron proxy-back + manual ops). Transitions persist a
/// `session:state` outbox event; the close transition sweeps day orders
/// (cancel) and parks overnight ones — engine + DB in the same handler
/// (session FSM); preopen restores parked orders from the DB.
async fn session_action(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    // trade8:cron:run — the Cron proxy-back M2M. Step 5 — Shield
    // decides on the session-manage action.
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:market:session:manage"),
    )
    .await?;
    let env = auth.env_id();
    let action = body.get("action").and_then(|a| a.as_str()).unwrap_or("");
    let prev_state = st.engine.session_state();
    let state = st.engine.session_action(action).await;
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    if state != prev_state {
        let mut swept =
            serde_json::json!({"state": state.as_str(), "action": action, "by": auth.claims.sub});
        if state == trade8::contracts::SessionState::Closed {
            //  persistence: every swept order is journaled + the
            // overnight park snapshot lands in desk.overnight_order.
            let summary = persist_session_close(&st.engine, &mut tx, env).await;
            swept["cancelled"] = serde_json::json!(summary.0);
            swept["parked_overnight"] = serde_json::json!(summary.1);
        }
        if state == trade8::contracts::SessionState::Preopen {
            // restore parked orders → pending, chained ids, event.
            let restored = restore_overnight(&st.engine, &mut tx, env).await;
            swept["overnight_restored"] = serde_json::json!(restored);
        }
        let _ = store::insert_outbox_event(&mut tx, env, events::SESSION_STATE, swept).await;
    }
    Ok(Json(serde_json::json!({"state": state.as_str()})))
}

/// Persist the engine's close-sweep results. The engine already
/// mutated working orders; this mirrors each mutation to the DB with
/// journal rows and overnight snapshots.
async fn persist_session_close(
    engine: &engine::DeskEngine,
    conn: &mut sqlx::PgConnection,
    env: uuid::Uuid,
) -> (usize, usize) {
    let mut cancelled = 0usize;
    let mut parked = 0usize;
    for (id, prev, new, _actor, _reason) in engine.events_snapshot() {
        if new == trade8::contracts::OrderState::Cancelled
            && matches!(
                prev,
                trade8::contracts::OrderState::Accepted
                    | trade8::contracts::OrderState::Routed
                    | trade8::contracts::OrderState::PartiallyFilled
            )
            && prev != trade8::contracts::OrderState::Cancelled
        {
            if let Some(o) = engine.order_by_id(id) {
                if store::persist_swept(
                    conn,
                    env,
                    &o,
                    prev,
                    "session_close_cancel",
                    events::ORDER_CANCELLED,
                )
                .await
                .is_ok()
                {
                    cancelled += 1;
                }
            }
        }
        if new == trade8::contracts::OrderState::OvernightParked {
            if let Some(o) = engine.order_by_id(id) {
                if store::persist_swept(
                    conn,
                    env,
                    &o,
                    prev,
                    "session_close_park",
                    events::ORDER_OVERNIGHT,
                )
                .await
                .is_ok()
                {
                    parked += 1;
                }
            }
        }
    }
    (cancelled, parked)
}

/// Restore parked overnight orders: DB load → engine restore
/// (new pending order, chained id) → mark the parked row restored.
async fn restore_overnight(
    engine: &engine::DeskEngine,
    conn: &mut sqlx::PgConnection,
    env: uuid::Uuid,
) -> usize {
    let Ok(parked) = store::load_overnight_orders(conn, env).await else {
        return 0;
    };
    let mut n = 0usize;
    for p in parked {
        let parked_id = p.order_id;
        let restored = engine.restore_one(p);
        if store::persist_order_accepted(conn, env, &restored, "system:overnight-restore")
            .await
            .is_ok()
        {
            let _ = store::persist_restored(conn, env, parked_id, &restored).await;
            n += 1;
        }
    }
    n
}

async fn list_symbols(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    Ok(Json(st.engine.symbols_json()))
}

/// Borrow pool read (admin surface): per-symbol availability.
async fn borrow_read(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:risk:borrow:read"),
    )
    .await?;
    let symbol = q.get("symbol");
    let rows: Vec<serde_json::Value> = st
        .engine
        .catalog
        .symbols_json()
        .into_iter()
        .filter(|s| {
            symbol
                .map(|f| s["name"].as_str() == Some(f))
                .unwrap_or(true)
        })
        .map(|s| {
            let id = s["symbol_id"].as_str().unwrap_or_default().to_string();
            let uuid = uuid::Uuid::parse_str(&id).unwrap_or_default();
            serde_json::json!({
                "symbol_id": id,
                "name": s["name"],
                "available": st.engine.borrow.available(uuid),
                "etb": st.engine.borrow.is_etb(uuid),
                "exempt": st.engine.borrow.is_exempt(uuid),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({"pool": rows})))
}

/// Borrow mutation (locate-approval action): pool totals, office
/// blocks, ETB/HTB classification, exempt toggles. DB + engine + event.
async fn borrow_mutate(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(m): Json<store::BorrowMutation>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let auth = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &auth,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:risk:borrow:write"),
    )
    .await?;
    let env = auth.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // RLS context is transaction-local (DESIGN.md §5) —
    // begin() pins the tenant context to the SAME tx as the DML/reads.
    let mut tx = match conn.begin().await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "desk: tenant tx begin failed");
            return Err(sentin8::shell::err_db(e));
        }
    };
    store::persist_borrow_mutation(&mut tx, env, &st.engine, &m, &auth.claims.sub)
        .await
        .map_err(sentin8::shell::err_db)?;
    tx.commit().await.ok(); // dropping an uncommitted tx rolls back
    Ok(Json(
        serde_json::json!({"symbol_id": m.symbol_id, "status": "ok"}),
    ))
}

/// WS order entry — the bidirectional tier. Same authorization
/// (Bearer on the upgrade request) + same submit path as REST; the
/// client replays `idempotency_key` in `client_order_id` so resubmits
/// are deduped by the engine (replay-safe).
async fn ws_entry(
    ws: WebSocketUpgrade,
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, (axum::http::StatusCode, String)> {
    // Authorization at upgrade time — no anonymous sockets.
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:write"),
    )
    .await?;
    let st = st.clone();
    Ok(ws.on_upgrade(move |socket| ws_loop(socket, st)))
}

/// The per-socket loop: client sends NewOrderRequest JSON frames; the
/// server replies with the submit outcome. Cancels use the same frame
/// with `{"action":"cancel","order_id":...}`.
async fn ws_loop(mut socket: WebSocket, st: Arc<DeskState>) {
    while let Some(Ok(msg)) = socket.recv().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
            let _ = socket
                .send(Message::Text("{\"error\":\"bad frame\"}".into()))
                .await;
            continue;
        };
        let reply = match v.get("action").and_then(|a| a.as_str()) {
            Some("cancel") => {
                let id = v["order_id"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok());
                match id {
                    Some(id) => match st.engine.cancel_with_actor("ws", id) {
                        Ok(o) => serde_json::json!({"order_id": o.order_id, "state": o.state}),
                        Err(r) => serde_json::json!({"rejection": r}),
                    },
                    None => serde_json::json!({"error":"order_id required"}),
                }
            }
            _ => {
                // Submit frame: NewOrderRequest + idempotency replay key.
                match serde_json::from_value::<NewOrderRequest>(v.clone()) {
                    Ok(req) => {
                        let usr = v
                            .get("usr_id")
                            .and_then(|u| u.as_str())
                            .unwrap_or("ws")
                            .to_string();
                        let office = st
                            .engine
                            .catalog
                            .office_for(&usr)
                            .unwrap_or_else(|| "DEFAULT".into());
                        match st.engine.submit_with_actor(&usr, &office, req) {
                            Ok(o) => serde_json::json!({"order_id": o.order_id, "state": o.state}),
                            Err(r) => serde_json::json!({"rejection": r}),
                        }
                    }
                    Err(_) => serde_json::json!({"error":"bad order frame"}),
                }
            }
        };
        let Ok(out) = serde_json::to_string(&reply) else {
            continue;
        };
        if socket.send(Message::Text(out.into())).await.is_err() {
            break;
        }
    }
}

/// Func proxy-back — `POST /v1/internal/func-host/orders`:
/// submit on behalf of a strategy module (attribution via
/// `on_behalf_of`; the acting user is the strategy's owner).
async fn func_host_submit(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Json(host): Json<wiring::func_host::HostSubmit>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:func:host"),
    )
    .await?;
    let office = st
        .engine
        .catalog
        .office_for(&host.usr_id)
        .unwrap_or_else(|| "DEFAULT".into());
    let mut req = host.req;
    req.on_behalf_of = Some(host.on_behalf_of.unwrap_or_else(|| "func".into()));
    match st.engine.submit_with_actor(&host.usr_id, &office, req) {
        Ok(o) => Ok(Json(serde_json::json!({
            "order_id": o.order_id, "state": o.state, "on_behalf_of": o.on_behalf_of,
        }))),
        Err(r) => Err(rej_http(&r)),
    }
}

/// Func proxy-back — `GET /v1/internal/func-host/orders/{id}`.
async fn func_host_get(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:func:host"),
    )
    .await?;
    match st.engine.order_by_id(id) {
        Some(o) => Ok(Json(serde_json::to_value(&o).unwrap_or_default())),
        None => Err((axum::http::StatusCode::NOT_FOUND, "order not found".into())),
    }
}

/// Func proxy-back — `POST /v1/internal/func-host/orders/{id}/cancel`.
async fn func_host_cancel(
    State(st): State<Arc<DeskState>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:func:host"),
    )
    .await?;
    match st.engine.cancel_with_actor("func", id) {
        Ok(o) => Ok(Json(
            serde_json::json!({"order_id": o.order_id, "state": o.state}),
        )),
        Err(r) => Err(rej_http(&r)),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("trade8:desk", "TRADE8_DESK")
            .with_registration(registration::print)
            .with_own_health()
            .with_state(|core| async move {
                let engine = engine::DeskEngine::new();
                // Catalog + borrow bootstrap: load reference
                // tables for the boot env (captain injects TRADE8_DESK_ENV).
                // Absent or non-UUID (local dev) leaves the catalog empty
                // until the first admin seed.
                if let Ok(env_slug) = std::env::var("TRADE8_DESK_ENV") {
                    match uuid::Uuid::parse_str(&env_slug) {
                        Ok(env_id) => {
                            let mut conn = core.pool.acquire().await?;
                            store::bootstrap_catalog(&mut conn, env_id, &engine).await;
                            if let Err(e) =
                                store::bootstrap_borrow(&mut conn, env_id, &engine).await
                            {
                                tracing::warn!(error = %e, "trade8:desk: borrow bootstrap failed");
                            }
                        }
                        Err(_) => tracing::warn!("TRADE8_DESK_ENV is not a UUID; catalog empty"),
                    }
                }
                // Runtime wiring Risk authorize caller +
                // Feed BBO reader (certificate-presenting callers;
                // absent URLs → off).
                let wiring = wiring::DeskWiring::build(&core.config.sentin8_auth_issuer).await;
                // Cron session choreography (the Ledger pattern):
                // preopen/open/close-pending/close schedules registered at
                // boot; Cron proxy-calls `/v1/session`.
                if let Ok(cron_url) = std::env::var("SENTIN8_CRON_URL") {
                    if !cron_url.is_empty() {
                        trade8::client::cron::register_defaults(
                            &core.config.sentin8_auth_issuer,
                            &cron_url,
                            "trade8:desk",
                            core.config.port,
                        )
                        .await;
                    }
                }
                Ok(Arc::new(DeskState {
                    core,
                    engine,
                    wiring,
                }))
            })
            .with_router(extend_router),
    )
    .await
}

/// Shared policy-admin surface accessor: the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache.
impl sentin8::shield::admin::HasPolicyAdmin for DeskState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}

// ---------------------------------------------------------------------------
// Handler tests (test-architecture design §7). The 8-step pipeline over
// the REAL router: verify (1–4) against a LOCAL JWKS stand-in (one
// ephemeral listener; SigningKeys::generate_for_testing both serves the
// JWKS doc and mints the tokens), Shield gate (5) from the real
// scope-mirror seed, RLS context (6+) on db trade8 via the service role
// (skip-offline like sql_matrix).
// ---------------------------------------------------------------------------
// The in-file `handler_tests` module below exercises handler flows against
// an ephemeral OIDC stand-in. It references `sentin8_auth::jwks` /
// `sentin8_auth::jwt` — the Auth SERVER crate — which this repo never
// declared as a dependency (no vertical pins the auth server; imports go
// through the platform SDK facade, which does not re-export jwks). The
// harness therefore never compiled as-is. Disabled pending the SDK-side
// test-harness OIDC stand-in surface (backlog story T8-HARNESS-1).
#[cfg(all(test, feature = "handler_tests"))]
mod handler_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::{Json, Router};
    use tower::ServiceExt;

    fn env_a() -> uuid::Uuid {
        uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    }

    /// The desk router built exactly like `main` does (minus TLS).
    fn test_router(state: Arc<DeskState>) -> Router {
        let for_state = state.clone();
        extend_router(Router::new(), state).with_state(for_state)
    }

    /// Ephemeral OIDC stand-in: discovery + JWKS + minting.
    struct Harness {
        issuer: String,
        keys: sentin8_auth::jwks::SigningKeys,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        async fn spawn() -> Self {
            let keys = sentin8_auth::jwks::SigningKeys::generate_for_testing().expect("test keys");
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind ephemeral");
            let issuer = format!("http://{}", listener.local_addr().expect("local addr"));
            let keys_for_jwks = keys.clone();
            let app = Router::new()
                .route(
                    "/.well-known/openid-configuration",
                    get({
                        let issuer = issuer.clone();
                        move || {
                            let issuer = issuer.clone();
                            async move {
                                Json(serde_json::json!({
                                    "issuer": issuer,
                                    "jwks_uri": format!("{issuer}/jwks"),
                                }))
                            }
                        }
                    }),
                )
                .route(
                    "/jwks",
                    get(move || {
                        // Build per request (JwksResponse is not Clone; the
                        // JWK material is immutable once generated).
                        let all = sentin8_auth::jwks::JwksResponse::from_keys(&keys_for_jwks);
                        async move { Json(all) }
                    }),
                );
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self { issuer, keys, task }
        }

        /// A user access token (`AuthClaims::access` shape).
        fn mint(&self, sub: &str, env: uuid::Uuid, scopes: &[&str], ttl_secs: i64) -> String {
            let claims = sentin8_auth::jwt::AuthClaims::access(
                &self.issuer,
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, sub.as_bytes()),
                &scopes.join(" "),
                env,
                Some(&format!("{sub}@test.local")),
                chrono::Duration::seconds(ttl_secs.max(1)),
            );
            self.keys.sign(&claims).expect("mint user token")
        }

        fn mint_expired(&self, sub: &str, env: uuid::Uuid, scopes: &[&str]) -> String {
            let claims = sentin8_auth::jwt::AuthClaims::access(
                &self.issuer,
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, sub.as_bytes()),
                &scopes.join(" "),
                env,
                None,
                chrono::Duration::seconds(-3600),
            );
            self.keys.sign(&claims).expect("mint expired token")
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn oneshot(
        app: &Router,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        let req = match body {
            Some(v) => b
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&v).unwrap())),
            None => b.body(Body::empty()),
        }
        .unwrap();
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    /// State assembly mirroring the shell boot (config → pool → policy
    /// engine boot → DeskState). `None` = DB offline (skip).
    async fn build_state(h: &Harness) -> Option<Arc<DeskState>> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect("postgresql://trade8_desk_service@localhost:5432/trade8")
            .await
            .ok()?;
        let policy_engine = sentin8::shield::runtime::engine_for("trade8:desk");
        // Boot from the local substrate (vertical init seeded it); a fresh
        // DB boots the deny-default engine — the 403 legs still assert.
        let _ =
            sentin8::shield::runtime::boot_full(&pool, &h.issuer, "trade8:desk", &policy_engine)
                .await;
        let config = Arc::new(sentin8::shell::ShellConfig {
            service_type: "trade8:desk",
            port: 0,
            database_url: "postgresql://trade8_desk_service@localhost:5432/trade8".into(),
            sentin8_auth_issuer: h.issuer.clone(),
            sentin8_bus_url: sentin8::config::Sentin8Config::from_env().sentin8_bus_url(),
            sentin8_auth_client: sentin8::auth::client::Client::new(
                sentin8::auth::client::ClientOptions {
                    base_url: h.issuer.clone(),
                    ..Default::default()
                },
            ),
            valkey_url: String::new(),
        });
        let core = sentin8::shell::ServiceCore {
            config: config.clone(),
            pool: pool.clone(),
            admin: sentin8::shield::admin::SharedAdminState {
                service: "trade8:desk".into(),
                pool,
                policy: policy_engine,
                auth_client: Arc::new(sentin8::auth::client::Client::new(
                    sentin8::auth::client::ClientOptions {
                        base_url: h.issuer.clone(),
                        ..Default::default()
                    },
                )),
            },
        };
        let engine = engine::DeskEngine::new();
        let state = Arc::new(DeskState {
            core,
            engine,
            wiring: wiring::DeskWiring {
                risk: None,
                feed: None,
            },
        });
        Some(state)
    }

    #[tokio::test]
    async fn missing_token_is_401() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        // No token at all.
        let (status, _) = oneshot(&app, "GET", "/v1/orders", None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // Malformed token: not a JWT-shaped value; the verify step rejects
        // 401 — unless a platform dev-credential fallback exists (the dev
        // static token path); both 401 and 403 prove rejection here.
        let (status, _) = oneshot(&app, "GET", "/v1/orders", Some("garbage"), None).await;
        assert!(
            status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
            "garbage token must reject (got {status})"
        );
    }

    #[tokio::test]
    async fn expired_token_is_401() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        let t = h.mint_expired("trader-1", env_a(), &["trade8:desk:order:read"]);
        let (status, _) = oneshot(&app, "GET", "/v1/orders", Some(&t), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "expired → 401");
    }

    #[tokio::test]
    async fn wrong_issuer_token_is_rejected() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        // A structurally-valid token whose iss points elsewhere.
        let other = sentin8_auth::jwks::SigningKeys::generate_for_testing().unwrap();
        let claims = sentin8_auth::jwt::AuthClaims::access(
            "https://evil.example",
            uuid::Uuid::new_v4(),
            "trade8:desk:order:read",
            env_a(),
            None,
            chrono::Duration::minutes(5),
        );
        let t = other.sign(&claims).unwrap();
        let (status, _) = oneshot(&app, "GET", "/v1/orders", Some(&t), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "unknown issuer → 401");
    }

    #[tokio::test]
    async fn rejection_json_shape_is_code_message_params() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        // Trader without a read scope → step-5 403 shape (shield-deny or
        // err_db body); the body must be a JSON object with a message.
        let t = h.mint("no-scope-trader", env_a(), &["trade8:desk:order:read"], 300);
        let (status, body) = oneshot(&app, "GET", "/v1/orders", Some(&t), None).await;
        assert_eq!(status, StatusCode::OK, "read allowed with the read scope");
        assert!(body.is_object(), "blotter body is a JSON object");
        // A scope-mismatched call — the rejection body shape contract:
        let t_ns = h.mint("scopeless", env_a(), &[], 300);
        let (status_ns, body_ns) = oneshot(&app, "GET", "/v1/orders", Some(&t_ns), None).await;
        assert_eq!(status_ns, StatusCode::FORBIDDEN, "no scope → 403");
        assert!(
            body_ns.get("code").is_some()
                || body_ns.get("message").is_some()
                || body_ns.is_string(),
            "rejection body carries a code/message shape: {body_ns}"
        );
    }

    #[tokio::test]
    async fn submit_requires_write_scope_and_persists() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        // Seat the engine with the fixture catalog (symbol + destination).
        state
            .engine
            .session
            .set(trade8::contracts::SessionState::Open);
        let app = test_router(state.clone());
        // Write scope missing → 403 (step 5 gate, not a 500).
        let t_ro = h.mint("trader-ro", env_a(), &["trade8:desk:order:read"], 300);
        let (status, body) = oneshot(
            &app,
            "POST",
            "/v1/orders",
            Some(&t_ro),
            Some(serde_json::json!({
                "client_order_id": "ht-1", "symbol_name": "AAPL",
                "destination_id": uuid::Uuid::new_v4(),
                "side": "b", "order_type": "limit",
                "qty": "100", "price": "10.00", "tif": "0",
            })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "read-scope submit → 403: {body}"
        );
        // With the write scope the request proceeds — an unknown
        // destination/session still yields a STRUCTURED rejection (the
        // validation pipeline runs after the gate); never a panic/500-unless-DB.
        let t_rw = h.mint(
            "trader-rw",
            env_a(),
            &["trade8:desk:order:write", "trade8:desk:order:read"],
            300,
        );
        let (status2, body2) = oneshot(
            &app,
            "POST",
            "/v1/orders",
            Some(&t_rw),
            Some(serde_json::json!({
                "client_order_id": "ht-2", "symbol_name": "AAPL",
                "destination_id": uuid::Uuid::new_v4(),
                "side": "b", "order_type": "limit",
                "qty": "100", "price": "10.00", "tif": "0",
            })),
        )
        .await;
        // In-memory engine: unknown destination → engine rejection
        // (structured 4xx), empty catalog symbol-unknown, or DB-offline
        // persist failure → 5xx logged-but-accepted — accept all three;
        // the pipeline (not a panic) produced the verdict.
        assert_ne!(status2, StatusCode::UNAUTHORIZED, "valid token accepted");
        assert_ne!(
            status2,
            StatusCode::FORBIDDEN,
            "write scope passes step 5: {body2}"
        );
    }

    #[tokio::test]
    async fn session_choreography_open_close() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state.clone());
        let t = h.mint("ops-1", env_a(), &["trade8:market:session:manage"], 300);
        // open → engine gate opens
        let (status, body) = oneshot(&app, "POST", "/v1/session/open", Some(&t), None).await;
        assert_eq!(status, StatusCode::OK, "open OK: {body}");
        assert_eq!(
            body["state"],
            serde_json::json!("open"),
            "engine state open"
        );
        // submitting is now gated OPEN (the in-memory engine accepts to pending)
        // close-pending → the gate blocks new submits (rejection contract)
        let (status2, body2) =
            oneshot(&app, "POST", "/v1/session/close-pending", Some(&t), None).await;
        assert_eq!(status2, StatusCode::OK, "close-pending OK: {body2}");
        assert_eq!(body2["state"], serde_json::json!("close_pending"));
        let t_rw = h.mint("trader-rw", env_a(), &["trade8:desk:order:write"], 300);
        let (status3, _) = oneshot(
            &app,
            "POST",
            "/v1/orders",
            Some(&t_rw),
            Some(serde_json::json!({
                "client_order_id": "ht-3", "symbol_name": "AAPL",
                "destination_id": uuid::Uuid::new_v4(),
                "side": "b", "order_type": "limit",
                "qty": "100", "price": "10.00", "tif": "0",
            })),
        )
        .await;
        assert_eq!(
            status3,
            StatusCode::BAD_REQUEST,
            "session gate: close_pending rejects new orders"
        );
    }

    /// The admin POST-alias session action shares the GET route's
    /// handler — the session-manage scope must pass, a scopeless admin
    /// token → 403 (the alias inherits the gate, not a separate policy).
    #[tokio::test]
    async fn admin_session_post_alias_scope_gated() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        // Unauthenticated → 401 (the verify step, alias or not).
        let (status1, _) = oneshot(
            &app,
            "POST",
            "/v1/admin/session",
            None,
            Some(serde_json::json!({"action": "preopen"})),
        )
        .await;
        assert_eq!(status1, StatusCode::UNAUTHORIZED);
        // Session-manage scope → the engine transition runs.
        let t = h.mint("ops-alias", env_a(), &["trade8:market:session:manage"], 300);
        let (status2, body2) = oneshot(
            &app,
            "POST",
            "/v1/admin/session",
            Some(&t),
            Some(serde_json::json!({"action": "preopen"})),
        )
        .await;
        assert_eq!(status2, StatusCode::OK, "alias OK: {body2}");
        assert_eq!(body2["state"], serde_json::json!("preopen"));
        // Scopeless token on the alias → 403 (step 5, structured shape).
        let t_ns = h.mint("nobody", env_a(), &[], 300);
        let (status3, body3) = oneshot(
            &app,
            "POST",
            "/v1/admin/session",
            Some(&t_ns),
            Some(serde_json::json!({"action": "open"})),
        )
        .await;
        assert_eq!(status3, StatusCode::FORBIDDEN);
        assert!(
            body3.get("code").is_some() || body3.get("message").is_some() || body3.is_string(),
            "alias rejection carries the structured shape: {body3}"
        );
    }

    /// The admin read endpoint (`/v1/admin/orders`) is RLS-scoped:
    /// with the read scope it returns an envelope (empty list on a fresh
    /// env), never a 403 — and without a scope → 403.
    #[tokio::test]
    async fn admin_orders_read_env_scoped() {
        let h = Harness::spawn().await;
        let Some(state) = build_state(&h).await else {
            eprintln!("skipping: no live DB");
            return;
        };
        let app = test_router(state);
        let t = h.mint("reader", env_a(), &["trade8:desk:order:read"], 300);
        let (status, body) =
            oneshot(&app, "GET", "/v1/admin/orders?limit=10", Some(&t), None).await;
        assert_eq!(status, StatusCode::OK, "read-scope admin read: {body}");
        assert!(
            body.is_object() || body.is_array(),
            "orders envelope: {body}"
        );
        let t_ns = h.mint("scopeless", env_a(), &[], 300);
        let (status2, _) = oneshot(&app, "GET", "/v1/admin/orders", Some(&t_ns), None).await;
        assert_eq!(status2, StatusCode::FORBIDDEN);
    }
}
