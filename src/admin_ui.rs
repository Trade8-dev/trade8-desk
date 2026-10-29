// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Trade8:Desk admin UI - RFW page definitions + data endpoints.
//!
//! Pages are served at `GET /v1/admin/ui/{page}` and rendered by
//! Sentin8:Dash at runtime (zero compile-time vertical knowledge -
//! architecture-principles.md §4.15). The RFW renderer only supports
//! POST for mutations, so update/delete carry POST alias routes.
//! Icons are Material Icons code points (hex strings).

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use sentin8::rfw::page::AdminPage;
use sentin8::rfw::response::AdminUiResponse;
use sentin8::rfw::widget::*;
use uuid::Uuid;

use crate::DeskState;

fn col(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: false,
        width: None,
        format: None,
    }
}
fn cols(key: &str, label: &str) -> WidgetDataTableColumn {
    WidgetDataTableColumn {
        key: key.into(),
        label: label.into(),
        sortable: true,
        width: None,
        format: None,
    }
}
fn table(columns: Vec<WidgetDataTableColumn>, endpoint: &str) -> RemoteWidget {
    RemoteWidget::DataTable(WidgetDataTable {
        columns,
        data_endpoint: endpoint.into(),
        sortable: true,
        filterable: true,
        page_size: 25,
        empty_message: Some("No data.".into()),
        row_link: None,
        row_actions: vec![],
    })
}
fn card(title: &str, body: &str) -> RemoteWidget {
    RemoteWidget::Card(WidgetCard {
        title: Some(title.into()),
        subtitle: None,
        children: vec![RemoteWidget::Text(WidgetText {
            text: body.into(),
            text_type: WidgetTextType::Body,
            align: TextAlign::Left,
            max_lines: None,
        })],
        actions: vec![],
    })
}
#[utoipa::path(
    get,
    path = "/v1/admin/ui/{page}",
    responses(
        (status = 200, description = "OK", body = sentin8::rfw::AdminUiResponse),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/ui/{page}` - the RFW definitions.
pub async fn get_admin_ui(
    State(_st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
    Path(page): Path<String>,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    // Admin pages render for env admins; the data endpoints re-check
    // per-table scopes. The page shell only needs an authenticated admin.
    let _a = sentin8::shell::verify_token(&_st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &_st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let def = match page.as_str() {
        "overview" => AdminPage::new("Desk Overview")
            .widget(card(
                "Desk",
                "Order lifecycle, working orders, session state, borrow pool.",
            ))
            .widget(table(
                vec![
                    col("state", "Session"),
                    col("working", "Working Orders"),
                    col("today", "Orders Today"),
                    col("parked", "Overnight Parked"),
                ],
                "/v1/admin/overview",
            ))
            .build(),
        "orders" => AdminPage::new("Orders")
            .widget(table(
                vec![
                    cols("order_id", "Order ID"),
                    cols("client_order_id", "Client ID"),
                    cols("symbol_name", "Symbol"),
                    cols("side", "Side"),
                    cols("qty", "Qty"),
                    cols("leaves_qty", "Leaves"),
                    cols("state", "State"),
                    cols("usr_id", "Trader"),
                    cols("office_id", "Office"),
                    cols("submitted_at", "Submitted"),
                ],
                "/v1/admin/orders",
            ))
            .build(),
        "symbols" => AdminPage::new("Symbols")
            .widget(table(
                vec![
                    cols("name", "Symbol"),
                    cols("price_scale", "Price Scale"),
                    cols("lot_size", "Lot"),
                    cols("feed_state", "Feed"),
                    cols("instrument_state", "State"),
                ],
                "/v1/admin/symbols",
            ))
            .build(),
        "markets" => AdminPage::new("Markets")
            .widget(table(
                vec![
                    cols("code", "Code"),
                    cols("description", "Description"),
                    cols("asset_class", "Asset Class"),
                    cols("timezone", "Timezone"),
                    cols("uptick_rule", "Uptick Rule"),
                ],
                "/v1/admin/markets",
            ))
            .build(),
        "destinations" => AdminPage::new("Destinations")
            .widget(table(
                vec![
                    cols("name", "Destination"),
                    cols("protocol", "Protocol"),
                    cols("endpoint_url", "Endpoint"),
                    cols("min_size", "Min Size"),
                    cols("enable", "Enabled"),
                ],
                "/v1/admin/destinations",
            ))
            .build(),
        "offices" => AdminPage::new("Offices")
            .widget(table(
                vec![
                    cols("office_id", "Office"),
                    cols("name", "Name"),
                    cols("status", "Status"),
                    cols("country_code", "Country"),
                ],
                "/v1/admin/offices",
            ))
            .build(),
        "borrow" => AdminPage::new("Borrow Pool")
            .widget(table(
                vec![
                    cols("symbol_id", "Symbol ID"),
                    cols("total_shares", "Total"),
                    cols("available_shares", "Available"),
                ],
                "/v1/admin/borrow",
            ))
            .build(),
        "session" => AdminPage::new("Session")
            .widget(card(
                "Session",
                "Trading session state machine: preopen → open → close_pending → closed.",
            ))
            .widget(table(
                vec![col("state", "State"), col("working", "Working Orders")],
                "/v1/admin/session",
            ))
            .build(),
        _ => {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("Unknown admin UI page: {page}"),
            ))
        }
    };
    Ok(Json(
        serde_json::to_value(AdminUiResponse::page(def)).unwrap_or_default(),
    ))
}

// ------------------------------------------------------------------------
// Data endpoints (POST-alias mutations per the RFW POST-only rule).
// ------------------------------------------------------------------------

type Qp = std::collections::HashMap<String, String>;

fn q_i64(q: &Qp, key: &str, default: i64) -> i64 {
    q.get(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}
#[utoipa::path(
    get,
    path = "/v1/admin/orders",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/orders` - env-wide order rows (admin scope).
pub async fn admin_orders(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
    Query(q): Query<Qp>,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let env = a.env_id();
    let limit = q_i64(&q, "limit", 100).min(500);
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // Named row type: the admin blotter row (11 columns) reads far better
    // with an alias than with an inline tuple.
    type AdminOrderRow = (
        Uuid,
        String,
        Option<String>,
        String,
        String,
        i64,
        i64,
        String,
        String,
        String,
        chrono::DateTime<chrono::Utc>,
    );
    let rows: Vec<AdminOrderRow> = sqlx::query_as(
        r#"SELECT o.order_id, o.client_order_id, s.name, o.side, o.state,
                      o.qty, o.leaves_qty, o.usr_id, o.office_id, o.tif, o.submitted_at
               FROM desk."order" o
               JOIN desk.symbol s ON s.symbol_id = o.symbol_id
               WHERE o.env_id = $1
               ORDER BY o.submitted_at DESC LIMIT $2"#,
    )
    .bind(env)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(
                    order_id,
                    client_order_id,
                    symbol_name,
                    side,
                    state,
                    qty,
                    leaves_qty,
                    usr_id,
                    office_id,
                    tif,
                    submitted_at,
                )| {
                    serde_json::json!({
                        "order_id": order_id, "client_order_id": client_order_id,
                        "symbol_name": symbol_name, "side": side, "state": state,
                        "qty": qty, "leaves_qty": leaves_qty, "usr_id": usr_id,
                        "office_id": office_id, "tif": tif, "submitted_at": submitted_at,
                    })
                },
            )
            .collect::<Vec<_>>(),
    ))
}
#[utoipa::path(
    get,
    path = "/v1/admin/overview",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/overview` - session + counts card.
pub async fn admin_overview(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let (working, today, parked): (i64, i64, i64) = sqlx::query_as(
        r#"SELECT
             count(*) FILTER (WHERE state IN ('accepted','routed','partially_filled')),
             count(*),
             count(*) FILTER (WHERE state = 'overnight_parked')
           FROM desk."order" WHERE env_id = $1"#,
    )
    .bind(env)
    .fetch_one(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(vec![serde_json::json!({
        "state": st.engine.session_state().as_str(),
        "working": working, "today": today, "parked": parked,
    })]))
}
#[utoipa::path(
    get,
    path = "/v1/admin/symbols",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/symbols`
pub async fn admin_symbols(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, a.env_id()).await;
    let rows = st.engine.symbols_json();
    Ok(Json(rows))
}
#[utoipa::path(
    get,
    path = "/v1/admin/markets",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/markets`
pub async fn admin_markets(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let rows: Vec<(Uuid, String, String, String, String, String)> = sqlx::query_as(
        r#"SELECT market_id, code, description, asset_class, timezone, uptick_rule
           FROM desk.market WHERE env_id = $1 ORDER BY code"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(rows
        .into_iter()
        .map(
            |(market_id, code, description, asset_class, timezone, uptick_rule)| {
                serde_json::json!({
                    "market_id": market_id, "code": code, "description": description,
                    "asset_class": asset_class, "timezone": timezone, "uptick_rule": uptick_rule,
                })
            }
        )
        .collect::<Vec<_>>()))
}
#[utoipa::path(
    get,
    path = "/v1/admin/destinations",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/destinations`
pub async fn admin_destinations(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    // credentials_ref is a secret *reference* - safe to show masked.
    let rows: Vec<(Uuid, String, String, String, i64, bool)> = sqlx::query_as(
        r#"SELECT destination_id, name, protocol, endpoint_url, min_size, enable
           FROM desk.destination WHERE env_id = $1 ORDER BY name"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(destination_id, name, protocol, endpoint_url, min_size, enable)| {
                    serde_json::json!({
                        "destination_id": destination_id, "name": name,
                        "protocol": protocol, "endpoint_url": endpoint_url,
                        "min_size": min_size, "enable": enable,
                    })
                },
            )
            .collect::<Vec<_>>(),
    ))
}
#[utoipa::path(
    get,
    path = "/v1/admin/offices",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/offices`
pub async fn admin_offices(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:desk:order:read"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(
        r#"SELECT office_id, name, status, country_code
           FROM desk.office WHERE env_id = $1 ORDER BY office_id"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(|(office_id, name, status, country_code)| {
                serde_json::json!({
                    "office_id": office_id, "name": name,
                    "status": status, "country_code": country_code,
                })
            })
            .collect::<Vec<_>>(),
    ))
}
#[utoipa::path(
    get,
    path = "/v1/admin/borrow",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/borrow` - short pool + category + exempt rows.
pub async fn admin_borrow(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:risk:borrow:write"),
    )
    .await?;
    let env = a.env_id();
    let mut conn = st
        .core
        .pool
        .acquire()
        .await
        .map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut conn, env).await;
    let rows: Vec<(Uuid, i64, i64)> = sqlx::query_as(
        r#"SELECT symbol_id, total_shares, available_shares
           FROM desk.short_pool WHERE env_id = $1"#,
    )
    .bind(env)
    .fetch_all(&mut *conn)
    .await
    .map_err(sentin8::shell::err_db)?;
    Ok(Json(
        rows.into_iter()
            .map(|(symbol_id, total_shares, available_shares)| {
                serde_json::json!({
                    "symbol_id": symbol_id,
                    "total_shares": total_shares,
                    "available_shares": available_shares,
                })
            })
            .collect::<Vec<_>>(),
    ))
}
#[utoipa::path(
    get,
    path = "/v1/admin/session",
    responses(
        (status = 200, description = "Admin rows", body = Vec<serde_json::Value>),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `GET /v1/admin/session`
pub async fn admin_session(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<serde_json::Value>>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:market:session:manage"),
    )
    .await?;
    Ok(Json(vec![serde_json::json!({
        "state": st.engine.session_state().as_str(),
        "working": st.engine.working_count(),
    })]))
}
#[utoipa::path(
    post,
    path = "/v1/admin/session",
    responses(
        (status = 200, description = "Session state", body = crate::SessionStateResponse),

        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "trade8-admin",
)]

/// `POST /v1/admin/session` - RFW POST-alias for the session action
/// (the renderer can't issue raw POST bodies to /v1/session ops).
pub async fn admin_session_action(
    State(st): State<std::sync::Arc<DeskState>>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<crate::SessionActionRequest>,
) -> Result<Json<crate::SessionStateResponse>, (axum::http::StatusCode, String)> {
    let _a = sentin8::shell::verify_token(&st.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &st.core.admin.policy,
        &_a,
        &headers,
        sentin8::shield::EvalTarget::new("trade8:market:session:manage"),
    )
    .await?;
    let action = body.action.as_str();
    let state = st.engine.session_action(action).await;
    Ok(Json(crate::SessionStateResponse {
        state: state.as_str().to_string(),
    }))
}
