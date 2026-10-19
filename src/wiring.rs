// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Desk runtime wiring  - the Risk authorize call on
//! the accept path, the Feed BBO snapshot read (flatten pricing), and the
//! Sentin8:Func proxy-back host functions.
//!
//! The Risk call is the ONLY synchronous inter-service hop on the hot
//! path (master design §5.2): Desk POSTs `AuthorizeRequest` to
//! `POST {risk}/v1/authorize` over Sentin8:TLS, presenting its own
//! workload certificate in the TLS handshake (CERTFREE-4V - scope
//! `trade8:risk:authorize` is authorized by the registry map). Fail-open
//! vs fail-closed is a one-line policy: this path is **fail-closed** - a
//! Risk outage rejects with `RiskUnavailable` rather than trading
//! unguarded.

use std::sync::Arc;

use trade8::contracts::order::NewOrderRequest;
use trade8::contracts::risk::{AuthorizeRequest, AuthorizeVerdict, Intent};
use trade8::contracts::{Order, RejectCode, Rejection};
use uuid::Uuid;

use crate::engine::DeskEngine;

/// The Risk caller + Feed reader held in Desk state.
#[derive(Clone)]
pub struct DeskWiring {
    /// Risk authorize caller (None in local dev - no Risk enrolled;
    /// submits then self-authorize structurally only).
    pub risk: Option<trade8::client::SharedCaller>,
    /// Feed BBO reader (None → no marks; flatten uses order prices).
    pub feed: Option<trade8::client::SharedCaller>,
}

impl DeskWiring {
    /// Build from env: `TRADE8_RISK_URL` / `TRADE8_FEED_URL` (captain
    /// injects service URLs at boot; empty/absent → feature off).
    pub async fn build(auth_issuer: &str) -> Self {
        let risk = match std::env::var("TRADE8_RISK_URL") {
            Ok(url) if !url.is_empty() => {
                match trade8::client::build_caller(auth_issuer, &url, "trade8:desk").await {
                    Ok(c) => Some(Arc::new(c)),
                    Err(e) => {
                        tracing::warn!(error = %e, "desk: risk caller unavailable (risk checks off)");
                        None
                    }
                }
            }
            _ => None,
        };
        let feed = match std::env::var("TRADE8_FEED_URL") {
            Ok(url) if !url.is_empty() => {
                match trade8::client::build_caller(auth_issuer, &url, "trade8:desk:feed").await {
                    Ok(c) => Some(Arc::new(c)),
                    Err(e) => {
                        tracing::warn!(error = %e, "desk: feed reader unavailable (no bbo marks)");
                        None
                    }
                }
            }
            _ => None,
        };
        DeskWiring { risk, feed }
    }

    /// The  authorize call. Build the request from the pending order,
    /// POST to Risk, map the verdict. Risk unavailable → fail-closed.
    pub async fn authorize_order(
        &self,
        env_id: Uuid,
        engine: &DeskEngine,
        order: &Order,
        intent: Intent,
    ) -> Result<AuthorizeVerdict, Rejection> {
        let Some(risk) = self.risk.as_ref() else {
            // Local dev / Risk not enrolled: structurally allow (the
            // engine's own checks already passed).
            return Ok(AuthorizeVerdict::allow(
                Vec::new(),
                order_side_state(order),
                None,
            ));
        };
        // Mark context from Feed ( read path; missing mark is fine -
        // Risk falls back to price).
        let mark_raw = self.bbo_mark(&order.symbol_name).await;
        let req = AuthorizeRequest {
            env_id,
            usr_id: order.usr_id.clone(),
            office_id: order.office_id.clone(),
            symbol_id: order.symbol_id,
            symbol_name: order.symbol_name.clone(),
            market_id: order.market_id,
            destination_id: order.destination_id,
            side_char: order.side.as_char(),
            qty: order.qty.raw,
            price_raw: order.price.map(|p| p.raw),
            price_scale: order.price.map(|p| p.scale).unwrap_or(4),
            mark_raw,
            intent,
            current_position: engine.position_of(&order.usr_id, order.symbol_id),
            // Halt context from the catalog feed_state (Risk mirrors Feed
            // halted symbols itself; this is the Desk-side observation).
            halted: engine
                .catalog
                .symbol(&order.symbol_name)
                .map(|s| s.feed_state != "act")
                .unwrap_or(false),
            last_print_raw: None,
            prior_print_raw: None,
            order_type: Some(trade8::contracts::OrderType::serde_str(order.order_type).to_string()),
            tif: Some(
                serde_json::to_value(order.tif)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_else(|| "0".into()),
            ),
            is_cancel: matches!(intent, Intent::Cancel),
        };
        match risk
            .post_json::<AuthorizeVerdict>("/v1/authorize", &req)
            .await
        {
            Ok(v) => Ok(v),
            Err(e) => {
                // Fail-closed (design §4.2): a Risk outage must never
                // trade unguarded. 503-shaped rejection.
                tracing::warn!(error = %e, "desk: risk authorize call failed - fail closed");
                Err(Rejection::new(RejectCode::RiskTraderDisabled)
                    .with_params(serde_json::json!({"unavailable": true})))
            }
        }
    }

    /// read the consolidated BBO mark for a symbol from Feed.
    /// Returns None when Feed is absent or has no quote yet.
    pub async fn bbo_mark(&self, symbol: &str) -> Option<i128> {
        let feed = self.feed.as_ref()?;
        #[derive(serde::Deserialize)]
        struct Quote {
            #[serde(default)]
            bid_raw: Option<i128>,
            #[serde(default)]
            ask_raw: Option<i128>,
            #[serde(default)]
            last_raw: Option<i128>,
        }
        let q = feed
            .get_json::<Quote>(&format!("/v1/quotes/{symbol}"))
            .await
            .ok()?;
        q.last_raw.or_else(|| match (q.bid_raw, q.ask_raw) {
            (Some(b), Some(a)) => Some((b + a) / 2),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            _ => None,
        })
    }
}

/// Trader state observed locally (the local-allow path only - the real
/// verdict carries Risk's own observation).
fn order_side_state(_order: &Order) -> trade8::contracts::TraderState {
    trade8::contracts::TraderState::Active
}

/// the Func proxy-back host functions. The executor (a WASM
/// strategy module) calls back for domain data it cannot derive from the
/// event payload:
/// - `get_order` GET /v1/internal/func-host/orders/{id}
/// - `submit_order` POST /v1/internal/func-host/orders
/// - `cancel_order` POST /v1/internal/func-host/orders/{id}/cancel
///   Func's own workload identity only - verified by the Func
///   proxy-back gate.
pub mod func_host {
    use super::*;

    /// One host-function request (Func → Desk proxy-back).
    #[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
    pub struct HostSubmit {
        pub usr_id: String,
        pub on_behalf_of: Option<String>,
        #[serde(flatten)]
        pub req: NewOrderRequest,
    }
}
