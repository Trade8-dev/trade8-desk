// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! The Risk test double for the cross-service flow tests
//! (desk flow tests; test-architecture design §6). This module lives under `src/`
//! but is compiled ONLY by the desk test targets - `main.rs` never
//! references it, so the production binary excludes it entirely.
//!
//! Design (desk design §4.2): Desk's order pipeline
//! delegates ALL policy checks to Risk in ONE batch `authorize` call.
//! The double is NOT a mock of the verdict logic - it hosts Risk's REAL
//! `RiskEngine` (embedded `#[path] = "../../risk/src/core.rs"`, the exact
//! production check pipeline) behind the wire-shaped
//! `AuthorizeRequest`/`AuthorizeVerdict` contract, so the flow tests
//! exercise the true allow/deny branches (trader state, buying power,
//! restrictions) instead of canned answers.
//!
//! The certificate-presenting mTLS transport itself (https-only mTLS
//! between the desk and risk processes) is platform-infrastructure
//! coverage (Sentin8:TLS suites); this double replaces that seam per
//! design §6.

use std::sync::Arc;

use trade8::contracts::risk::LimitRow;

/// Risk's REAL check pipeline, embedded verbatim from the risk service
/// (`trade8-risk/src/core.rs`). `core.rs` imports only
/// `trade8::contracts` + `arc_swap` - both are desk dependencies, so the
/// embed needs no extra crate wiring.
///
/// Embedded via `include!` inside a feature-gated inline `mod`
/// (tree-layout-only flow tests - see `tests/flow.rs` header and
/// DESIGN.md §9): `include!` is opaque to rustfmt, so single-repo CI's
/// blocking fmt gate stays green, and the path resolves only with
/// `--features cross_repo_flows` (the aggregation-tree layout).
#[cfg(feature = "cross_repo_flows")]
pub mod risk_core {
    include!("../../trade8-risk/src/core.rs");
}

/// The Risk double: the real engine + the fixture snapshot seeders.
pub struct RiskDouble {
    pub engine: risk_core::RiskEngine,
}

impl RiskDouble {
    /// A double whose snapshot carries the given limit rows (everything
    /// else empty → trader Active, no restrictions, no halts).
    pub fn with_limits(limits: Vec<LimitRow>) -> Self {
        let engine = risk_core::RiskEngine::new();
        engine.publish(risk_core::RiskSnapshot {
            limits,
            ..risk_core::RiskSnapshot::default()
        });
        Self { engine }
    }

    /// A double seeded with per-trader states (the trader-state deny leg).
    pub fn with_states(
        states: impl IntoIterator<Item = (&'static str, trade8::contracts::TraderState)>,
    ) -> Self {
        let engine = risk_core::RiskEngine::new();
        engine.publish(risk_core::RiskSnapshot {
            trader_states: states
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ..risk_core::RiskSnapshot::default()
        });
        Self { engine }
    }
}

/// A buying-power limit row bound to everyone in the env
/// (`usr_id`/`office_id` both None - the env-level default binding).
pub fn limit(value: i128) -> LimitRow {
    LimitRow {
        usr_id: None,
        office_id: None,
        asset_class: None,
        currency: None,
        kind: trade8::contracts::RiskLimitKind::BuyingPower,
        value,
        override_value: None,
        disabled: false,
    }
}

/// The wiring-level branches of `DeskWiring::authorize_order` (desk
/// design §4.2): the not-enrolled (`risk = None`) branch and the
/// fail-closed outage branch. Modeled as an enum so the flow tests can
/// assert both branches' contracts without a live network stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskDoubleWiring {
    /// `DeskWiring { risk: None, .. }` - Risk not enrolled (local dev):
    /// structurally allow, empty trace (desk design §4.2).
    NotEnrolled,
    /// Risk unreachable: fail CLOSED - the outage rejection
    /// (`trade8_risk_trader_disabled` + `unavailable: true` param), never
    /// an allow.
    Down,
}

impl RiskDoubleWiring {
    /// The branch outcome in the desk wiring's result shape:
    /// `Ok(AuthorizeVerdict)` (allow) or `Err(Rejection)` (fail-closed).
    pub fn authorize_outcome(
        self,
    ) -> Result<trade8::contracts::risk::AuthorizeVerdict, trade8::contracts::Rejection> {
        match self {
            Self::NotEnrolled => Ok(trade8::contracts::risk::AuthorizeVerdict::allow(
                Vec::new(),
                trade8::contracts::TraderState::Active,
                None,
            )),
            Self::Down => Err(trade8::contracts::Rejection::new(
                trade8::contracts::RejectCode::RiskTraderDisabled,
            )
            .with_params(serde_json::json!({"unavailable": true}))),
        }
    }
}

/// Force the `Arc` import to stay (the double's callers share engine
/// handles across tasks the same way `RiskState` does).
#[allow(dead_code)]
type _EngineShared = Arc<risk_core::RiskEngine>;
