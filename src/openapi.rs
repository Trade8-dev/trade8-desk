// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI spec for Trade8:Desk.
//!
//! utoipa annotations on the binary's route handlers + the shared Shield
//! policy-admin surface (`sentin8::shell::policy_*` wrappers). See
//! `sentin8/design/sentin8-openapi-coverage-design.md` §2.

use crate::store;
use crate::wiring;
use utoipa::OpenApi;

pub mod w {


    /// Spec wrapper for the inline health closure (binary has no named fn).
    #[utoipa::path(
        get,
        path = "/v1/health",
        responses(
            (status = 200, description = "Service is healthy", body = crate::HealthInfo),
        ),
        security(),
        tag = "desk",
    )]
    pub async fn desk_health() {}

}

/// OpenAPI spec for the desk API.
#[derive(OpenApi)]
#[openapi(
    paths(
crate::submit_order,
crate::list_orders,
crate::get_order,
crate::cancel_order,
crate::replace_order,
crate::submit_batch,
crate::flatten,
crate::session_state,
crate::session_action,
crate::list_symbols,
crate::borrow_read,
crate::borrow_mutate,
crate::ws_entry,
crate::func_host_submit,
crate::func_host_get,
crate::func_host_cancel,
crate::admin_ui::get_admin_ui,
crate::admin_ui::admin_orders,
crate::admin_ui::admin_overview,
crate::admin_ui::admin_symbols,
crate::admin_ui::admin_markets,
crate::admin_ui::admin_destinations,
crate::admin_ui::admin_offices,
crate::admin_ui::admin_borrow,
crate::admin_ui::admin_session,
crate::admin_ui::admin_session_action,
crate::openapi::w::desk_health,
        sentin8::shell::policy_list_sets,
        sentin8::shell::policy_get_set,
        sentin8::shell::policy_put_set,
        sentin8::shell::policy_delete_set,
        sentin8::shell::policy_test_set,
        sentin8::shell::policy_mode_enforce,
        sentin8::shell::policy_mode_shadow,
        sentin8::shell::policy_mode_disable,
        sentin8::shell::policy_list_relations,
        sentin8::shell::policy_put_relation,
        sentin8::shell::policy_delete_relation,
        sentin8::shell::policy_check_relation,
        sentin8::shell::policy_list_attributes,
        sentin8::shell::policy_put_attribute,
        sentin8::shell::policy_delete_attribute,
    ),
    components(schemas(
        crate::HealthInfo,
        crate::SubmitOrderResponse,
        crate::BatchResponse,
        crate::MutationAck,
        crate::ReplaceAck,
        crate::FlattenAck,
        crate::SessionStateResponse,
        crate::SessionActionRequest,
        crate::SymbolsResponse,
        crate::BorrowPool,
        crate::BorrowMutationAck,
        crate::HostSubmitResponse,
        trade8::contracts::Order,
        trade8::contracts::OrderSummary,
        trade8::contracts::NewOrderRequest,
        trade8::contracts::ReplaceOrderRequest,
        trade8::contracts::FlattenRequest,
        store::BorrowMutation,
        wiring::func_host::HostSubmit,
        sentin8::rfw::AdminUiResponse,
    )),
    security(),
    tags(
        (name = "desk", description = "Trade8:Desk - order management"),
        (name = "trade8-admin", description = "Trade8 admin surface (RFW)"),
        (name = "policy", description = "Shield policy administration (shared surface)"),
    ),
)]
pub struct DeskOpenApi;
