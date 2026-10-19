// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Desk engine - the order state machine core.
//!
//! Single-writer per order id: submit/cancel/replace/flatten transition the
//! working map and append to the event journal; this module owns the pure
//! state logic, `store.rs` owns DB persistence (journal + outbox in one tx).
//!
//! Design: `../desk/DESIGN.md` §4.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use trade8::contracts::{
    FlattenRequest, NewOrderRequest, Order, OrderState, OrderSummary, OrderType, RejectCode,
    Rejection, ReplaceOrderRequest, SessionState, Side, TimeInForce,
};
use trade8::fixed::{Price, Qty};
use uuid::Uuid;

use crate::borrow::BorrowPool;
use crate::catalog::Catalog;
use crate::session::SessionClock;

/// The engine's durable state (in-memory working set; DB mirror lands with
/// the tokio pass - event journal shape is fixed here).
#[derive(Default)]
pub struct DeskEngineInner {
    /// Working orders by id - the session read path.
    pub orders: HashMap<Uuid, Order>,
    /// Append-only journal (seq per env in the DB form).
    pub events: Vec<(Uuid, OrderState, OrderState, String, String)>,
    /// client_order_id → order_id (idempotency).
    pub by_client_id: HashMap<String, Uuid>,
}

/// The Desk engine handle.
pub struct DeskEngine {
    inner: Arc<RwLock<DeskEngineInner>>,
    pub catalog: Catalog,
    pub borrow: BorrowPool,
    pub session: SessionClock,
}

impl DeskEngine {
    pub fn new() -> Self {
        DeskEngine {
            inner: Arc::new(RwLock::new(DeskEngineInner::default())),
            catalog: Catalog::default(),
            borrow: BorrowPool::default(),
            session: SessionClock::default(),
        }
    }

    /// Submit a new order: structural validation → (Risk lands with the
    /// hot-path wiring; the structural + session + duplicate checks are
    /// the accept path's front) → park in the working map.
    pub fn submit_with_actor(
        &self,
        usr_id: &str,
        office_id: &str,
        req: NewOrderRequest,
    ) -> Result<Order, Rejection> {
        // Session gate: no new orders outside Open.
        if !self.session.state().accepts_orders() {
            return Err(Rejection::new(RejectCode::SessionNotOpen));
        }
        // Duplicate client id (idempotent resubmit returns the original).
        {
            let inner = self.inner.read();
            if let Some(existing) = inner.by_client_id.get(&req.client_order_id) {
                if let Some(o) = inner.orders.get(existing) {
                    return Ok(o.clone());
                }
            }
        }
        // Symbol + destination resolution (catalog cache).
        let sym = self
            .catalog
            .symbol(&req.symbol_name)
            .ok_or_else(|| Rejection::new(RejectCode::SymbolUnknown))?;
        if sym.feed_state != "act" || sym.instrument_state != "active" {
            return Err(Rejection::new(RejectCode::SymbolInactive));
        }
        let dest = self
            .catalog
            .destination(req.destination_id)
            .ok_or_else(|| Rejection::new(RejectCode::DestinationInvalid))?;
        if !dest.enable {
            return Err(Rejection::new(RejectCode::DestinationInvalid));
        }
        // Quantity + price validation ( structural).
        let qty = Qty::parse_decimal(&req.qty, 0)
            .ok_or_else(|| Rejection::new(RejectCode::QtyInvalid))?;
        if qty.raw == 0 || qty.raw < dest.min_size as u64 {
            return Err(Rejection::new(RejectCode::QtyInvalid)
                .with_params(serde_json::json!({"min_size": dest.min_size})));
        }
        let price = match req.price.as_deref() {
            None => None,
            Some(p) => Some(
                Price::parse_decimal(p, sym.price_scale)
                    .ok_or_else(|| Rejection::new(RejectCode::PriceInvalid))?,
            ),
        };
        // Type/price consistency (limit + stop-limit need prices).
        if req.order_type.needs_price() && price.is_none() {
            return Err(Rejection::new(RejectCode::TypeNeedsPrice));
        }
        // Tick validation (bands from the symbol's tick rule).
        if let Some(p) = price {
            let tick = self.catalog.tick_for(sym.tick_rule_id, p);
            if !trade8::fixed::is_on_tick(p, tick) {
                return Err(Rejection::new(RejectCode::PriceOffTick)
                    .with_params(serde_json::json!({"tick": tick, "price_raw": p.raw})));
            }
        }
        // Short-locate reservation: reserve before accept.
        if req.side.is_short_opening() && !self.borrow.is_exempt(sym.symbol_id) {
            let etb = self.borrow.is_etb(sym.symbol_id);
            if !etb && !self.borrow.reserve(sym.symbol_id, qty.raw as i64) {
                return Err(Rejection::new(RejectCode::RiskLocateUnavailable));
            }
        }

        let order = self.park_new(usr_id, office_id, &req, sym, qty, price);
        let order = Order {
            state: OrderState::Accepted,
            ..order
        };
        let mut inner = self.inner.write();
        let id = order.order_id;
        inner.by_client_id.insert(req.client_order_id, id);
        inner.events.push((
            id,
            OrderState::Pending,
            OrderState::Accepted,
            usr_id.to_string(),
            "submit".into(),
        ));
        inner.orders.insert(id, order.clone());
        Ok(order)
    }

    /// The two-phase submit for the Risk-gated path: structural
    /// validation (identical to `submit_with_actor`) builds a **Pending**
    /// order parked in the working map, WITHOUT the accept transition -
    /// the caller then invokes Risk `authorize` and calls
    /// [`Self::accept_pending`] or [`Self::reject_pending`].
    ///
    /// Locate reservation happens on the accept leg (only when Risk
    /// allows) so a deny never holds shares.
    pub fn submit_pending(
        &self,
        usr_id: &str,
        office_id: &str,
        req: NewOrderRequest,
    ) -> Result<Order, Rejection> {
        if !self.session.state().accepts_orders() {
            return Err(Rejection::new(RejectCode::SessionNotOpen));
        }
        {
            let inner = self.inner.read();
            if let Some(existing) = inner.by_client_id.get(&req.client_order_id) {
                if let Some(o) = inner.orders.get(existing) {
                    return Ok(o.clone());
                }
            }
        }
        let sym = self
            .catalog
            .symbol(&req.symbol_name)
            .ok_or_else(|| Rejection::new(RejectCode::SymbolUnknown))?;
        if sym.feed_state != "act" || sym.instrument_state != "active" {
            return Err(Rejection::new(RejectCode::SymbolInactive));
        }
        let dest = self
            .catalog
            .destination(req.destination_id)
            .ok_or_else(|| Rejection::new(RejectCode::DestinationInvalid))?;
        if !dest.enable {
            return Err(Rejection::new(RejectCode::DestinationInvalid));
        }
        let qty = Qty::parse_decimal(&req.qty, 0)
            .ok_or_else(|| Rejection::new(RejectCode::QtyInvalid))?;
        if qty.raw == 0 || qty.raw < dest.min_size as u64 {
            return Err(Rejection::new(RejectCode::QtyInvalid)
                .with_params(serde_json::json!({"min_size": dest.min_size})));
        }
        let price = match req.price.as_deref() {
            None => None,
            Some(p) => Some(
                Price::parse_decimal(p, sym.price_scale)
                    .ok_or_else(|| Rejection::new(RejectCode::PriceInvalid))?,
            ),
        };
        if req.order_type.needs_price() && price.is_none() {
            return Err(Rejection::new(RejectCode::TypeNeedsPrice));
        }
        if let Some(p) = price {
            let tick = self.catalog.tick_for(sym.tick_rule_id, p);
            if !trade8::fixed::is_on_tick(p, tick) {
                return Err(Rejection::new(RejectCode::PriceOffTick)
                    .with_params(serde_json::json!({"tick": tick, "price_raw": p.raw})));
            }
        }
        let order = self.park_new(usr_id, office_id, &req, sym, qty, price);
        let mut inner = self.inner.write();
        let id = order.order_id;
        inner.by_client_id.insert(req.client_order_id, id);
        inner.orders.insert(id, order.clone());
        Ok(order)
    }

    /// Build the parked Order struct (shared by both submit legs; the
    /// caller decides the initial state).
    fn park_new(
        &self,
        usr_id: &str,
        office_id: &str,
        req: &NewOrderRequest,
        sym: crate::catalog::SymbolRow,
        qty: Qty,
        price: Option<Price>,
    ) -> Order {
        Order {
            order_id: Uuid::now_v7(),
            client_order_id: req.client_order_id.clone(),
            chain_root_id: Uuid::now_v7(),
            replaces_order_id: None,
            usr_id: usr_id.to_string(),
            office_id: office_id.to_string(),
            symbol_id: sym.symbol_id,
            symbol_name: sym.name.clone(),
            market_id: sym.market_id,
            destination_id: req.destination_id,
            side: req.side,
            order_type: req.order_type,
            tif: req.tif,
            qty,
            price,
            stop_price: None,
            state: OrderState::Pending,
            leaves_qty: qty,
            filled_qty: Qty::shares(0),
            flags: req.flags.clone().unwrap_or(serde_json::Value::Null),
            on_behalf_of: req.on_behalf_of.clone(),
            submitted_at: Utc::now(),
            terminal_at: None,
        }
    }

    /// Accept a pending order (Risk allow): Pending → Accepted, locate
    /// reservation for short opens, journal the transition.
    pub fn accept_pending(&self, order_id: Uuid, actor: &str) -> Result<Order, Rejection> {
        let mut inner = self.inner.write();
        let o = inner
            .orders
            .get_mut(&order_id)
            .ok_or_else(|| Rejection::new(RejectCode::OrderNotFound))?;
        if o.state != OrderState::Pending {
            return Err(Rejection::new(RejectCode::OrderNotCancellable));
        }
        // Locate reservation on the accept leg only: a deny never
        // holds shares.
        if o.side.is_short_opening() && !self.borrow.is_exempt(o.symbol_id) {
            let etb = self.borrow.is_etb(o.symbol_id);
            if !etb && !self.borrow.reserve(o.symbol_id, o.qty.raw as i64) {
                return Err(Rejection::new(RejectCode::RiskLocateUnavailable));
            }
        }
        o.state = OrderState::Accepted;
        let accepted = o.clone();
        inner.events.push((
            order_id,
            OrderState::Pending,
            OrderState::Accepted,
            actor.to_string(),
            "submit".into(),
        ));
        Ok(accepted)
    }

    /// Reject a pending order (Risk deny): Pending → Rejected with the
    /// mapped code; the trace rides the rejection event params.
    pub fn reject_pending(&self, order_id: Uuid, _code: RejectCode) -> Result<Order, Rejection> {
        let mut inner = self.inner.write();
        let o = inner
            .orders
            .get_mut(&order_id)
            .ok_or_else(|| Rejection::new(RejectCode::OrderNotFound))?;
        if o.state != OrderState::Pending {
            return Err(Rejection::new(RejectCode::OrderNotCancellable));
        }
        o.state = OrderState::Rejected;
        o.terminal_at = Some(Utc::now());
        let rejected = o.clone();
        inner.events.push((
            order_id,
            OrderState::Pending,
            OrderState::Rejected,
            "risk".into(),
            "risk_deny".into(),
        ));
        Ok(rejected)
    }

    /// Cancel a working order (user or manager - scope enforced upstream).
    pub fn cancel_with_actor(&self, actor: &str, order_id: Uuid) -> Result<Order, Rejection> {
        let mut inner = self.inner.write();
        let Some(order) = inner.orders.get_mut(&order_id) else {
            return Err(Rejection::new(RejectCode::OrderNotFound));
        };
        if !self.session.state().accepts_cancels() {
            return Err(Rejection::new(RejectCode::SessionNotOpen));
        }
        let cancellable = matches!(
            order.state,
            OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
        );
        if !cancellable {
            return Err(Rejection::new(RejectCode::OrderNotCancellable));
        }
        let prev = order.state;
        let symbol_id = order.symbol_id;
        let short_opening = order.side.is_short_opening();
        let remaining = order.leaves_qty.raw as i64;
        order.state = OrderState::Cancelled;
        order.leaves_qty = Qty::shares(0);
        order.terminal_at = Some(Utc::now());
        // Release the locate reservation for the unfilled remainder.
        if short_opening {
            self.borrow.release(symbol_id, remaining);
        }
        let cancelled = order.clone();
        inner.events.push((
            order_id,
            prev,
            OrderState::Cancelled,
            actor.to_string(),
            "cancel".into(),
        ));
        Ok(cancelled)
    }

    /// Cancel-replace: old → Replaced, new chained order.
    pub fn replace_with_actor(
        &self,
        actor: &str,
        order_id: Uuid,
        req: ReplaceOrderRequest,
    ) -> Result<Order, Rejection> {
        let (chain_root_id, symbol_id, side, market_id, destination_id, office_id, usr_id) = {
            let inner = self.inner.read();
            let order = inner
                .orders
                .get(&order_id)
                .ok_or_else(|| Rejection::new(RejectCode::OrderNotFound))?;
            if !matches!(
                order.state,
                OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
            ) {
                return Err(Rejection::new(RejectCode::OrderNotCancellable));
            }
            (
                order.chain_root_id,
                order.symbol_id,
                order.side,
                order.market_id,
                order.destination_id,
                order.office_id.clone(),
                order.usr_id.clone(),
            )
        };
        // Build the replacement as a fresh order in the same chain.
        let sym = self
            .catalog
            .symbol_by_id(symbol_id)
            .ok_or_else(|| Rejection::new(RejectCode::SymbolUnknown))?;
        let qty = Qty::parse_decimal(&req.qty, 0)
            .ok_or_else(|| Rejection::new(RejectCode::QtyInvalid))?;
        if qty.raw == 0 {
            return Err(Rejection::new(RejectCode::QtyInvalid));
        }
        let price = match req.price.as_deref() {
            None => None,
            Some(p) => Some(
                Price::parse_decimal(p, sym.price_scale)
                    .ok_or_else(|| Rejection::new(RejectCode::PriceInvalid))?,
            ),
        };
        let new_order = Order {
            order_id: Uuid::now_v7(),
            client_order_id: req.client_order_id.clone(),
            chain_root_id,
            replaces_order_id: Some(order_id),
            usr_id,
            office_id,
            symbol_id,
            symbol_name: sym.name.clone(),
            market_id,
            destination_id,
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Day,
            qty,
            price,
            stop_price: None,
            state: OrderState::Accepted,
            leaves_qty: qty,
            filled_qty: Qty::shares(0),
            flags: serde_json::Value::Null,
            on_behalf_of: None,
            submitted_at: Utc::now(),
            terminal_at: None,
        };
        let mut inner = self.inner.write();
        // old → replaced
        if let Some(old) = inner.orders.get_mut(&order_id) {
            old.state = OrderState::Replaced;
            old.terminal_at = Some(Utc::now());
            old.leaves_qty = Qty::shares(0);
            inner.events.push((
                order_id,
                OrderState::Accepted,
                OrderState::Replaced,
                actor.to_string(),
                "replace".into(),
            ));
        }
        let id = new_order.order_id;
        inner.by_client_id.insert(req.client_order_id, id);
        inner.orders.insert(id, new_order.clone());
        Ok(new_order)
    }

    /// Session-close sweep: cancel day-TIF remnants, park
    /// overnight-TIF, return (cancelled, parked) counts.
    pub fn close_session_sweep(&self) -> (usize, usize) {
        let mut inner = self.inner.write();
        let mut cancelled = 0;
        let mut parked = 0;
        let ids: Vec<Uuid> = inner
            .orders
            .iter()
            .filter(|(_, o)| {
                matches!(
                    o.state,
                    OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
                )
            })
            .map(|(k, _)| *k)
            .collect();
        for id in ids {
            let Some(o) = inner.orders.get_mut(&id) else {
                continue;
            };
            let prev = o.state;
            if o.tif.parks_overnight() {
                o.state = OrderState::OvernightParked;
                parked += 1;
                inner.events.push((
                    id,
                    prev,
                    OrderState::OvernightParked,
                    "system".into(),
                    "session_close_park".into(),
                ));
            } else {
                // day/gtc remnants cancel at close (day) or leave (gtc -
                // venue semantics; the Desk cancels day only)
                if o.tif.sweeps_at_close() {
                    o.state = OrderState::Cancelled;
                    o.terminal_at = Some(Utc::now());
                    o.leaves_qty = Qty::shares(0);
                    cancelled += 1;
                    inner.events.push((
                        id,
                        prev,
                        OrderState::Cancelled,
                        "system".into(),
                        "session_close_cancel".into(),
                    ));
                }
            }
        }
        (cancelled, parked)
    }

    /// Restore parked overnight orders at preopen.
    pub fn restore_overnight(&self) -> usize {
        let mut inner = self.inner.write();
        let ids: Vec<Uuid> = inner
            .orders
            .iter()
            .filter(|(_, o)| o.state == OrderState::OvernightParked)
            .map(|(k, _)| *k)
            .collect();
        let n = ids.len();
        for id in ids {
            if let Some(o) = inner.orders.get_mut(&id) {
                let prev = o.state;
                o.state = OrderState::Pending;
                inner.events.push((
                    id,
                    prev,
                    OrderState::Pending,
                    "system".into(),
                    "overnight_restore".into(),
                ));
            }
        }
        n
    }

    /// Restore one parked order with a NEW chained order id ( -
    /// "parked orders → pending at preopen, chain new order ids").
    /// The parked row stays (audit), the new pending order carries
    /// `replaces_order_id = parked_id`. Works for both the in-memory
    /// working set and DB-loaded snapshots.
    pub fn restore_one(&self, parked: Order) -> Order {
        let mut inner = self.inner.write();
        // If the parked order is still in the working map, terminalize it.
        if let Some(o) = inner.orders.get_mut(&parked.order_id) {
            if o.state == OrderState::OvernightParked {
                o.state = OrderState::Cancelled;
                o.terminal_at = Some(Utc::now());
            }
        }
        let restored = Order {
            order_id: Uuid::now_v7(),
            client_order_id: format!("{}-on", parked.client_order_id),
            chain_root_id: parked.chain_root_id,
            replaces_order_id: Some(parked.order_id),
            state: OrderState::Pending,
            terminal_at: None,
            ..parked
        };
        let id = restored.order_id;
        inner
            .by_client_id
            .insert(restored.client_order_id.clone(), id);
        inner.orders.insert(id, restored.clone());
        inner.events.push((
            id,
            OrderState::Pending,
            OrderState::Pending,
            "system".into(),
            "overnight_restore".into(),
        ));
        restored
    }

    /// Blotter read (self scope: own rows only - explicit WHERE parity in
    /// the DB pass; here the in-memory filter).
    pub fn blotter_for(&self, usr_id: &str, office_id: &str) -> Vec<OrderSummary> {
        self.inner
            .read()
            .orders
            .values()
            .filter(|o| o.usr_id == usr_id || o.office_id == office_id)
            .map(order_summary)
            .collect()
    }

    /// Event journal read (test + audit parity).
    pub fn events_snapshot(&self) -> Vec<(Uuid, OrderState, OrderState, String, String)> {
        self.inner.read().events.clone()
    }

    /// Order detail.
    pub fn order_by_id(&self, id: Uuid) -> Option<Order> {
        self.inner.read().orders.get(&id).cloned()
    }

    /// Working-order count for a trader (Risk max_open_orders input).
    pub fn open_order_count(&self, usr_id: &str) -> usize {
        self.inner
            .read()
            .orders
            .values()
            .filter(|o| {
                o.usr_id == usr_id
                    && matches!(
                        o.state,
                        OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
                    )
            })
            .count()
    }

    /// Working-order count across the whole book (admin overview card).
    pub fn working_count(&self) -> usize {
        self.inner
            .read()
            .orders
            .values()
            .filter(|o| {
                matches!(
                    o.state,
                    OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
                )
            })
            .count()
    }

    /// Desk-side position view for one (trader, symbol) - signed net of
    /// filled quantities (buys positive). The Pnl service owns the
    /// authoritative position ledger; this is the order-flow view Risk
    /// sees on the authorize path when no Pnl mirror has landed yet.
    pub fn position_of(&self, usr_id: &str, symbol_id: Uuid) -> i64 {
        self.inner
            .read()
            .orders
            .values()
            .filter(|o| o.usr_id == usr_id && o.symbol_id == symbol_id)
            .map(|o| match o.side {
                Side::Buy => o.filled_qty.raw as i64,
                Side::Sell => -(o.filled_qty.raw as i64),
                Side::Trade => 0,
            })
            .sum()
    }

    /// Session state (main.rs handler).
    pub fn session_state(&self) -> SessionState {
        self.session.state()
    }

    /// Session action (main.rs handler) - Cron parity actions.
    /// Returns the resulting state; unknown actions leave state unchanged.
    pub async fn session_action(&self, action: &str) -> SessionState {
        if let Some(next) = SessionState::parse(action) {
            // Only forward-transitions via the manual ops path; the
            // Cron proxy-back owns the choreography.
            match (self.session.state(), next) {
                (SessionState::Closed, SessionState::Preopen)
                | (SessionState::Preopen, SessionState::Open)
                | (SessionState::Open, SessionState::ClosePending)
                | (SessionState::ClosePending, SessionState::Closed) => {
                    if next == SessionState::Closed {
                        let (cancelled, parked) = self.close_session_sweep();
                        tracing::info!(cancelled, parked, "session closed");
                    }
                    if next == SessionState::Preopen {
                        let restored = self.restore_overnight();
                        tracing::info!(restored, "overnight orders restored");
                    }
                    self.session.set(next);
                }
                _ => {}
            }
        }
        self.session.state()
    }

    /// Blotter (main.rs handler - self scope).
    pub async fn blotter(&self, auth: &AuthContext) -> Vec<OrderSummary> {
        let office = self
            .catalog
            .office_for(&auth.claims.sub)
            .unwrap_or_else(|| "DEFAULT".to_string());
        self.blotter_for(&auth.claims.sub, &office)
    }

    /// Order detail (main.rs handler).
    pub async fn order_detail(
        &self,
        _auth: &AuthContext,
        id: Uuid,
    ) -> Result<serde_json::Value, Rejection> {
        match self.order_by_id(id) {
            Some(o) => Ok(serde_json::to_value(&o).unwrap_or_default()),
            None => Err(Rejection::new(RejectCode::OrderNotFound)),
        }
    }

    /// Symbols read (main.rs handler - catalog cache).
    pub fn symbols_json(&self) -> Vec<serde_json::Value> {
        self.catalog.symbols_json()
    }
}

fn order_summary(o: &Order) -> OrderSummary {
    OrderSummary {
        order_id: o.order_id,
        client_order_id: o.client_order_id.clone(),
        symbol_name: o.symbol_name.clone(),
        side: o.side,
        state: o.state,
        qty: o.qty,
        leaves_qty: o.leaves_qty,
        filled_qty: o.filled_qty,
        price: o.price,
        usr_id: o.usr_id.clone(),
        office_id: o.office_id.clone(),
        submitted_at: o.submitted_at,
    }
}

use sentin8::shell::AuthContext;

impl DeskEngine {
    pub async fn submit(
        &self,
        auth: &AuthContext,
        req: NewOrderRequest,
    ) -> Result<Order, Rejection> {
        let usr = auth.claims.sub.clone();
        // Office membership is env-profile data (`trader_profile`); the
        // engine takes the actor's default office from the catalog, falling
        // back to the default office when the profile has not been seeded.
        let office = self
            .catalog
            .office_for(&usr)
            .unwrap_or_else(|| "DEFAULT".to_string());
        self.submit_with_actor(&usr, &office, req)
    }

    pub async fn cancel(&self, auth: &AuthContext, id: Uuid) -> Result<Order, Rejection> {
        self.cancel_with_actor(&auth.claims.sub, id)
    }

    pub async fn replace(
        &self,
        auth: &AuthContext,
        id: Uuid,
        req: ReplaceOrderRequest,
    ) -> Result<Order, Rejection> {
        self.replace_with_actor(&auth.claims.sub, id, req)
    }

    /// Flatten: cancel all working orders for the target scope ( -
    /// the resubmit strategy loop is a Func module concern; the Desk
    /// performs the cancel side synchronously).
    pub async fn flatten(&self, auth: &AuthContext, req: FlattenRequest) -> usize {
        let target_usr = req
            .usr_id
            .clone()
            .unwrap_or_else(|| auth.claims.sub.clone());
        let target_office = req.office_id.clone();
        let inner = self.inner.read();
        let ids: Vec<Uuid> = inner
            .orders
            .iter()
            .filter(|(_, o)| {
                let usr_hit = o.usr_id == target_usr;
                let office_hit = target_office
                    .as_ref()
                    .map(|off| o.office_id == *off)
                    .unwrap_or(true);
                let sym_hit = req
                    .symbol_name
                    .as_ref()
                    .map(|s| o.symbol_name == *s)
                    .unwrap_or(true);
                usr_hit
                    && office_hit
                    && sym_hit
                    && matches!(
                        o.state,
                        OrderState::Accepted | OrderState::Routed | OrderState::PartiallyFilled
                    )
            })
            .map(|(k, _)| *k)
            .collect();
        drop(inner);
        let mut n = 0;
        for id in ids {
            if self.cancel_with_actor(auth.sub(), id).is_ok() {
                n += 1;
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{DestinationRow, SymbolRow, TickBand};
    use std::sync::OnceLock;
    use trade8::contracts::Side;

    /// Fixed order id for the transition-matrix seeds.
    const ORDER_ID: Uuid = Uuid::nil();

    fn engine_with_catalog() -> DeskEngine {
        let e = DeskEngine::new();
        e.catalog.load_symbols(vec![SymbolRow {
            symbol_id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            name: "AAPL".into(),
            price_scale: 4,
            tick_rule_id: rule_id(),
            feed_state: "act".into(),
            instrument_state: "active".into(),
            lot_size: 1,
        }]);
        e.catalog.load_destinations(vec![DestinationRow {
            destination_id: Uuid::new_v4(),
            name: "BRK1".into(),
            min_size: 1,
            enable: true,
        }]);
        e.catalog.load_tick_rules(
            rule_id(),
            vec![TickBand {
                price_lt: i128::MAX,
                tick_size: 100,
            }],
        );
        e
    }

    fn req(client_id: &str, qty: &str, price: Option<&str>) -> NewOrderRequest {
        NewOrderRequest {
            client_order_id: client_id.into(),
            symbol_name: "AAPL".into(),
            destination_id: e2e_dest(),
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

    fn e2e_dest() -> Uuid {
        // The engine validates destination against the catalog; tests seed one.
        use std::sync::OnceLock;
        static DEST: OnceLock<Uuid> = OnceLock::new();
        *DEST.get_or_init(Uuid::new_v4)
    }

    fn rule_id() -> Uuid {
        use std::sync::OnceLock;
        static RULE: OnceLock<Uuid> = OnceLock::new();
        *RULE.get_or_init(Uuid::new_v4)
    }

    // Tests share one catalog-loaded engine to reuse the seeded destination.
    fn shared_engine() -> &'static DeskEngine {
        static E: OnceLock<DeskEngine> = OnceLock::new();
        E.get_or_init(|| {
            let e = DeskEngine::new();
            e.catalog.load_symbols(vec![SymbolRow {
                symbol_id: Uuid::new_v4(),
                market_id: Uuid::new_v4(),
                name: "AAPL".into(),
                price_scale: 4,
                tick_rule_id: rule_id(),
                feed_state: "act".into(),
                instrument_state: "active".into(),
                lot_size: 1,
            }]);
            let dest = DestinationRow {
                destination_id: e2e_dest(),
                name: "BRK1".into(),
                min_size: 1,
                enable: true,
            };
            e.catalog.load_destinations(vec![dest]);
            e.catalog.load_tick_rules(
                rule_id(),
                vec![TickBand {
                    price_lt: i128::MAX,
                    tick_size: 100,
                }],
            );
            e.session.set(SessionState::Open);
            e
        })
    }

    #[test]
    fn submit_accept_and_duplicate() {
        let e = shared_engine();
        let o = e
            .submit_with_actor("u1", "HO", req("c-1", "100", Some("10.05")))
            .unwrap();
        assert_eq!(o.state, OrderState::Accepted);
        assert_eq!(o.qty.raw, 100);
        // duplicate client id → same order id (idempotent resubmit)
        let again = e
            .submit_with_actor("u1", "HO", req("c-1", "100", Some("10.05")))
            .unwrap();
        assert_eq!(again.order_id, o.order_id);
    }

    #[test]
    fn submit_rejections() {
        let e = shared_engine();
        // unknown symbol
        let mut r = req("c-sym", "100", Some("10.00"));
        r.symbol_name = "NOPE".into();
        let rej = e.submit_with_actor("u1", "HO", r).unwrap_err();
        assert_eq!(rej.code, RejectCode::SymbolUnknown);
        // off-tick price (tick 100 = 0.01; 10.055 → invalid)
        let rej = e
            .submit_with_actor("u1", "HO", req("c-tick", "100", Some("10.055")))
            .unwrap_err();
        assert_eq!(rej.code, RejectCode::PriceOffTick);
        // limit without price
        let mut r = req("c-np", "100", None);
        r.order_type = OrderType::Limit;
        let rej = e.submit_with_actor("u1", "HO", r).unwrap_err();
        assert_eq!(rej.code, RejectCode::TypeNeedsPrice);
        // zero qty
        let rej = e
            .submit_with_actor("u1", "HO", req("c-q0", "0", Some("10.00")))
            .unwrap_err();
        assert_eq!(rej.code, RejectCode::QtyInvalid);
    }

    /// A dedicated engine for tests that MUTATE exclusive state (session
    /// clock, order chains) - the shared engine's u1 order set and session
    /// clock are mutated by parallel tests (memory: no shared mutable test
    /// state). Same fixture shape as `shared_engine`.
    fn test_engine() -> DeskEngine {
        let e = DeskEngine::new();
        e.catalog.load_symbols(vec![SymbolRow {
            symbol_id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            name: "AAPL".into(),
            price_scale: 4,
            tick_rule_id: rule_id(),
            feed_state: "act".into(),
            instrument_state: "active".into(),
            lot_size: 1,
        }]);
        e.catalog.load_destinations(vec![DestinationRow {
            destination_id: e2e_dest(),
            name: "BRK1".into(),
            min_size: 1,
            enable: true,
        }]);
        e.catalog.load_tick_rules(
            rule_id(),
            vec![TickBand {
                price_lt: i128::MAX,
                tick_size: 100,
            }],
        );
        e.session.set(SessionState::Open);
        e
    }

    #[test]
    fn session_gate_blocks_submit() {
        // Dedicated engine - mutating the shared engine's session clock
        // races with parallel tests (memory: no shared mutable test state).
        let e = test_engine();
        e.session.set(SessionState::Closed);
        let rej = e
            .submit_with_actor("u1", "HO", req("c-closed", "100", Some("10.00")))
            .unwrap_err();
        assert_eq!(rej.code, RejectCode::SessionNotOpen);
        e.session.set(SessionState::Open);
        assert!(e
            .submit_with_actor("u1", "HO", req("c-open-ok", "100", Some("10.00")))
            .is_ok());
    }

    #[test]
    fn cancel_and_replace_chain() {
        // Dedicated engine - the shared engine's tests mutate u1's orders
        // concurrently (a parallel sweep cancelling this test's order is
        // the "not cancellable" race test_engine's doc explains).
        let e = test_engine();
        let o = e
            .submit_with_actor("u1", "HO", req("c-r1", "100", Some("10.00")))
            .unwrap();
        let rep = e
            .replace_with_actor(
                "u1",
                o.order_id,
                ReplaceOrderRequest {
                    order_id: o.order_id,
                    client_order_id: "c-r2".into(),
                    qty: "200".into(),
                    price: Some("10.10".into()),
                },
            )
            .unwrap();
        assert_eq!(rep.replaces_order_id, Some(o.order_id));
        assert_eq!(rep.chain_root_id, o.chain_root_id);
        let old = e.order_by_id(o.order_id).unwrap();
        assert_eq!(old.state, OrderState::Replaced);
        // old is no longer cancellable
        let rej = e.cancel_with_actor("u1", o.order_id).unwrap_err();
        assert_eq!(rej.code, RejectCode::OrderNotCancellable);
    }

    #[test]
    fn close_sweep_cancels_day_parks_overnight() {
        let e = shared_engine();
        e.submit_with_actor("u1", "HO", req("c-d1", "100", Some("10.00")))
            .unwrap();
        let mut on = req("c-o1", "100", Some("10.00"));
        on.tif = TimeInForce::Overnight;
        e.submit_with_actor("u1", "HO", on).unwrap();
        let (cancelled, parked) = e.close_session_sweep();
        assert!(cancelled >= 1);
        assert!(parked >= 1);
        let restored = e.restore_overnight();
        assert_eq!(restored, parked);
    }

    #[test]
    fn short_locate_reserved_and_released() {
        let e = shared_engine();
        e.borrow.set_pool(symbol_for(e), 500);
        let mut short = req("c-sh1", "300", Some("10.00"));
        short.side = Side::Sell;
        let o = e.submit_with_actor("u1", "HO", short).unwrap();
        assert_eq!(e.borrow.available(symbol_for(e)), 200);
        e.cancel_with_actor("u1", o.order_id).unwrap();
        assert_eq!(e.borrow.available(symbol_for(e)), 500); // released
                                                            // second short exceeding pool → locate unavailable
        let mut big = req("c-sh2", "600", Some("10.00"));
        big.side = Side::Sell;
        let rej = e.submit_with_actor("u1", "HO", big).unwrap_err();
        assert_eq!(rej.code, RejectCode::RiskLocateUnavailable);
    }

    fn symbol_for(e: &DeskEngine) -> Uuid {
        e.catalog.symbol("AAPL").unwrap().symbol_id
    }

    #[test]
    fn blotter_scope_filter() {
        let e = shared_engine();
        e.submit_with_actor("u2", "BR", req("c-b1", "100", Some("10.00")))
            .unwrap();
        let rows = e.blotter_for("u2", "BR");
        assert!(rows.iter().all(|r| r.usr_id == "u2"));
    }

    ///  - the exhaustive transition matrix property: for every
    /// (from, action) pair the engine either performs the transition or
    /// rejects with the mapped code - never a silent no-op and never an
    /// illegal state landing. Terminal states reject every mutation;
    /// working states accept cancel; replace chains only from working.
    #[test]
    fn transition_matrix_exhaustive() {
        let all_states = [
            OrderState::Pending,
            OrderState::Accepted,
            OrderState::Routed,
            OrderState::PartiallyFilled,
            OrderState::Filled,
            OrderState::Cancelled,
            OrderState::Replaced,
            OrderState::Rejected,
            OrderState::Expired,
            OrderState::OvernightParked,
        ];
        let working = [
            OrderState::Accepted,
            OrderState::Routed,
            OrderState::PartiallyFilled,
        ];
        let terminal = [
            OrderState::Filled,
            OrderState::Cancelled,
            OrderState::Replaced,
            OrderState::Rejected,
            OrderState::Expired,
        ];
        for from in all_states {
            // cancel: legal exactly from working states.
            let e = engine_with_state(from);
            let res = e.cancel_with_actor("t", ORDER_ID);
            if working.contains(&from) {
                assert!(
                    res.is_ok(),
                    "cancel from {from:?} must succeed (session open)"
                );
                assert_eq!(res.unwrap().state, OrderState::Cancelled);
            } else {
                assert!(
                    res.is_err(),
                    "cancel from {from:?} must reject (terminal/pending/parked)"
                );
            }
            // replace: legal exactly from working states; the chain
            // fields (replaces/chain_root) are set on success.
            let e = engine_with_state(from);
            let res = e.replace_with_actor(
                "t",
                ORDER_ID,
                ReplaceOrderRequest {
                    order_id: ORDER_ID,
                    client_order_id: format!("r-{from:?}"),
                    qty: "50".into(),
                    price: Some("10.50".into()),
                },
            );
            if working.contains(&from) {
                let o = res.unwrap();
                assert_eq!(o.replaces_order_id, Some(ORDER_ID));
                assert_eq!(o.state, OrderState::Accepted);
            } else {
                assert!(res.is_err(), "replace from {from:?} must reject");
            }
        }
        // Property: terminal states never resurrect - every action on a
        // terminal order leaves it terminal (state map unchanged).
        for t in terminal {
            let e = engine_with_state(t);
            let _ = e.cancel_with_actor("t", ORDER_ID);
            let _ = e.replace_with_actor(
                "t",
                ORDER_ID,
                ReplaceOrderRequest {
                    order_id: ORDER_ID,
                    client_order_id: "x".into(),
                    qty: "1".into(),
                    price: None,
                },
            );
            assert_eq!(
                e.order_by_id(ORDER_ID).unwrap().state,
                t,
                "terminal {t:?} resurrected"
            );
        }
    }

    /// Seed one engine whose working order is parked in the given state.
    /// Fresh engine per call (the transition matrix mutates it).
    fn engine_with_state(state: OrderState) -> DeskEngine {
        let e = DeskEngine::new();
        e.catalog.load_symbols(vec![SymbolRow {
            symbol_id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            name: "AAPL".into(),
            price_scale: 4,
            tick_rule_id: rule_id(),
            feed_state: "act".into(),
            instrument_state: "active".into(),
            lot_size: 1,
        }]);
        e.catalog.load_destinations(vec![DestinationRow {
            destination_id: e2e_dest(),
            name: "BRK1".into(),
            min_size: 1,
            enable: true,
        }]);
        e.catalog.load_tick_rules(
            rule_id(),
            vec![TickBand {
                price_lt: i128::MAX,
                tick_size: 100,
            }],
        );
        let o = Order {
            order_id: ORDER_ID,
            client_order_id: format!("c-{state:?}"),
            chain_root_id: ORDER_ID,
            replaces_order_id: None,
            usr_id: "t".into(),
            office_id: "HO".into(),
            symbol_id: e
                .catalog
                .symbol("AAPL")
                .map(|s| s.symbol_id)
                .unwrap_or_default(),
            symbol_name: "AAPL".into(),
            market_id: Uuid::new_v4(),
            destination_id: e2e_dest(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::Day,
            qty: Qty::shares(100),
            price: Some(Price::equity(100_000)),
            stop_price: None,
            state,
            leaves_qty: Qty::shares(100),
            filled_qty: Qty::shares(0),
            flags: serde_json::Value::Null,
            on_behalf_of: None,
            submitted_at: Utc::now(),
            terminal_at: None,
        };
        {
            let mut inner = e.inner.write();
            inner
                .by_client_id
                .insert(o.client_order_id.clone(), ORDER_ID);
            inner.orders.insert(ORDER_ID, o);
        }
        // Session must be Open for cancels/replaces to pass the gate.
        e.session.set(SessionState::Open);
        e
    }
}
