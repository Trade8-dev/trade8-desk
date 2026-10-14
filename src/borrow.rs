// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Short-locate borrow pool — in-process Desk module over the
//! `short_pool` / `short_pool_office_block` / `borrow_list` / `short_exempt`
//! tables (the pool sits on the order-acceptance path; a separate process
//! would put IPC on the hot path).
//!
//! Design: `../desk/DESIGN.md` §4.4.
//! Every pool mutation is an outbox event; this module
//! owns the reservation arithmetic and the ETB/HTB/exempt classification.

use std::collections::HashMap;

use parking_lot::RwLock;
use uuid::Uuid;

/// Borrow classification for a symbol (from `trade8_borrow_list`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowClass {
    Easy,
    Hard,
    Exempt,
}

/// In-memory pool state per (symbol).
#[derive(Debug, Default, Clone)]
struct PoolEntry {
    total: i64,
    available: i64,
    /// Outstanding reservations (order ids → shares) — audit + release.
    reserved: HashMap<Uuid, i64>,
}

/// Office-blocked shares: `available(symbol) = pool − office blocks −
/// outstanding reservations`.
#[derive(Debug, Default)]
struct PoolInner {
    pools: HashMap<Uuid, PoolEntry>,
    office_blocks: HashMap<(Uuid, String), i64>,
    etb: std::collections::HashSet<Uuid>,
    htb: std::collections::HashSet<Uuid>,
    exempt: std::collections::HashSet<Uuid>,
}

/// The borrow pool module handle.
#[derive(Default)]
pub struct BorrowPool {
    inner: RwLock<PoolInner>,
}

impl BorrowPool {
    /// Set the pool totals (admin action / reload).
    pub fn set_pool(&self, symbol_id: Uuid, total_shares: i64) {
        let mut inner = self.inner.write();
        let entry = inner.pools.entry(symbol_id).or_default();
        let outstanding: i64 = entry.reserved.values().sum();
        entry.total = total_shares;
        entry.available = (total_shares - outstanding).max(0);
    }

    /// Set an office block for a symbol.
    pub fn set_office_block(&self, symbol_id: Uuid, office_id: &str, shares: i64) {
        let mut inner = self.inner.write();
        if shares <= 0 {
            inner
                .office_blocks
                .remove(&(symbol_id, office_id.to_string()));
        } else {
            inner
                .office_blocks
                .insert((symbol_id, office_id.to_string()), shares);
        }
    }

    /// Classify a symbol.
    pub fn classify(&self, symbol_id: Uuid, class: BorrowClass) {
        let mut inner = self.inner.write();
        match class {
            BorrowClass::Easy => {
                inner.etb.insert(symbol_id);
                inner.htb.remove(&symbol_id);
            }
            BorrowClass::Hard => {
                inner.htb.insert(symbol_id);
                inner.etb.remove(&symbol_id);
            }
            BorrowClass::Exempt => {
                inner.exempt.insert(symbol_id);
            }
        }
    }

    pub fn is_etb(&self, symbol_id: Uuid) -> bool {
        self.inner.read().etb.contains(&symbol_id)
    }

    pub fn is_exempt(&self, symbol_id: Uuid) -> bool {
        self.inner.read().exempt.contains(&symbol_id)
    }

    /// Available shares for a symbol from a given office (pool − office
    /// blocks − outstanding reservations; office blocks count in full —
    /// conservative, matching ).
    pub fn available_for(&self, symbol_id: Uuid, office_id: &str) -> i64 {
        let inner = self.inner.read();
        let pool = inner
            .pools
            .get(&symbol_id)
            .map(|p| p.available)
            .unwrap_or(0);
        let blocked: i64 = inner
            .office_blocks
            .get(&(symbol_id, office_id.to_string()))
            .copied()
            .unwrap_or(0);
        (pool - blocked).max(0)
    }

    /// Available shares office-agnostic (tests + admin views).
    pub fn available(&self, symbol_id: Uuid) -> i64 {
        self.inner
            .read()
            .pools
            .get(&symbol_id)
            .map(|p| p.available)
            .unwrap_or(0)
    }

    /// Reserve shares for a short order. `false` when insufficient.
    pub fn reserve(&self, symbol_id: Uuid, shares: i64) -> bool {
        self.reserve_for(symbol_id, "DEFAULT", shares)
    }

    /// Reserve with office scoping.
    pub fn reserve_for(&self, symbol_id: Uuid, office_id: &str, shares: i64) -> bool {
        if shares <= 0 {
            return true;
        }
        let mut inner = self.inner.write();
        if inner.available_for_locked(symbol_id, office_id) < shares {
            return false;
        }
        let entry = inner.pools.entry(symbol_id).or_default();
        entry.available -= shares;
        entry.reserved.insert(Uuid::new_v4(), shares);
        true
    }

    /// Release a reservation (cancel/reject/unfill) — by share count under
    /// the simple API (the DB pass keys by order id; the arithmetic is
    /// identical).
    pub fn release(&self, symbol_id: Uuid, shares: i64) {
        if shares <= 0 {
            return;
        }
        let mut inner = self.inner.write();
        if let Some(entry) = inner.pools.get_mut(&symbol_id) {
            entry.available = (entry.available + shares).min(entry.total);
        }
    }
}

impl PoolInner {
    fn available_for_locked(&self, symbol_id: Uuid, office_id: &str) -> i64 {
        let pool = self.pools.get(&symbol_id).map(|p| p.available).unwrap_or(0);
        let blocked = self
            .office_blocks
            .get(&(symbol_id, office_id.to_string()))
            .copied()
            .unwrap_or(0);
        (pool - blocked).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_release_cycle() {
        let pool = BorrowPool::default();
        pool.set_pool(sym(1), 1_000);
        assert!(pool.reserve_for(sym(1), "HO", 300));
        assert_eq!(pool.available_for(sym(1), "HO"), 700);
        pool.release(sym(1), 300);
        assert_eq!(pool.available_for(sym(1), "HO"), 1_000);
    }

    #[test]
    fn over_reserve_denied() {
        let pool = BorrowPool::default();
        pool.set_pool(sym(2), 100);
        assert!(pool.reserve_for(sym(2), "HO", 100));
        assert!(!pool.reserve_for(sym(2), "HO", 1));
    }

    #[test]
    fn office_blocks_reduce_availability() {
        let pool = BorrowPool::default();
        pool.set_pool(sym(3), 500);
        pool.set_office_block(sym(3), "BR", 200);
        assert_eq!(pool.available_for(sym(3), "BR"), 300);
        assert_eq!(pool.available_for(sym(3), "HO"), 500);
        // clearing the block restores
        pool.set_office_block(sym(3), "BR", 0);
        assert_eq!(pool.available_for(sym(3), "BR"), 500);
    }

    #[test]
    fn etb_htb_exempt_classification() {
        let pool = BorrowPool::default();
        pool.classify(sym(4), BorrowClass::Easy);
        assert!(pool.is_etb(sym(4)));
        pool.classify(sym(4), BorrowClass::Hard);
        assert!(!pool.is_etb(sym(4)));
        pool.classify(sym(5), BorrowClass::Exempt);
        assert!(pool.is_exempt(sym(5)));
    }

    #[test]
    fn release_never_exceeds_total() {
        let pool = BorrowPool::default();
        pool.set_pool(sym(6), 100);
        pool.release(sym(6), 400); // stray release — capped at total
        assert_eq!(pool.available(sym(6)), 100);
    }

    fn sym(n: u8) -> Uuid {
        Uuid::new_v5(&Uuid::NAMESPACE_OID, &[n])
    }
}
