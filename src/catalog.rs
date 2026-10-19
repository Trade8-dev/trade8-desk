// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Catalog caches - arc-swap snapshots of the reference tables,
//! loaded at startup and invalidated by `catalog:updated` events.
//!
//! Design: `../desk/DESIGN.md` §4.3.
//! hot path never queries the DB; reads are wait-free snapshot reads.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use trade8::fixed::Price;
use uuid::Uuid;

/// A catalog snapshot - one immutable generation, swapped atomically.
#[derive(Debug, Default, Clone)]
pub struct CatalogSnapshot {
    pub symbols: HashMap<String, SymbolRow>,
    pub symbols_by_id: HashMap<Uuid, SymbolRow>,
    pub destinations: HashMap<Uuid, DestinationRow>,
    pub tick_rules: HashMap<Uuid, Arc<Vec<TickBand>>>,
    /// usr_id → office_id (trader_profile; office resolution on submit).
    pub trader_offices: HashMap<String, String>,
}

/// The `desk.symbol` row as decoded from SQL (`Postgres` integers lack
/// unsigned types; converted into [`SymbolRow`] via `From`).
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct SymbolRowRaw {
    pub symbol_id: Uuid,
    pub market_id: Uuid,
    pub name: String,
    /// Postgres has no u8 - the SQL casts to int; converted into
    /// [`SymbolRow::price_scale`] at bootstrap.
    pub price_scale: i32,
    pub tick_rule_id: Uuid,
    pub feed_state: String,
    pub instrument_state: String,
    pub lot_size: i64,
}

impl From<SymbolRowRaw> for SymbolRow {
    fn from(r: SymbolRowRaw) -> Self {
        SymbolRow {
            symbol_id: r.symbol_id,
            market_id: r.market_id,
            name: r.name,
            price_scale: r.price_scale.max(0) as u8,
            tick_rule_id: r.tick_rule_id,
            feed_state: r.feed_state,
            instrument_state: r.instrument_state,
            lot_size: r.lot_size.max(0) as u64,
        }
    }
}

/// Symbol catalog row (decoded from `desk.symbol` at bootstrap; the
/// `SymbolRowRaw` → `SymbolRow` conversion adapts Postgres integers to the
/// unsigned catalog types).
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolRow {
    pub symbol_id: Uuid,
    pub market_id: Uuid,
    pub name: String,
    pub price_scale: u8,
    pub tick_rule_id: Uuid,
    pub feed_state: String,
    pub instrument_state: String,
    pub lot_size: u64,
}

/// Destination catalog row.
#[derive(Debug, Clone, PartialEq)]
pub struct DestinationRow {
    pub destination_id: Uuid,
    pub name: String,
    pub min_size: i64,
    pub enable: bool,
}

/// One tick band: price below `price_lt` uses `tick_size` (scaled units).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickBand {
    pub price_lt: i128,
    pub tick_size: i128,
}

/// The wait-free catalog reader + writer.
#[derive(Default)]
pub struct Catalog {
    snapshot: ArcSwap<CatalogSnapshot>,
}

impl Catalog {
    /// Replace the whole symbol set (reload event `{kind:symbol}`).
    pub fn load_symbols(&self, rows: Vec<SymbolRow>) {
        let mut snap = (**self.snapshot.load()).clone();
        snap.symbols.clear();
        snap.symbols_by_id.clear();
        for r in rows {
            snap.symbols_by_id.insert(r.symbol_id, r.clone());
            snap.symbols.insert(r.name.clone(), r);
        }
        self.snapshot.store(Arc::new(snap));
    }

    /// Replace the destination set (`{kind:destination}`).
    pub fn load_destinations(&self, rows: Vec<DestinationRow>) {
        let mut snap = (**self.snapshot.load()).clone();
        snap.destinations.clear();
        for r in rows {
            snap.destinations.insert(r.destination_id, r);
        }
        self.snapshot.store(Arc::new(snap));
    }

    /// Replace a tick rule's bands (`{kind:tick_rule}`).
    pub fn load_tick_rules(&self, rule_id: Uuid, bands: Vec<TickBand>) {
        let mut snap = (**self.snapshot.load()).clone();
        snap.tick_rules.insert(rule_id, Arc::new(bands));
        self.snapshot.store(Arc::new(snap));
    }

    /// Set trader→office mappings (`{kind:office}` / trader_profile reload).
    pub fn load_trader_offices(&self, pairs: Vec<(String, String)>) {
        let mut snap = (**self.snapshot.load()).clone();
        snap.trader_offices.clear();
        for (usr, office) in pairs {
            snap.trader_offices.insert(usr, office);
        }
        self.snapshot.store(Arc::new(snap));
    }

    pub fn symbol(&self, name: &str) -> Option<SymbolRow> {
        self.snapshot.load().symbols.get(name).cloned()
    }

    pub fn symbol_by_id(&self, id: Uuid) -> Option<SymbolRow> {
        self.snapshot.load().symbols_by_id.get(&id).cloned()
    }

    pub fn destination(&self, id: Uuid) -> Option<DestinationRow> {
        self.snapshot.load().destinations.get(&id).cloned()
    }

    pub fn office_for(&self, usr: &str) -> Option<String> {
        self.snapshot.load().trader_offices.get(usr).cloned()
    }

    /// Effective tick size for a price (bands ascending; first bound ≥
    /// price wins; fallback 1 tick = the minimum).
    pub fn tick_for(&self, tick_rule_id: Uuid, price: Price) -> i128 {
        let snap = self.snapshot.load();
        if let Some(bands) = snap.tick_rules.get(&tick_rule_id) {
            for b in bands.iter() {
                if b.price_lt >= price.raw {
                    return b.tick_size;
                }
            }
        }
        1
    }

    /// Symbols as JSON rows (the `/v1/symbols` read shape).
    pub fn symbols_json(&self) -> Vec<serde_json::Value> {
        let snap = self.snapshot.load();
        let mut rows: Vec<_> = snap
            .symbols
            .values()
            .map(|s| {
                serde_json::json!({
                    "symbol_id": s.symbol_id,
                    "name": s.name,
                    "market_id": s.market_id,
                    "price_scale": s.price_scale,
                    "lot_size": s.lot_size,
                    "feed_state": s.feed_state,
                    "instrument_state": s.instrument_state,
                })
            })
            .collect();
        rows.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_read_write() {
        let c = Catalog::default();
        c.load_symbols(vec![SymbolRow {
            symbol_id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            name: "AAPL".into(),
            price_scale: 4,
            tick_rule_id: Uuid::new_v4(),
            feed_state: "act".into(),
            instrument_state: "active".into(),
            lot_size: 1,
        }]);
        assert!(c.symbol("AAPL").is_some());
        assert!(c.symbol("MSFT").is_none());
        // reload replaces (no duplicate growth)
        c.load_symbols(vec![]);
        assert!(c.symbol("AAPL").is_none());
    }

    #[test]
    fn tick_band_resolution() {
        let c = Catalog::default();
        let rule = Uuid::new_v4();
        c.load_tick_rules(
            rule,
            vec![
                TickBand {
                    price_lt: 50_000,
                    tick_size: 100,
                },
                TickBand {
                    price_lt: 1_000_000,
                    tick_size: 500,
                },
            ],
        );
        assert_eq!(c.tick_for(rule, Price::equity(49_000)), 100);
        assert_eq!(c.tick_for(rule, Price::equity(500_000)), 500);
        assert_eq!(c.tick_for(rule, Price::equity(9_999_999)), 1); // fallback
        assert_eq!(c.tick_for(Uuid::new_v4(), Price::equity(1)), 1); // unknown rule
    }

    #[test]
    fn office_resolution() {
        let c = Catalog::default();
        c.load_trader_offices(vec![("u1".into(), "HO".into())]);
        assert_eq!(c.office_for("u1").as_deref(), Some("HO"));
        assert!(c.office_for("u2").is_none());
    }
}
