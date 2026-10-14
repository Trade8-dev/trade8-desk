-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
-- SPDX-License-Identifier: Apache-2.0

-- Trade8:Desk — order lifecycle, catalogs, borrow pool, offices, keystroke log.
--
-- Design: ../desk/DESIGN.md §6,
-- ../../DESIGN.md §3 (data model) §2.1–2.3, §2.5.
-- No migration code (new platform): `sentin8-cli vertical init` applies this.

CREATE SCHEMA IF NOT EXISTS desk;

-- ---------------------------------------------------------------------------
-- Reference catalogs (§2.1)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS desk.currency (
 currency_code TEXT PRIMARY KEY,
 env_id UUID NOT NULL,
 name TEXT NOT NULL,
 rate_to_usd BIGINT NOT NULL DEFAULT 10000, -- ×10⁴ (1.0)
 modified TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS desk.asset_class (
 asset_class TEXT PRIMARY KEY,
 env_id UUID NOT NULL,
 description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS desk.market (
 market_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 code TEXT NOT NULL,
 description TEXT NOT NULL DEFAULT '',
 asset_class TEXT NOT NULL,
 country TEXT NOT NULL DEFAULT '',
 timezone TEXT NOT NULL,
 uptick_rule TEXT NOT NULL DEFAULT 'updown'
 CHECK (uptick_rule IN ('down','updown','up')),
 allow_convert_short BOOLEAN NOT NULL DEFAULT false,
 price_type TEXT NOT NULL DEFAULT 'float',
 enable_imbalance BOOLEAN NOT NULL DEFAULT true,
 UNIQUE (env_id, code)
);

CREATE TABLE IF NOT EXISTS desk.symbol_group (
 symbol_group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS desk.tick_rule (
 tick_rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 description TEXT,
 bands JSONB NOT NULL DEFAULT '[]', -- [{price_lt, tick_size}] ascending
 UNIQUE (env_id, description)
);

CREATE TABLE IF NOT EXISTS desk.symbol (
 symbol_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 market_id UUID NOT NULL REFERENCES desk.market,
 name TEXT NOT NULL,
 currency_code TEXT NOT NULL,
 asset_class TEXT NOT NULL,
 isin TEXT,
 cusip TEXT,
 description TEXT,
 lot_size BIGINT NOT NULL DEFAULT 100,
 tick_rule_id UUID NOT NULL,
 symbol_group_id UUID,
 instrument_state TEXT NOT NULL DEFAULT 'active',
 feed_state TEXT NOT NULL DEFAULT 'act' CHECK (feed_state IN ('act','ina')),
 fix_tags JSONB NOT NULL DEFAULT '{}',
 low_limit_px BIGINT,
 high_limit_px BIGINT,
 avg_30d_volume BIGINT NOT NULL DEFAULT 0,
 UNIQUE (env_id, market_id, name)
);

CREATE TABLE IF NOT EXISTS desk.future (
 symbol_id UUID PRIMARY KEY REFERENCES desk.symbol,
 env_id UUID NOT NULL,
 expiry TEXT NOT NULL, -- 'YYYY-MM'
 price_scale INT NOT NULL DEFAULT 4, -- per-instrument scale
 price_format TEXT NOT NULL DEFAULT 'decimal',
 settle TEXT NOT NULL CHECK (settle IN ('cash','deliver')),
 max_contract_size BIGINT,
 tick_value BIGINT NOT NULL DEFAULT 0,
 display_factor BIGINT NOT NULL DEFAULT 10000,
 instrument_group TEXT
);

CREATE TABLE IF NOT EXISTS desk.market_period (
 period_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 market_id UUID NOT NULL REFERENCES desk.market,
 period_type TEXT NOT NULL, -- preopen|open|close_pending|closed|auction
 auction_type TEXT,
 start_time TIME NOT NULL,
 end_time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS desk.trading_calendar (
 env_id UUID NOT NULL,
 trading_date DATE NOT NULL,
 state TEXT NOT NULL DEFAULT 'trading', -- trading|holiday|half_day
 PRIMARY KEY (env_id, trading_date)
);

CREATE TABLE IF NOT EXISTS desk.broker (
 broker_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 name TEXT NOT NULL,
 description TEXT,
 UNIQUE (env_id, name)
);

CREATE TABLE IF NOT EXISTS desk.gateway (
 gateway_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 name TEXT NOT NULL,
 description TEXT NOT NULL DEFAULT '',
 env_mode TEXT NOT NULL DEFAULT 'live' CHECK (env_mode IN ('live','shadow')),
 UNIQUE (env_id, name)
);

CREATE TABLE IF NOT EXISTS desk.destination (
 destination_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 gateway_id UUID NOT NULL REFERENCES desk.gateway,
 broker_id UUID NOT NULL REFERENCES desk.broker,
 name TEXT NOT NULL,
 protocol TEXT NOT NULL CHECK (protocol IN ('fix42','fix44','ouch31')),
 endpoint_url TEXT NOT NULL,
 credentials_ref TEXT NOT NULL, -- secret reference — never a value
 currency_code TEXT NOT NULL,
 min_size BIGINT NOT NULL DEFAULT 1,
 support_mix_lot BOOLEAN NOT NULL DEFAULT true,
 visibility TEXT CHECK (visibility IN ('b','d','h')),
 fix_fields JSONB,
 heartbeat_s INT NOT NULL DEFAULT 30,
 seq_reset_policy TEXT NOT NULL DEFAULT 'daily'
 CHECK (seq_reset_policy IN ('daily','weekly')),
 is_uat BOOLEAN NOT NULL DEFAULT false,
 enable BOOLEAN NOT NULL DEFAULT true,
 UNIQUE (env_id, name)
);

CREATE TABLE IF NOT EXISTS desk.market_gateway_destination (
 env_id UUID NOT NULL,
 market_id UUID NOT NULL REFERENCES desk.market,
 gateway_id UUID NOT NULL REFERENCES desk.gateway,
 destination_id UUID NOT NULL REFERENCES desk.destination,
 PRIMARY KEY (env_id, market_id, gateway_id, destination_id)
);

-- ---------------------------------------------------------------------------
-- Identity-adjacent (§2.2) — Auth owns users; these are env profiles.
-- Defined before their dependents (office_broker, trader_profile).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS desk.office (
 office_id TEXT PRIMARY KEY, -- branch-code slug
 env_id UUID NOT NULL,
 prefix TEXT,
 name TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'enabled'
 CHECK (status IN ('enabled','closed','disabled')),
 country_code TEXT NOT NULL DEFAULT '',
 state_code TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS desk.office_broker (
 env_id UUID NOT NULL,
 gateway_id UUID NOT NULL REFERENCES desk.gateway,
 broker_id UUID NOT NULL REFERENCES desk.broker,
 office_id TEXT NOT NULL REFERENCES desk.office,
 PRIMARY KEY (env_id, gateway_id, broker_id, office_id)
);

CREATE TABLE IF NOT EXISTS desk.execution_rule (
 rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 description TEXT,
 scope JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS desk.order_class (
 order_class_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 name TEXT NOT NULL,
 properties JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS desk.trader_profile (
 usr_id TEXT PRIMARY KEY, -- Sentin8:Auth sub — no local password
 env_id UUID NOT NULL,
 office_id TEXT NOT NULL REFERENCES desk.office,
 display_name TEXT NOT NULL,
 email TEXT NOT NULL DEFAULT '',
 account_prefix TEXT,
 allow_to_trade BOOLEAN NOT NULL DEFAULT true,
 tool_preferences JSONB NOT NULL DEFAULT '{}',
 default_destination_id UUID REFERENCES desk.destination,
 default_sizes JSONB NOT NULL DEFAULT '[]'
);

-- ---------------------------------------------------------------------------
-- Trading (§2.3)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS desk."order" (
 order_id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 client_order_id TEXT NOT NULL,
 chain_root_id UUID NOT NULL,
 replaces_order_id UUID,
 usr_id TEXT NOT NULL REFERENCES desk.trader_profile,
 office_id TEXT NOT NULL REFERENCES desk.office,
 symbol_id UUID NOT NULL REFERENCES desk.symbol,
 destination_id UUID NOT NULL REFERENCES desk.destination,
 side TEXT NOT NULL CHECK (side IN ('b','s','t')),
 order_type TEXT NOT NULL,
 tif TEXT NOT NULL,
 qty BIGINT NOT NULL,
 price_px BIGINT,
 stop_px BIGINT,
 price_scale SMALLINT NOT NULL DEFAULT 4,
 qty_scale SMALLINT NOT NULL DEFAULT 0,
 leaves_qty BIGINT NOT NULL,
 filled_qty BIGINT NOT NULL DEFAULT 0,
 flags JSONB NOT NULL DEFAULT '{}',
 order_class_id UUID REFERENCES desk.order_class,
 on_behalf_of TEXT,
 state TEXT NOT NULL,
 submitted_at TIMESTAMPTZ NOT NULL,
 terminal_at TIMESTAMPTZ,
 UNIQUE (env_id, client_order_id)
);
CREATE INDEX IF NOT EXISTS idx_t8_order_usr ON desk."order" (env_id, usr_id, state);
CREATE INDEX IF NOT EXISTS idx_t8_order_symbol ON desk."order" (env_id, symbol_id, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_t8_chain ON desk."order" (env_id, chain_root_id);

CREATE TABLE IF NOT EXISTS desk.order_event (
 env_id UUID NOT NULL,
 seq BIGSERIAL PRIMARY KEY,
 order_id UUID NOT NULL,
 prev_state TEXT NOT NULL,
 new_state TEXT NOT NULL,
 actor TEXT NOT NULL,
 reason TEXT,
 payload JSONB NOT NULL DEFAULT '{}',
 at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_t8_orderevent_order ON desk.order_event (env_id, order_id, seq);

CREATE TABLE IF NOT EXISTS desk.overnight_order (
 order_id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 source_order_id UUID NOT NULL,
 parked_date DATE NOT NULL,
 snapshot JSONB NOT NULL,
 restored_order_id UUID,
 pnl_carry BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS desk.keystroke_log (
 env_id UUID NOT NULL,
 log_id BIGSERIAL PRIMARY KEY,
 usr_id TEXT NOT NULL,
 action TEXT NOT NULL,
 order_id UUID,
 requested_by TEXT,
 at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
--
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS desk.short_pool (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL REFERENCES desk.symbol,
 total_shares BIGINT NOT NULL,
 available_shares BIGINT NOT NULL,
 PRIMARY KEY (env_id, symbol_id)
);

CREATE TABLE IF NOT EXISTS desk.short_pool_office_block (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL REFERENCES desk.symbol,
 office_id TEXT NOT NULL REFERENCES desk.office,
 total_shares BIGINT,
 available_shares BIGINT,
 PRIMARY KEY (env_id, symbol_id, office_id)
);

CREATE TABLE IF NOT EXISTS desk.borrow_list (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL REFERENCES desk.symbol,
 category TEXT NOT NULL CHECK (category IN ('etb','htb')),
 PRIMARY KEY (env_id, symbol_id, category)
);

CREATE TABLE IF NOT EXISTS desk.short_exempt (
 env_id UUID NOT NULL,
 symbol_id UUID NOT NULL REFERENCES desk.symbol,
 PRIMARY KEY (env_id, symbol_id)
);

-- ---------------------------------------------------------------------------
-- Sequence allocation (platform D5 convention)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS desk.sequence (
 env_id UUID NOT NULL,
 service TEXT NOT NULL,
 seq_type TEXT NOT NULL,
 value BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY (env_id, service, seq_type)
);

-- ---------------------------------------------------------------------------
-- Router shared catalogs (data-model §2.1 — single source of truth; Router
-- reads these, owns only session runtime state + the journal in its schema)
-- ---------------------------------------------------------------------------

-- Per-destination outbound message templates (EXECUTION_BROKER_MSG_TEMPLATE
-- typed; router-technical-design §4.3). tag→value-source map, versioned.
CREATE TABLE IF NOT EXISTS desk.msg_template (
 env_id UUID NOT NULL,
 destination_id UUID NOT NULL REFERENCES desk.destination,
 msg_id TEXT NOT NULL, -- new_order|cancel|replace|…
 version INT NOT NULL DEFAULT 1,
 protocol TEXT NOT NULL DEFAULT 'fix44',
 fields JSONB NOT NULL, -- typed field-source map
 PRIMARY KEY (env_id, destination_id, msg_id, version)
);

-- Fee catalog (GATEWAY_FEE_TYPE/_FORMULA/_CONDITION/_FEE +
-- DESTINATION_DEFAULT_FEE → charger DSL, router-technical-design §4.4).
CREATE TABLE IF NOT EXISTS desk.fee_catalog (
 env_id UUID NOT NULL,
 fee_id UUID PRIMARY KEY,
 destination_id UUID REFERENCES desk.destination,
 fee_type TEXT NOT NULL, -- sec|activity|gateway|execution|clearing
 formula TEXT NOT NULL, -- per_share|per_order|percent_notional|banded
 conditions JSONB NOT NULL DEFAULT '[]',
 params JSONB NOT NULL, -- FEE_VALUE_1..3 typed
 currency_code TEXT,
 is_default BOOLEAN NOT NULL DEFAULT false,
 description TEXT
);
CREATE INDEX IF NOT EXISTS idx_t8_feecat_dest ON desk.fee_catalog (env_id, destination_id);

-- Reject code catalog (ORDER_REJECT_MESSAGE) — Risk denials map to codes.
CREATE TABLE IF NOT EXISTS desk.reject_code (
 env_id UUID NOT NULL,
 code TEXT NOT NULL,
 message TEXT NOT NULL,
 params JSONB NOT NULL DEFAULT '[]',
 PRIMARY KEY (env_id, code)
);

-- ---------------------------------------------------------------------------
-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant
-- tables. Per-table exception handling — one bad table must not block
-- the rest (platform SQL DO-block convention).
-- The policy-name fragment must be the *unquoted* table name (quotes are
-- re-added by %I) — embedded quotes from names like desk."order" break
-- the format string (memory: reserved-word table names are the classic
-- DO-block failure class).
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
DECLARE pol TEXT;
BEGIN
 FOREACH tbl IN ARRAY ARRAY[
 'desk.currency', 'desk.asset_class', 'desk.market',
 'desk.symbol_group', 'desk.tick_rule', 'desk.symbol',
 'desk.future', 'desk.market_period', 'desk.trading_calendar',
 'desk.broker', 'desk.gateway', 'desk.destination',
 'desk.market_gateway_destination', 'desk.office_broker',
 'desk.execution_rule', 'desk.order_class', 'desk.office',
 'desk.trader_profile', 'desk."order"', 'desk.order_event',
 'desk.overnight_order', 'desk.keystroke_log',
 'desk.short_pool', 'desk.short_pool_office_block',
 'desk.borrow_list', 'desk.short_exempt', 'desk.sequence',
 'desk.msg_template', 'desk.fee_catalog', 'desk.reject_code'
 ] LOOP
 BEGIN
 pol := replace(split_part(tbl, '.', 2), '"', '');
 EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
 EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
 -- Suffix BEFORE %I quoting: `order` alone is reserved, but
 -- `order_select` is a plain unquoted identifier.
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_select', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_insert', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_update', tbl);
 EXECUTE format('DROP POLICY IF EXISTS %1$I ON %2$s;
CREATE POLICY %1$I ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', pol || '_delete', tbl);
 EXCEPTION WHEN OTHERS THEN
 RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
 END;
 END LOOP;
END $$;
-- ---------------------------------------------------------------------------
-- Per-service role grants + search_path (bank8-ledger pattern; the CLI
-- creates the role BEFORE this SQL — grant-before-role incident rule).
-- Grants live in db trade8 (this database) only; the scope→role map row
-- is upserted by `vertical enroll` from --section=pg_roles.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trade8_desk_service') THEN
  ALTER ROLE trade8_desk_service SET search_path = desk, public;
  GRANT USAGE ON SCHEMA desk TO trade8_desk_service;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA desk TO trade8_desk_service;
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA desk TO trade8_desk_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA desk
   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trade8_desk_service;
  ALTER DEFAULT PRIVILEGES FOR ROLE sentin8 IN SCHEMA desk
   GRANT USAGE, SELECT ON SEQUENCES TO trade8_desk_service;
 END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox + audit journal (DBX §6.7 / §6.8 — shell-generated DDL).
-- Generated from sentin8-outbox::outbox_table_sql("desk") and
-- sentin8-audit::audit_journal_sql("desk").
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS desk.outbox (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 env_id UUID NOT NULL,
 event_type TEXT NOT NULL,
 stream_family TEXT NOT NULL,
 payload JSONB NOT NULL,
 publisher_id_token TEXT,
 publisher_subject TEXT,
 relayed BOOLEAN NOT NULL DEFAULT false,
 relayed_at TIMESTAMPTZ,
 relay_attempts INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_unrelayed
 ON desk.outbox (created_at) WHERE relayed = false;
ALTER TABLE desk.outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE desk.outbox FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_select ON desk.outbox;
CREATE POLICY outbox_select ON desk.outbox FOR SELECT
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_insert ON desk.outbox;
CREATE POLICY outbox_insert ON desk.outbox FOR INSERT
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
DROP POLICY IF EXISTS outbox_update ON desk.outbox;
CREATE POLICY outbox_update ON desk.outbox FOR UPDATE
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
CREATE OR REPLACE FUNCTION desk.desk_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
 PERFORM pg_notify('desk_outbox_changed', '');
 RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON desk.outbox;
CREATE TRIGGER outbox_notify_trigger
 AFTER INSERT ON desk.outbox
 FOR EACH ROW
 EXECUTE FUNCTION desk.desk_notify_outbox();

CREATE TABLE IF NOT EXISTS desk.audit_journal (
 id UUID PRIMARY KEY,
 env_id UUID NOT NULL,
 action TEXT NOT NULL,
 target_resource_type TEXT,
 target_resource_id TEXT,
 actor_user_id TEXT,
 actor_subject_type TEXT,
 severity TEXT NOT NULL DEFAULT 'info',
 correlation_id TEXT,
 before_state JSONB,
 after_state JSONB,
 details JSONB,
 pushed BOOLEAN NOT NULL DEFAULT false,
 pushed_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_desk_audit_journal_unpushed
 ON desk.audit_journal (created_at) WHERE pushed = false;
ALTER TABLE desk.audit_journal ENABLE ROW LEVEL SECURITY;
ALTER TABLE desk.audit_journal FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_journal_dml ON desk.audit_journal;
CREATE POLICY audit_journal_dml ON desk.audit_journal
 USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())
 WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id());
