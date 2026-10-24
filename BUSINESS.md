# Trade8:Desk - Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Trade8:Desk **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Give professional traders one place to enter, manage, cancel, replace, and
  flatten orders across all supported venues and brokers, with **sub-second
  acceptance** and a complete, regulator-audit trail.
- Provide the trading desk's **operational backbone**: instrument catalogs,
  broker/destination routing catalogs, trading calendars, market session
  control, and the short-sale locate pool.
- Guarantee that **no order is accepted without a pre-trade authorization**
  (desk-owned risk is delegated to Trade8:Risk, called synchronously).
- Give compliance an immutable record of *every* order action, including who
  acted, when, and on whose behalf (keystroke + audit journaling).

## 2. Business Scope (What the Service Provides)

| Capability | Description |
|---|---|
| Order intake | New orders (limit/market/stop classes) with idempotent client order ids; per-item basket submission |
| Order management | Cancel, cancel-replace (chained order ids), one-click flatten with typed strategy parameters |
| Working-order state | Live order/state tracking per session with an append-only transition ledger |
| Pre-trade gate integration | Synchronous authorization against Trade8:Risk; fail-closed when the gate is unavailable |
| Session control | Market session lifecycle (preopen → open → close-pending → closed) driven by scheduler callbacks or admin action |
| Overnight processing | Park overnight-GTC orders at close; restore them at preopen |
| Instrument catalogs | Markets, symbols (incl. futures), tick rules, trading calendars, market periods, fees, reject codes |
| Routing catalogs | Destinations, gateways, broker/office routing triples, outbound message templates |
| Short-sale locate pool | Per-symbol availability, office-level borrow caps, ETB/HTB classification, short-exempt management |
| Audit & keystroke trail | Who acted on which order, when, from which surface, including strategy-proxy actions |
| Admin workspace | RFW admin pages: overview, orders, symbols, markets, destinations, offices, borrow, session |
| Event publication | Outbox-first events for other desk systems (Router routes on acceptance; PnL/Risk track state) |
| Two-way trader channel | WS order entry with the same authorization and audit trail as REST |
| Strategy host surface | Proxy-back API for Sentin8:Func strategy modules acting on behalf of a trader |

### What the service does NOT own (business boundary)

| Concern | Owned by |
|---|---|
| Pre-trade risk limits & authorization math | Trade8:Risk |
| Broker connectivity, FIX/OUCH sessions, execution reports, fee application | Trade8:Router |
| Market data distribution and alerts | Trade8:Feed |
| Historical bars and EOD reports | Trade8:Bars |
| Positions, day P&L, office roll-ups | Trade8:Pnl |
| Scheduling (session times, journal purge, EOD) | Sentin8:Cron |
| Strategy module execution | Sentin8:Func |
| Identity, sign-in, role/permission administration | Sentin8:Auth + Shield |

## 3. Business Capabilities

| Area | Capability | Business rule |
|---|---|---|
| Intake | Structural validation | Symbol must exist and be feed-active; price/qty must respect tick bands and instrument scales; destination must be valid for the symbol's market |
| Intake | Idempotent submission | Resubmitting the same client order id returns the existing order and never duplicates it |
| Intake | Basket submission | Each basket item is risk-checked and persisted independently (per-item all-or-none, not across items) |
| Authorization | Risk-gated acceptance | An order only leaves Pending with an *allow* verdict from Trade8:Risk; a deny is journaled with the full trace |
| Authorization | Fail-closed | If Risk is unreachable, the order is rejected, never accepted without authorization |
| Lifecycle | Cancel | Any non-terminal order can be cancelled by its owner or by a manager with cancel permission; the reason is journaled |
| Lifecycle | Cancel-replace | Replaces create a new order id linked to the chain root; the replaced order becomes terminal |
| Lifecycle | Locate management | Short-selling needs pool availability (respecting office blocks); accepted shorts decrement the pool; cancels/rejects restore it |
| Lifecycle | Short-exempt | Symbols on the exempt list bypass locate for qualifying orders (Risk enforces) |
| Session | Session gating | Orders are accepted only when the session state allows; opening/closing are controlled actions, fully journaled |
| Session | Close sweep | At close, day-remaining orders are cancelled and overnight orders parked with their carried details |
| Session | Preopen restore | Parked overnight orders return to Pending state for the trader to action |
| Flatten | Strategy-driven exit | Flatten takes typed strategy parameters (attempt count, hold time, price mode, visibility) and delegates the attempt ladder to the configured strategy module |
| Catalogs | Reference data | Traders/destinations/symbols come from admin-managed catalogs, not from order entry |
| Audit | Keystroke journal | Every order-affecting action from any surface lands in the audit trail with actor and on-behalf-of attribution |

## 4. Business Events (Outbound)

| Event | Fires when |
|---|---|
| `trade8:desk:order:submitted` | A structurally valid order is parked for authorization |
| `trade8:desk:order:accepted` | Risk allows and the order is persisted |
| `trade8:desk:order:rejected` | Deny (risk or structural) - includes the reject code and trace |
| `trade8:desk:order:cancelled` | User/manager/session/close/risk cancellation |
| `trade8:desk:order:replaced` | A cancel-replace chains a new order |
| `trade8:desk:order:flatten` | A flatten instruction is requested (strategy trigger) |
| `trade8:desk:order:overnight` · `:restored` | Overnight parking / preopen restore |
| `trade8:desk:session:state` | Session transitions (preopen/open/close-pending/closed) |
| `trade8:desk:borrow:updated` | Locate pool mutation (reserve/release/admin adjustment) |
| `trade8:desk:keystroke:logged` | A keystroke-audit action is recorded |

**Consumed (business view):** fills and session status come back through
Trade8:Router's events; quote snapshots for flatten pricing come from
Trade8:Feed. Inbound events (`trade8:router:fill:executed`,
`trade8:feed:trade:printed`, …) update working state, but the *order-of-record*
state stays in Desk.

## 5. Business Rules & Invariants

1. No order is ever accepted without a synchronous allow verdict from Trade8:Risk.
2. Every order-state change is journaled with actor and reason; the ledger is
   immutable and complete for the trading day.
3. Resubmitted client order ids are idempotent - one order per client order id.
4. Short sells reserve locate availability before acceptance and release it on
   terminal states - availability never goes negative.
5. Close parks overnight orders and cancels day remnants; preopen restores
   parked orders exactly as parked.
6. The blotter shows only orders the trader is entitled to see (own / office /
   environment by role).
7. All catalog changes, session actions, and borrow adjustments are admin
   permission-gated and audited.
8. Strategy modules act only through the proxy-back interface and are always
   attributed (never appear as the trader).

## 6. Target-User Roles & Use Cases

| Persona | Use cases |
|---|---|
| Trader | Enter/cancel/replace orders, watch the working blotter, flatten positions, park/restore overnight orders, manage personal shortcuts |
| Office/branch manager | Monitor the office blotter, flatten an office's positions, control borrowing blocks for the office |
| Trading desk supervisor | Open/close the session, manage the locate pool (ETB/HTB, exempt list), handle rejects |
| Back office / catalog admin | Maintain symbols, markets, destinations, fees, reject codes |
| Compliance | Review the keystroke/audit trail for any order action |
| Automations (strategies) | Flattening and basket modules submitting/cancelling on a trader's behalf through the host interface |

## 7. KPIs / Success Criteria

- Order acceptance path completes (validate + authorize + persist + reply)
  within the session's latency budget; submit/cancel P50 tracked by the latency
  harness and must remain well below the trader-perceived threshold.
- Zero un-authorized accepts (fail-closed proof in tests).
- 100% of transitions journaled; audit journal delivery to the platform is
  retried until pushed (no lost audit).
- Locate availability never negative; cancels always restore reserved shares.
- Overnight park/restore round-trips exactly (no lost or duplicated orders).
- Blotter queries remain scope-correct under role changes (RLS-verified).

## 8. Dependencies & Reliance on Other Services

| Dependency | Why the business relies on it |
|---|---|
| Trade8:Risk | The authorization verdict is the gate for every acceptance |
| Trade8:Router | Turns accepted orders into broker traffic and reports fills back |
| Trade8:Feed | BBO snapshot for close-only/flatten pricing; triggers strategies |
| Trade8:Pnl | Position context for flatten planning (via its own service) |
| Sentin8:Cron | Session opening/closing and scheduled sweeps |
| Sentin8:Func | Runs the flattening/basket strategy modules |
| Sentin8:Auth + Shield | Identity, live role grants, and the authorization decision |
| Sentin8:Bus | Delivery of desk events to Router/Risk/PnL (outbox-first) |
| Sentin8:Sys | Audit ingest and service administration |
| Sentin8:DB | Durable persistence (schema `desk`, db `trade8`) |

## 9. Acceptance Criteria

1. A trader can submit, see, cancel, and replace orders end-to-end through the
   app with every step authorized and journaled.
2. A denied order is visible in the blotter with its reject reason and trace.
3. Session close parks overnight orders; next preopen restores them intact.
4. The locate pool reflects accepts/cancels/adjustments within the same
   transaction that changed the order.
5. Admins manage catalogs, offices, borrow lists, and session state through the
   admin workspace without engineering involvement.
6. Compliance can reconstruct any order's full history (who/what/when/why).
7. Basket submission reports per-item outcomes and never blocks on one item's
   failure.

## 10. Open Business Questions

- Should basket submission support an async "request then progress" mode for
  very large baskets (the declared `trade8:desk:order:batch-requested` trigger
  is unused today)?
- Does the business need per-reservation borrow events for downstream
  systems, or is the summary event sufficient?
- Office-manager cross-environment flatten (supervising multiple desks) -
  product decision pending.
- Catalog replication across offices/environments: one owner catalog per
  environment vs a centrally-owned, event-replicated catalog.

---

**Architecture & Design:** trade8:desk and its underlying concepts were designed by [Hamid Jahanzad](https://www.linkedin.com/in/hamidjahanzad/).
