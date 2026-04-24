# Pulse — Architecture & Product Vision

**Status:** design document, not code-as-written. Describes the production system the POC is pointing toward and the integration strategy with HH's existing data tooling.

**Last updated:** 2026-04-24

---

## TL;DR

We want one place at Hinge Health where anyone — PM, EM, analyst, or engineer — can ask a
natural-language question about our Databricks data and get a reliable answer. The product
is not "another chatbot" — it's a **trust-graded answering engine wrapped in a chatbot UI**,
built by composing existing HH investments rather than rebuilding any of them.

Every answer carries a trust badge (verified metric / curated space / generated + audited)
so users learn quickly which answers to act on directly vs. which to review before trusting.

Net build effort: **~8-12 weeks of focused work**, with roughly **70% of the production
stack reused from existing HH tools** (mcp-server-analytics, Hinge Data Catalog, Genie
Spaces, QueryNow, databricks-rules patterns).

---

## Problem framing

**For whom:** Hinge Health PMs, EMs, analysts, and data-curious non-SQL users across the org.

**What they want:** Ask a question in natural language. Get a reliable answer. Trust it
enough to act on it.

**What they have today:** Fragmented.
- QueryNow handles some Genie-covered schemas
- Hinge Data Catalog covers some metrics
- Mode has saved reports
- databricks-rules is a dev context repo for Claude Code
- Each requires knowing which tool to use for which question

**Gap this product fills:** One entry point. Any question. Route to the right backend
automatically. Communicate confidence so the user knows what to trust.

---

## Architecture

```
╔══════════════════════════════════════════════════════════════════════════╗
║                           USER SURFACES                                  ║
║  Slack bot         Web UI (pulse.hingehealth.com)      Claude Code MCP   ║
║  (QueryNow shell)  (for non-Slack power users)         (for devs)        ║
╚══════════════════════════════╤═══════════════════════════════════════════╝
                               │
               ┌───────────────▼────────────────┐
               │   INTENT CLASSIFIER (Haiku)    │
               │   "is this a known metric?     │
               │    a Genie-covered question?   │
               │    or novel ad-hoc?"           │
               └───────┬───────┬───────┬────────┘
                       │       │       │
             Tier 1 ◄──┘       │       └──► Tier 3
             (metric lookup)   │            (generate + verify)
                               ▼
                           Tier 2
                        (Genie Space)

┌──────────────────┬───────────────────┬──────────────────────────────────┐
│   TIER 1         │   TIER 2          │   TIER 3                         │
│   Metric         │   Genie           │   KG + SQL Generation            │
│   Retrieval      │   Space           │                                  │
│                  │                   │   ┌─────────────────────────┐    │
│   ✓ Verified     │   ✓ Space owner   │   │ CONTEXT LAYER (novel)   │    │
│     by humans    │     curated       │   │ • Auto-discover hs_*    │    │
│   ✓ Zero         │   ✓ Native memory │   │ • Per-schema KG (proto  │    │
│     hallucination│     for follow-   │   │   + dbt + schema + data)│    │
│                  │     ups           │   │ • Schema CSVs           │    │
│   Hinge Data     │   ✓ Inherits      │   │ • Per-domain topic docs │    │
│   Catalog        │     Unity Catalog │   │ • Enum/funnel/join      │    │
│   (177 metrics,  │     permissions   │   │   inference             │    │
│    OSI YAML)     │                   │   └───────────┬─────────────┘    │
│                  │   Genie Spaces    │               ▼                  │
│                  │   (6+ today,      │   ┌─────────────────────────┐    │
│                  │    need HS Space) │   │ ROUTER (Haiku)          │    │
│                  │                   │   │ 1-3 schemas from active │    │
│                  │                   │   └───────────┬─────────────┘    │
│                  │                   │               ▼                  │
│                  │                   │   ┌─────────────────────────┐    │
│                  │                   │   │ SQL GENERATOR (Sonnet)  │    │
│                  │                   │   │ Produces Databricks SQL │    │
│                  │                   │   └───────────┬─────────────┘    │
│                  │                   │               ▼                  │
│                  │                   │   ┌─────────────────────────┐    │
│                  │                   │   │ VERIFIER (Haiku)        │    │
│                  │                   │   │ Audits SQL vs. context  │    │
│                  │                   │   └───────────┬─────────────┘    │
└──────────┬───────┴─────┬─────────────┴───────────────┼──────────────────┘
           │             │                             │
           └─────────────┴─────────────┬───────────────┘
                                       ▼
                      ┌─────────────────────────────────┐
                      │ EXECUTION                       │
                      │ mcp-server-analytics MCP        │
                      │ (OAuth, read-only, approved)    │
                      └────────────────┬────────────────┘
                                       ▼
                      ┌─────────────────────────────────┐
                      │ DATABRICKS                      │
                      │ via HINGE_SELECT_PHI_SP or      │
                      │ domain service principals       │
                      └────────────────┬────────────────┘
                                       ▼
                      ┌─────────────────────────────────┐
                      │ NARRATOR (Sonnet)               │
                      │ Trust-tier-aware English output │
                      └────────────────┬────────────────┘
                                       ▼
                      ┌─────────────────────────────────┐
                      │ FEEDBACK + MEMORY               │
                      │ • Validated-query cache         │
                      │ • 👍/👎 per answer              │
                      │ • Promote to Hinge Data Catalog │
                      │   if analyst verifies           │
                      └─────────────────────────────────┘
```

---

## User experience

### Slack surface (primary — ~90% of users)

**Verified metric (Tier 1):**
```
/ask how many members enrolled last month broken down by program

🟢 Verified metric from Hinge Data Catalog
**18,342 members enrolled in March 2026**
• Back Program: 11,204 (61%)
• Neck Program: 4,891 (27%)
• Joint Pain Program: 2,247 (12%)

[show SQL] [see catalog entry] [👍 accurate] [👎 wrong]
```

**Curated space (Tier 2):**
```
/ask what's the average time from enrollment to first visit

🔵 Genie Space: Care Team Activation
**Median: 4.2 days • P90: 11 days**
Derived from prod.member360.user_identity joined with first session logs.

[show SQL] [ask follow-up] [👍] [👎]
```

**Generated + audited (Tier 3):**
```
/ask which merchants have the most providers in hs_graph

🟡 Generated + audited — Tier 3
⚠ No Genie Space covers this. SQL was generated and verified.
   Review before acting on it.

**Athletico Physical Therapy leads with 672 active providers**
Top 5 merchants account for 1,912 providers combined...

[show SQL] [save as validated query] [👍] [👎]
```

**Not answerable (escape hatch):**
```
/ask what's our revenue forecast for next quarter

⚪ Not answerable
Confidence too low (28%). No catalog metric matches, no Genie Space covers forecasting,
and the prod.hs_* schemas don't appear to contain forecast data.

Suggestions:
• Check Mode: #analytics-forecasts
• Ask in #ops-analytics-assistant
```

### Web UI surface

Same trust tiers, richer display. SQL panel with edit-and-rerun. Full result tables.
Saved queries per user. Cross-schema question builder for power users.

### Claude Code MCP surface (devs)

MCP server (`pulse-schema-mcp`) exposing:
- `get_schema_context(schema_id)` → rich proto-aware KG summary
- `find_metric(question)` → Hinge Data Catalog lookup
- `generate_sql(question, scope)` → full pipeline
- `execute_sql(sql)` → via `mcp-server-analytics`

Dev writes in Claude Code: *"@pulse-schema-mcp what columns does
`accounts_customer_employer_account_details` have and how does it join to invoices?"* —
gets proto-enriched answer no human curated.

---

## Build vs. reuse

| Component | Source | Status | Effort |
|---|---|---|---|
| Slack bot shell | **Reuse** QueryNow / Glean agent pattern | In production | 0 (configure) |
| Web UI | **Build new** (React + shadcn/ui) | Novel | 3 weeks |
| Claude Code MCP | **Build new** (MCP SDK) | Novel | 1 week |
| Intent classifier (tier 1 vs 2 vs 3) | **Build** (Haiku call) | Novel | 2 days |
| Tier 1: Metric retrieval | **Reuse** Hinge Data Catalog Python engine | 177 metrics curated | 0 (integrate) |
| Tier 2: Genie Spaces | **Reuse** Databricks native API | 6+ spaces exist | 2 weeks (add HingeSelect Space) |
| Tier 3: Context layer (auto-discover + proto + KG) | **Keep from POC** | Built in POC | 2 weeks (productionize) |
| Tier 3: Router | **Keep from POC** | Built | Done |
| Tier 3: SQL generator | **Keep from POC** | Built | Done |
| Tier 3: Verifier (Haiku audit) | **Keep from POC** | Built | Done |
| Schema CSV snapshots | **Adopt** databricks-rules pattern | Template exists | 1 week (generate for hs_*) |
| Per-schema topic docs | **Adopt** databricks-rules pattern | Template exists | Ongoing (per domain) |
| Execution | **Reuse** mcp-server-analytics | Security-approved | 0 (integrate) |
| Auth | **Reuse** HINGE_SELECT_PHI_SP + service principals | Existing | 0 (use them) |
| Narrator | **Build** (Sonnet call, trust-tier-aware) | Similar to POC | 3 days |
| Validated query cache | **Keep from POC** | Built | Done |
| Feedback collection | **Build** (log to DynamoDB or Postgres) | Novel | 1 week |
| Memory for follow-ups | **Use** Genie's native (Tier 2) + **build** session state (Tier 1, 3) | Partial reuse | 2 weeks |
| Cross-referenced repo reading | **Adopt** databricks-rules pattern | Documented approach | 3 days |

**Net build effort:** ~8-12 weeks for Tier 1 product. Roughly 70% of the production stack
is reused from existing HH tools.

---

## Why each built component exists

### The orchestrator (intent classifier + router)
Without this, users have to know which tool to use for which question. The orchestrator is
the entire point of "one chatbot, any question."

### The context layer (auto-discover + proto + KG)
Genie Spaces don't scale — each is hand-curated. There are 15+ `hs_*` schemas that will
never all have Genie Spaces. The context layer is how Tier 3 can answer questions about
newly-landed schemas without someone manually onboarding each one.

**Proto awareness is the biggest single reason to build this.** Nobody else's tool uses
your protobuf definitions, and `hs_graph` alone has 117 messages and 10 enums worth of
semantics currently wasted.

### The verifier
Tier 3 answers hallucinate ~5-15% of the time. A Haiku audit pass (~$0.002, ~2s) catches
80%+ of those before they hit the user. Critical for user trust in Tier 3.

### Trust-tier-aware narrator
Different tiers need different language. Tier 1: "verified metric," confident. Tier 3:
"generated and audited — review before acting on it." Lumping all three into one narrator
style is how users stop trusting Tier 1 over time.

### The web UI + MCP
Slack is primary, but some users want persistent views (web UI) and devs want
programmatic access (MCP). Both are small if the core pipeline is clean.

### Validated query cache
Every verified Tier 3 answer becomes a Tier 1 answer next time. This is the mechanism by
which the system gets more trustworthy over time instead of drifting.

---

## Phasing

### Phase 0 — prerequisites (weeks 0-2)
- Conversations with George Nakhleh (Genie), Shreya Kuruvilla (Catalog),
  Nicholas DiQuattro (mcp-server-analytics), Robyn Latchford (QueryNow)
- Get HINGE_SELECT_PHI_SP access granted to the development environment
- Rotate the PAT in the current POC; switch to service-principal auth

### Phase 1 — HingeSelect in QueryNow (weeks 2-5)
- Work with George/Andres to build a HingeSelect Genie Space
  (`prod.hingeselect` + `prod.dbt_select`)
- Register with QueryNow
- Add HingeSelect funnel metrics to Hinge Data Catalog (with Shreya)
- **Ship:** PMs can ask HingeSelect questions in `#query-now-sql-bot`

### Phase 2 — Unified orchestrator (weeks 5-10)
- Build the three-tier orchestrator (intent classifier, router, dispatcher)
- Integrate Catalog + Genie APIs + Tier 3 fallback
- Trust-tier-aware narrator
- Feedback logging
- **Ship:** one Slack entry point routes across all three tiers

### Phase 3 — Tier 3 productionization (weeks 10-16)
- Extract POC's novel bits into production code (proto parser, KG builder, router, verifier)
- Adopt databricks-rules' schema CSV + topic doc pattern
- Onboard all 15 `hs_*` schemas as Tier 3 candidates
- Web UI for power users
- **Ship:** novel-schema questions work without hand curation

### Phase 4 — MCP server + dev surface (weeks 16-18)
- Package Tier 3 context as an MCP server
- Distribute via Claude Code
- **Ship:** devs have proto-enriched context available for any schema

---

## Trust model

Every answer shows a trust badge:

| Badge | Tier | Meaning | When to trust |
|---|---|---|---|
| 🟢 **Verified metric** | 1 | Pulled from Hinge Data Catalog; human-curated SQL | Safe for decision-making |
| 🔵 **Curated space** | 2 | Answered by a Genie Space with owner-curated tables/instructions | Trust unless the number looks off |
| 🟡 **Generated + audited** | 3 | Claude-generated SQL that passed Haiku audit | Verify SQL before acting |
| 🟠 **Generated, unaudited** | 3 (verify off) | Same as above without the verifier | Exploratory only |
| ⚪ **Not answerable** | — | Low confidence; no matching metric/space/schema | Data gap, not tool gap |

The badge is the single most important UX element. Without it, Tier 1's trust leaks down
into Tier 3. With it, users learn the gradient quickly.

---

## Non-goals

Explicitly **not** building:

- **Write access** — read-only. Enforced via service principal permissions.
- **Join across prod + external tools** (Mode, Looker) — integrates, doesn't replace.
- **Anomaly detection / alerting** — that's `data-pipeline-monitor` territory.
- **Replacement for Mode reports** — complements. PMs may still prefer Mode dashboards for
  tracked KPIs.
- **Anything forecasting-related** — points users to existing forecast tools.

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| PMs lose trust after a bad Tier 3 answer | Trust badges + `verify on` by default for PM-facing surfaces; scale Tier 3 usage gradually as validated-query cache grows |
| Cost blows up at scale | Haiku for router + verifier + intent classifier; Sonnet only for KG build (one-time) and SQL gen. Prompt caching on schema context. Est. $0.01-0.05 per question. |
| PHI exposure in narrative | Extend POC's PHI-pattern filter to JSON paths; verifier checks for PHI columns in generated SQL |
| Schema drift invalidates KG | Hash `information_schema.columns` per schema; rebuild only affected KGs on change |
| Genie Space curation bottlenecks new schemas | Tier 3 auto-handles them; Genie Spaces become a gradual upgrade, not a prerequisite |

---

## Success metrics (6 months)

- **70%** of PM data questions answered without a human writing SQL
- **30%** of those answered by Tier 1 or Tier 2 (zero-hallucination tiers)
- **Validated-query cache** contains 100+ entries, reducing ongoing cost
- **Average latency**: <10s (Tier 1), <20s (Tier 2), <40s (Tier 3)
- **Weekly active users**: 50+ across PM/EM/DS/analytics
- **Zero PHI incidents**
- **Trust-tier accuracy** (measured via 👍/👎): 95%+ Tier 1, 90%+ Tier 2, 75%+ Tier 3

The last metric determines whether this ships a v2. If Tier 3 accuracy is <70%, scale back
to Tier 1+2 only (narrower coverage). If >80%, Tier 3 becomes a genuine PM tool, not just
a dev convenience.

---

## Related HH tools (integration surface)

| Tool | Owner | Role in this architecture |
|---|---|---|
| QueryNow | Robyn Latchford, Andres Mora | User surface for Slack (Glean agent shell) |
| Hinge Data Catalog | Shreya Kuruvilla | Tier 1 metric registry |
| Databricks Genie Spaces | George Nakhleh | Tier 2 curated answering |
| `mcp-server-analytics` | Nicholas DiQuattro | Execution layer |
| `databricks-rules` | Joe Templeman | Source pattern for schema CSVs + topic docs |
| Hinge Mode Copilot | Andy Tan | Optional Mode retrieval for pre-written queries |
| Databricks MCP (hinge-de-utils) | Rajagopal Parthasarathi | Alternative execution backend |
| Mixpanel sync (Fivetran) | Data Engineering | Data source; known reliability issues |
| CDC replication | Hector Chu, Trevor Laity | Landing HS data into prod |

---

## Appendix: what the POC contributed

This POC validated three novel patterns that couldn't be bought:

1. **Proto-first context** — Using `.proto` files (117 messages, 10 enums for hs_graph) as
   a primary semantic source alongside schema + sample data. Claude correctly inferred
   polymorphic JSON access patterns (`properties:merchant.name`) and enum stringification
   (`type = 'ENTITY_TYPE_MERCHANT'`) from protos alone.

2. **Auto-discovery + glob-based routing** — `auto_discover.include: ["hs_*"]` surfaced
   all 15 HS schemas in prod automatically. Haiku router picks the right 1-3 per question
   from schema names + purpose summaries.

3. **Validated-query reuse** — Every verified answer becomes a zero-cost zero-hallucination
   pattern the system can match future questions against.

Three pieces worth productizing. The rest of the POC (Databricks REST client, auth, UI
shell, execution loop) reinvented existing HH infrastructure and should be deleted in favor
of integration.
