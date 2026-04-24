# Pulse POC

A POC for schema-agnostic natural-language querying of Databricks, using LLM-inferred
knowledge graphs and proto/dbt-enriched context.

**Status:** working POC, ~1,600 lines of TypeScript, built in 2 days. Not production.
Validates specific design choices before integrating with Hinge Health's existing
NL→SQL ecosystem (QueryNow, Genie Spaces, Hinge Data Catalog, `mcp-server-analytics`).

For the production architecture + integration plan, see [ARCHITECTURE.md](./ARCHITECTURE.md).

---

## What it does

Point it at a Databricks workspace. It:

1. Auto-discovers every schema matching a glob pattern (e.g., `hs_*`) via
   `information_schema.schemata`
2. For the first question touching a schema, samples tables (PHI-safe) + parses any
   protobuf definitions + reads any dbt YAML docs + builds a knowledge graph via one
   Claude call
3. Routes each question through a cheap Haiku pre-filter that picks the 1-3 most relevant
   schemas
4. Classifies the question, generates SQL, optionally audits the SQL with a Haiku
   verifier pass, executes it via the Databricks Statement API, and summarizes results
   in plain English

All without hand-curating a schema catalog. The knowledge graph is Claude's synthesis of
three sources: protobuf definitions (highest fidelity), Databricks schema metadata
(ground truth), and sample data (reality check).

---

## What makes it different from existing HH tools

| HH tool | What it is | Gap this POC targets |
|---|---|---|
| QueryNow | Glean agent over hand-curated Genie Spaces | Doesn't cover novel schemas (no HS Space yet) |
| Query Future | Slack bot with context/ directory of schemas | Manual context curation per schema |
| mcp-server-analytics | Claude Code MCP for raw SQL | Not NL→SQL; no routing or KG |
| Databricks Genie Spaces | Native NL→SQL in Databricks | Each space hand-curated |
| Hinge Data Catalog | 177 curated metric definitions (OSI YAML) | Metric-level only, not table-level |
| databricks-rules | Claude Code context repo with static schema CSVs | Dev-facing, no chatbot surface |

The novel pieces this POC validates:

- **Proto-first context** — uses `.proto` files from `Hinge-Select/hinge-select` as a
  primary semantic source. Nobody else uses them. hs_graph alone has 117 messages +
  10 enums worth of semantics currently wasted.
- **Auto-discovery** — `hs_*` glob in config → 15 schemas surfaced automatically
- **Cross-schema routing** — Haiku picks relevant schemas per question; builds on-demand
- **Validated-query reuse** — verified answers become hallucination-free templates

---

## Quickstart

### Prerequisites

- Node.js 22+
- `gh` CLI (for fetching `.proto` files from private GitHub repos)
- Databricks workspace with a SQL warehouse you can query
- Anthropic API key

### Setup

```bash
git clone <this-repo>
cd pulse-poc
npm install
```

Create a `.env` file:

```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=abc123def456
ANTHROPIC_API_KEY=sk-ant-...
```

> **Security note:** The PAT in `.env` is a stopgap. For any shared or production use,
> switch to Databricks service-principal auth (see `ARCHITECTURE.md` → Security).

Optional — fetch the `.proto` files the POC was tested against (requires `gh` CLI with
access to `Hinge-Select/hinge-select`):

```bash
mkdir -p protos
for f in node_properties edge_properties graph; do
  gh api -H "Accept: application/vnd.github.raw" \
    "repos/Hinge-Select/hinge-select/contents/protobuf/hingehealth/kg/storage/${f}.proto" \
    > "protos/${f}.proto"
done
gh api -H "Accept: application/vnd.github.raw" \
  "repos/Hinge-Select/hinge-select/contents/protobuf/hingehealth/kg/workflows/workflows.proto" \
  > "protos/workflows.proto"
```

> `.proto` files are gitignored — they're internal HH artifacts kept in their source repo.
> The POC will gracefully fall back to schema-only context if `protos/` is empty.

### Run

```bash
npm run pulse
```

First start:
- Loads `schemas.yaml` (auto-discovers schemas matching `hs_*` in the `prod` catalog)
- Connects to Databricks
- Lists matched schemas
- Active schema at startup: `hs_graph` (by default — edit `schemas.yaml` to change)
- For the active schema: discovers tables, samples, parses protos, builds the KG
  (~90-180s first time, cached for 24h thereafter)

Then drops you into a REPL.

---

## Configuration — `schemas.yaml`

```yaml
auto_discover:
  catalogs: [prod]
  include: ["hs_*"]          # glob patterns; everything matching is eligible
  exclude: []                # optional exclusions

defaults:
  active: hs_*               # "all" | <id> | <glob> | <catalog>.*
  phi_skip_patterns:
    - name
    - first_name
    - email
    - phone
    # ... etc
  phi_skip_patterns_add:
    - mrn
    - patient_id
    - member_id
  large_fleet_threshold: 10  # above this, Haiku pre-filter engages

overrides:
  hs_graph:
    aliases: [hs, graph, kg]
    protos: protos/
```

No hand-maintained table lists. Add a new schema matching the glob in Databricks and it
appears on next startup.

---

## REPL commands

| Command | What it does |
|---|---|
| `schemas` | List all configured schemas with active marker + build status |
| `use <id\|all>` | Switch active schema(s). `use hs_*` activates all hs_* schemas. |
| `tables` | List tables in active schemas |
| `table <id.name>` | Full details: columns, joins, default filters, enum values |
| `metrics` | Derivable metrics Claude inferred for each schema |
| `funnel` | Detected funnel workflows |
| `relationships` | Entity relationships (proto-sourced or inferred) |
| `enums` | Proto enum types and their valid values |
| `graph` | Full KG summary across active schemas |
| `plan <question>` | Show classification without executing SQL — useful pre-flight |
| `verify on` / `verify off` | Toggle Haiku SQL-audit before execution |
| `sql` | Show the last executed SQL |
| `save` | Save last question + SQL as a validated query (zero-cost reuse) |
| `good` / `bad` | Rate the last answer; `bad` prompts for correction |
| `correct: <rule>` | Add a correction rule the system applies to future questions |
| `stats` | Feedback accuracy stats |
| `validated` | List saved validated queries |
| `refresh` | Clear cache, rebuild from scratch |
| `exit` | Quit |

Anything else you type is treated as a question. Questions run through:
**route → classify → generate SQL → (verify) → execute → narrate**

---

## Architecture (POC)

```
.env + schemas.yaml
      │
      ▼
┌─────────────────────────┐
│ Config loader           │  Loads YAML, expands auto-discover rules via Databricks
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Per-schema discovery    │  information_schema dump, table sampling, proto/dbt parse
└──────────┬──────────────┘  Runs on-demand when schema first touched
           │
           ▼
┌─────────────────────────┐
│ KG builder (Sonnet 4.6) │  One Claude call per schema, sharded if >30 tables
└──────────┬──────────────┘  Normalizer + linter guard against LLM output variance
           │
           ▼
┌─────────────────────────┐
│ Question routing        │  Haiku picks 1-3 schemas (all discovered, built or not)
│ (Haiku 4.5)             │  Builds on-demand if picked schema is cold
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Classifier (Sonnet 4.6) │  Picks specific tables + joins + filters + enum context
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ SQL generator           │  Databricks-dialect SQL, fully qualified, default filters
│ (Sonnet 4.6)            │  applied, enum values respected
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Verifier (Haiku 4.5)    │  OPTIONAL (verify on). Audits SQL vs. schema context.
└──────────┬──────────────┘  Flags invented columns, bad enum values, missing filters.
           │
           ▼
┌─────────────────────────┐
│ Safety-gated execution  │  Rejects INSERT/UPDATE/DELETE/DROP/ALTER/CREATE.
│ (Databricks REST API)   │  Adds LIMIT 10000 if missing.
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Narrator (Sonnet 4.6)   │  Turns rows into business English with actual numbers
└──────────┬──────────────┘  Cites seed context (per-schema briefing)
           │
           ▼
┌─────────────────────────┐
│ Feedback logger         │  Every question logged for future analysis
└─────────────────────────┘
```

### Source files

```
src/
├── index.ts            Orchestrator + REPL + command dispatch  (~750 LOC)
├── config.ts           schemas.yaml loader + auto-discovery expansion  (~200 LOC)
├── databricks.ts       Databricks Statement Execution API client  (~130 LOC)
├── discovery.ts        Schema + tag + PHI-safe sample discovery  (~230 LOC)
├── proto-parser.ts     Parses .proto files, preserves comments, matches to tables  (~280 LOC)
├── dbt-parser.ts       Parses dbt_project.yml + models/**/*.yml  (~130 LOC)
├── knowledge.ts        KG builder (Claude Sonnet), sharded build, seed context  (~340 LOC)
├── kg-validator.ts     Normalizer + linter; heals LLM output variance  (~160 LOC)
├── router.ts           Haiku-based cross-schema router  (~100 LOC)
├── classifier.ts       Claude-based classifier w/ validated-query fast path  (~300 LOC)
├── sql-generator.ts    Claude-based SQL generator w/ focused context  (~210 LOC)
├── verifier.ts         Haiku SQL audit (optional)  (~120 LOC)
├── narrative.ts        Claude-based narrative synthesis  (~100 LOC)
├── engine.ts           Safety-gated execution + table formatting  (~100 LOC)
├── learning.ts         Correction + validated-query + feedback persistence  (~180 LOC)
└── types.ts            TypeScript interfaces  (~180 LOC)
```

Total: ~3,500 lines of TypeScript. Roughly 30% is novel (proto parser, auto-discovery,
KG builder with proto context, router, verifier, validated-query reuse). The other 70%
reinvents infrastructure that already exists at HH (Databricks client, auth, narrative
synthesis, execution).

---

## Known limitations

- **PAT-in-`.env` auth** — fine for local POC, unsafe for production. Switch to service
  principals (HINGE_SELECT_PHI_SP) before any shared use.
- **PHI filter only covers column names** — the `properties` JSON column can still contain
  PHI (e.g., `properties:provider_individual.given_name`). Claude can extract these freely.
  Production needs a JSON-path-aware filter.
- **No cross-catalog support** — `schemas.yaml`'s `auto_discover` takes a catalog list,
  but the classifier hasn't been tested with schemas from multiple catalogs.
- **Single-dialect (Databricks)** — all SQL generation assumes Databricks syntax
  (`DATE_TRUNC`, `:` JSON paths, `DATEADD`). Porting to Snowflake/BigQuery requires a
  dialect abstraction that doesn't exist yet.
- **No retry on SQL execution errors** — if Databricks rejects a generated query, the
  question fails. Production needs closed-loop retry (re-prompt Claude with the error).
- **No memory for follow-up questions** — every question is independent.
- **Tier 3 hallucination rate unknown** — no benchmark data yet. The verifier helps but
  isn't measured.
- **Large `hs_*` fleet first-run latency** — activating all 15 schemas via `use hs_*`
  takes 10-20 min sequentially. Production needs parallel first-run with rate limiting.

---

## What we'd change for production

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full plan — specifically the
**"POC → Production: the architectural shift"** section, which covers per-component
reuse-vs-build rationale (why `mcp-server-analytics` replaces our Databricks client,
why Hinge Data Catalog replaces our `derivable_metrics`, why QueryNow replaces the
REPL, etc.) and **"What the product becomes"** for the next-phase vision.

Summary of the changes:

### Delete (reinvented existing infrastructure)

- **`databricks.ts`** — replaced by `mcp-server-analytics` MCP
- **`engine.ts`** (execution) — replaced by `mcp-server-analytics`
- **`narrative.ts`** — replaced by tier-aware narrator (or QueryNow's existing summary layer)
- **`.env`-based auth** — replaced by service principal auth via CLI SSO
- **REPL loop in `index.ts`** — replaced by Slack bot shell (QueryNow pattern) +
  web UI + Claude Code MCP

### Keep (novel, worth productionizing)

- **`proto-parser.ts`** — nobody else uses HH's protos; high-fidelity semantic source
- **`dbt-parser.ts`** — complements; reads dbt YAML docs
- **`config.ts` + `auto_discover`** — makes new schemas zero-config
- **`knowledge.ts`** — proto-aware KG builder
- **`kg-validator.ts`** — normalizer + linter; guards against LLM output variance
- **`router.ts`** — cross-schema routing
- **`classifier.ts`** + **`sql-generator.ts`** — the NL→SQL pipeline
- **`verifier.ts`** — Haiku SQL audit; catches most Tier 3 hallucinations
- **`learning.ts`** — validated-query cache is the mechanism by which the system gets
  more trustworthy over time

### Add (missing from POC)

- **Intent classifier** — picks Tier 1 (Hinge Data Catalog metric lookup) vs. Tier 2
  (Genie Space) vs. Tier 3 (KG+SQL generation) per question
- **Hinge Data Catalog integration** — query the 177 curated metrics first; fall back to
  generation only when nothing matches
- **Genie Space dispatch** — delegate to Databricks' native NL→SQL for schemas with a
  curated Space
- **Trust badges** — every answer carries a tier-based badge so users calibrate trust
- **Feedback storage** — persisted 👍/👎 per answer; promote verified Tier 3 answers into
  Hinge Data Catalog as curated metrics
- **Follow-up memory** — for non-Genie tiers (Genie has its own)
- **Closed-loop SQL retry** — if Databricks rejects a query, feed the error back to Claude
- **JSON-path-aware PHI filter** — extend the column-name filter into JSON extraction
  paths Claude generates
- **Schema hash-based cache invalidation** — detect schema changes automatically
- **Observability** — per-tier latency, cost, accuracy metrics

### Why in one paragraph

Pulse validated three patterns nobody else at HH is doing (proto-first context,
auto-discovery, cross-schema routing). The right next step isn't polishing Pulse — it's
extracting those three patterns into production code that composes with the rest of the
HH ecosystem. The output isn't "a better Pulse" — it's an orchestrator that picks between
Hinge Data Catalog (verified), Genie Spaces (curated), and a productionized Tier 3 built
from Pulse's novel bits (generated + audited), with a trust badge on every answer so users
know what they're getting.

---

## Security notes

1. **Rotate the PAT and Anthropic API key** after any demo session where they've been
   shared. These are personal tokens, not meant for long-lived use.
2. **Never commit `.env`** — `.gitignore` protects this, verify before every push.
3. **Databricks PAT scope is the full workspace** — a service principal scoped to the
   target schemas is the correct replacement. Contact Rajagopal Parthasarathi for
   the HINGE_SELECT_PHI_SP access process.
4. **PHI column-name filter is not enough** — see "Known limitations." Do not use this
   POC on schemas with PHI in JSON payloads without adding a JSON-path filter.
5. **Every question + SQL + result is logged** to `learning/feedback-log.json`. This file
   is gitignored but present on disk. Clear it before any repo hand-off:
   `rm learning/feedback-log.json && echo "[]" > learning/feedback-log.json`.

---

## References

| Resource | What's there |
|---|---|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Full product vision + integration plan |
| [schemas.yaml](./schemas.yaml) | Fleet configuration (auto-discovery rules, overrides) |
| [`Hinge-Select/hinge-select` · `protobuf/hingehealth/kg/`](https://github.com/Hinge-Select/hinge-select/tree/main/protobuf/hingehealth/kg) | Source of protos used in POC |
| `hinge-health/databricks-rules` | Pattern this POC draws from for schema CSVs + topic docs |
| `hinge-health/mcp-server-analytics` | Execution layer that production Pulse should reuse |
| `hinge-health/hinge-data-catalog` | Tier 1 metric registry production Pulse should integrate with |
| Hinge Data Catalog (hosted) | https://hinge-health.github.io/hinge-data-catalog |

---

## Contact

Built as a POC by Vijeta Karani (April 2026). Questions, feedback, and "can you just
point me at the production version" requests welcome.

If you'd like to pick up any of the "keep for production" pieces — especially the proto
parser or the auto-discovery logic — open an issue and let's coordinate with the relevant
tool owners (Nicholas DiQuattro for mcp-server-analytics, George Nakhleh for Genie Spaces,
Shreya Kuruvilla for the Catalog).
