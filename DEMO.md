# Pulse POC — Demo Script

**Duration:** 15-20 minutes live + 5-10 min Q&A

**Audience adaptations:**
- **PMs / leadership** — emphasize Act 2 (cross-schema question) and Act 5 (vision). Skip the `sql` command deep-dive.
- **Data team / platform** — emphasize Act 1 (default filters Claude inferred), Act 3 (verifier), Act 4 (what's novel vs. what exists).
- **The four integration owners** (George, Shreya, Nicholas, Robyn) — emphasize Act 4 + Act 5. Their question is: "where does my tool fit?"

---

## Pre-demo checklist

### 30 minutes before

- [ ] Open a fresh terminal. Large font (18pt+). Dark background, light text.
- [ ] `cd /Users/vijeta.karani/pulse-poc`
- [ ] Verify `.env` has working tokens (Databricks PAT not expired, Anthropic key valid)
- [ ] Run `npm run pulse` once, let setup finish, run `which employers have the highest outstanding invoice amounts`, confirm it works, `exit`. This warms the cache for hs_employerinvoice + hs_accounts_customer so your live demo is fast.
- [ ] Open browser tabs (in a second monitor if possible):
  - `https://github.com/hh-vijetakarani/pulse-poc/blob/main/ARCHITECTURE.md` — the architecture section
  - `https://hinge-health.github.io/hinge-data-catalog` — Hinge Data Catalog
  - `#query-now-sql-bot` in Slack — to reference QueryNow

### 5 minutes before

- [ ] Clear the terminal. Don't run setup yet — starting from a clean boot is part of the demo.
- [ ] Close Slack notifications, calendar reminders, anything that pops up.
- [ ] Test network: `curl -I https://hingehealth.cloud.databricks.com` — if that hangs, you have bigger problems.

### Fallback plan

If live fails partway through:
- **Plan B:** screenshots of the three flagship moments (saved in `docs/screenshots/` — or make them during your practice run). Cut to "here's what it looked like when I ran it this morning."
- **Plan C:** walk through ARCHITECTURE.md + README.md instead. The docs carry the story.

Rehearse Act 1-2 at least twice the day before. If something breaks during rehearsal, debug before the demo, not during.

---

## The arc

1. **Cold open** (30s) — one sentence setting expectations
2. **Act 1: simple question** (3 min) — prove end-to-end works
3. **Act 2: cross-schema question** (4 min) — the flagship moment
4. **Act 3: trust** (3 min) — how users know what to trust
5. **Act 4: what's novel vs. what exists** (3 min) — honest positioning
6. **Act 5: vision** (3 min) — how this plugs into HH's ecosystem
7. **Q&A** (5-10 min)

---

## Cold open

> **Say:** "I want to show you what a data question looks like when the tool has no pre-configured schema list, no hand-curated Genie Space, and no one has told it what any of our tables mean. It auto-discovers, figures out which schemas matter, builds context on the fly, generates SQL, audits it for hallucinations, runs it against prod, and gives me a business answer. About 20 seconds per question."

> **Don't say:** "I built this in 2 days." Don't dismiss the architecture work.

**Live:** `npm run pulse`

---

## Act 1 — First question: prove it works

**While setup runs** (~30s):

> "Notice: I didn't configure 15 schemas. It's running a glob pattern — `hs_*` — against `information_schema.schemata`. Every schema matching that pattern becomes eligible. If a new one appears in prod tomorrow, it shows up here automatically."

**When the banner appears**, point at: `15 schemas discovered │ 15 active │ 0 built`

> "None of them are built yet. We only pay the build cost when we ask a question that needs it."

**Type:**
```
how many nodes do we have by type
```

**While it runs** (~12 seconds total):

> "Watch the three phases: **Routing** — a cheap Haiku call picks the 1-3 most relevant schemas. **Classifying** — Sonnet picks the specific tables and filters. **Planning query** — generates the SQL. **Executing** — runs it against our warehouse. **Summarizing** — turns the rows into English."

**When the answer appears:**

> "20,770 nodes across 8 types. Providers, merchant locations, access points. This is a polymorphic schema — the `type` column determines the shape of the `properties` JSON blob — and the system knew that from the protobuf definitions. No one told it."

**Now show the SQL.** Type:
```
sql
```

> "Look at the WHERE clause:
> ```
> WHERE terminated_at IS NULL
>   AND __is_deleted = false
>   AND aliased_to = id
> ```
> Three default filters. I didn't write them. The knowledge graph inferred them from column names plus sample data. `aliased_to = id` means 'canonical non-deduped record only' — that's a subtle HS convention, and the system picked it up."

**The payoff line:**

> "If I'd written that SQL by hand, I'd probably have forgotten that `aliased_to` filter. That's the kind of business rule that quietly bloats everyone's numbers. The system applies it automatically every time."

---

## Act 2 — Cross-schema question: the flagship

> **Say:** "Now the question that wouldn't work in Genie Spaces, wouldn't work in Mode, and would take an analyst 30 minutes."

**Type:**
```
which employers have the highest outstanding invoice amounts
```

**While it runs** (~15 seconds if cache is warm, otherwise ~2 min for cold build):

> "Routing... Haiku picked **two** schemas this time: `hs_employerinvoice` and `hs_accounts_customer`. This is a cross-schema question."

If it triggers a cold build: use the wait to pivot to architecture:

> "While it builds the context, let me show you the architecture..."
> [Switch to ARCHITECTURE.md — show the three-tier diagram]
> "The first time we touch any schema, there's a one-time cost: sample tables (PHI-safe), parse any protos, call Claude once to build the knowledge graph. After that, everything's cached. Watch — it's just about done."

**When the answer appears:**

> "Two employers, total outstanding $3,579.73. Brookdale $3,321. Staples $258.73."

**The killer observation** — zoom in on this line:

> "'Results Physiotherapy' and 'Results Physiotherapy Together With Ascension Saint Thomas' appear as separate merchants — if these are the same brand, combined they'd be 701 locations, surpassing Athletico.'"

> "That's a data-quality finding. Nobody asked it to look for duplicates. It surfaced because the tool saw both entities in the data and noticed the pattern. This is the kind of observation that, in a normal PM workflow, gets missed."

**Now show the SQL** (`sql`):

Point at the JOIN across `prod.hs_employerinvoice.employerinvoice_employer_invoices` and `prod.hs_accounts_customer.accounts_customer_employer_account_details`.

> "Cross-schema JOIN. Proper catalog qualification. Default filters applied on both tables. Used the right employer-id column as the join key — which is named differently in each schema."

---

## Act 3 — The trust story

> **Say:** "Obvious next question: how do I know when to trust an answer from this tool?"

### Show the plan command

**Type:**
```
plan how many merchant locations offer imaging services
```

> "The `plan` command shows what it WOULD do without actually executing. I see which schemas it picked, which tables, which filters, which enum values. I can veto before a single token gets spent on SQL generation or execution."

### Show the verifier

**Type:**
```
verify on
```

> "This enables a second Haiku call between SQL generation and execution. It audits the generated SQL against the schema context — flags invented columns, bad enum values, missing default filters. About two cents and two seconds per question. Worth it for high-stakes questions."

**Run a question** (any of the cached ones):
```
how many nodes do we have by type
```

**Point out the new line:** `🔍 Verifying SQL... ✓ verified`

> "Haiku reviewed the SQL and confirmed nothing looks invented. If it had flagged issues, it would either suggest a fix — which we'd auto-retry — or block execution and tell me to review."

### Show the save flow (the trust compounding)

> "Here's the most important piece for 'trust over time.'"

**Type:**
```
save
```
Prompt: "top employers by outstanding invoice amount"

> "Now that question-SQL pair is cached. Next time someone asks a similar question — even reworded — it matches via keyword overlap and returns this SQL without calling Claude at all. Zero hallucination risk. The system gets more trustworthy with use, not less."

**The payoff:**

> "In production, every validated answer becomes a Tier 1 candidate — human-reviewed, promoted into the Hinge Data Catalog. Shreya's team goes from 177 curated metrics to 500 over a year, without having to write a single one from scratch. Users validate; we promote. That's the feedback loop."

---

## Act 4 — What's novel vs. what exists

> **Say:** "I want to be honest about what's genuinely new in this POC and what isn't. There are three novel ideas here. Everything else reinvents infrastructure we already have at HH."

**The three novel things:**

> **1. Proto-first context.** Nobody at HH currently uses our protobuf definitions to teach Claude about our data. `hs_graph` alone has 117 messages and 10 enums. That's why the tool knew about polymorphic JSON access and enum stringification without being told. Demonstrate:

**Type:**
```
enums
```

> "Look — `EntityType`, `RelationType`, `ProcedureCodeModifierType` — these come from `.proto` files. Every one of those values is a string Claude uses in `WHERE type = '...'` clauses. Before this, those protos were going unread."

> **2. Auto-discovery.** Every other NL-to-SQL tool at HH — QueryNow, Genie Spaces, databricks-rules — requires someone to hand-curate a schema list. This one uses a glob pattern. Watch:

**Type:**
```
schemas
```

> "15 schemas discovered automatically. If Hector Chu adds a `hs_care_team` schema tomorrow, it appears here on next startup. No config change."

> **3. Cross-schema routing.** That employers-and-invoices question only worked because a cheap Haiku pre-filter picked **two** schemas from 15, and the tool built both on demand. Neither QueryNow nor Genie Spaces can do cross-space questions."

**Now the honest pivot:**

> "Everything else reinvents infrastructure we already have. The Databricks client duplicates `mcp-server-analytics`. The PAT-based auth is worse than service principals. The CLI REPL isn't what PMs want — they want Slack via QueryNow. And the derived metrics are a poor-man's version of the Hinge Data Catalog's 177 curated ones."

> "Roughly 70% of this POC should be deleted in favor of existing HH tools. The 30% that survives is the three novel pieces plus the thin orchestration that wires them together."

---

## Act 5 — Vision

**Switch to ARCHITECTURE.md in browser — show the three-tier diagram.**

> "Here's what the production version looks like. Not a standalone tool. An orchestrator that composes three tiers:"

> **"Tier 1 — Hinge Data Catalog lookup.** 🟢 Verified metric. 177 human-curated definitions. Zero hallucination risk. Safe for decision-making."

> **"Tier 2 — Genie Spaces dispatch.** 🔵 Curated space. 6+ existing spaces for curated domains. Bounded hallucination, native follow-up memory."

> **"Tier 3 — The novel bits of this POC, productionized.** 🟡 Generated and audited. For the 15 `hs_*` schemas that don't have a Genie Space yet. Passes through the verifier. User can review SQL before trusting."

> **"One entry point — probably Slack via QueryNow's Glean shell.** PMs already use QueryNow. We're not building a new bot next to it; we're extending its reach into schemas Genie doesn't cover."

> **"Every answer carries a trust badge.** PMs learn the gradient within a week — 🟢 is gospel, 🟡 wants a quick SQL review, 🟠 is exploratory only."

**The close:**

> "Net build effort: 8-12 weeks. Roughly 70% of the production stack is reused — `mcp-server-analytics` for execution, Hinge Data Catalog for metrics, Genie Spaces for curated answering, QueryNow for the UI shell, service principals for auth. The 30% that's new is the three novel pieces plus trust-badge routing."

> "The framing that matters most: this isn't a new NL-to-SQL tool. It's the orchestrator that routes between what HH already has, plus a Tier 3 fallback for schemas not covered yet. If anyone takes away 'we're building another Genie,' they've misread it."

---

## Q&A — pre-prepared answers

**Q: How does this compare to QueryNow?**
> "QueryNow is great for schemas with a curated Genie Space. This sits behind it as Tier 3 for schemas QueryNow's Spaces don't cover — currently most of HS, HingeSelect, and the 15 `hs_*` schemas. Not a competitor; a backstop for coverage gaps."

**Q: Can Tier 3 hallucinate?**
> "Yes — that's the whole reason for the trust tiers. Tier 1 is pulled straight from the Catalog, can't hallucinate. Tier 2 is bounded by the Genie Space's curation. Tier 3 has the verifier catching most hallucinations before execution, and the validated-query cache means every verified answer becomes hallucination-free next time."

**Q: What about PHI?**
> "Column-name pattern filtering handles the basics during sampling. There's a gap today — the JSON extraction paths in SQL aren't filtered. That's explicitly called out in the README as not-production-ready. The fix is a JSON-path-aware filter layer. Rajagopal's team would own that review."

**Q: Who owns this if it goes into production?**
> "TBD — that's a conversation I need to have with Data Platform. The POC was me exploring the idea. My proposal in ARCHITECTURE.md assumes a joint ownership model between DE (for the backend) and DS (for Catalog integration)."

**Q: Why didn't you start from existing tools?**
> "I should have — and I learned that the hard way. 70% of the POC reinvented infrastructure. But the process was what made it obvious what's genuinely novel. That 30% is what I'm proposing to productionize, integrated with what exists."

**Q: What's the cost per question?**
> "**Tier 1:** zero Claude cost (Catalog lookup only). **Tier 2:** whatever Genie charges. **Tier 3:** about $0.01-0.05 depending on complexity, plus ~$0.002 for the verifier. With prompt caching on the schema context, the per-question cost stays flat as the system scales."

**Q: How long until first PMs can actually use it?**
> "**Phase 1** in ARCHITECTURE.md — 3 weeks. Doesn't require any of this POC's code. Just needs a HingeSelect Genie Space (George Nakhleh) and HS-specific Catalog entries (Shreya Kuruvilla) wired into QueryNow. That gets PMs a functional Slack experience today. **Phase 2** — the unified orchestrator — is 5-10 weeks after that."

**Q: How do you handle data freshness?**
> "KG cache TTL is 24 hours. Schema changes invalidate that schema's KG. In production we'd hash `information_schema.columns` per schema and rebuild only affected KGs on change — described in the ARCHITECTURE doc's Risks section."

**Q: What if Claude is down?**
> "Tier 1 (Catalog lookup) is pure Python, no Claude dependency. Works in a Claude outage. Tier 2 is Databricks Genie, also Claude-independent from our side. Only Tier 3 has a hard Claude dependency. For critical PM questions, Catalog-backed Tier 1 coverage is the fallback."

**Q: What's the security posture?**
> "Today — not production-ready. PAT in `.env`, user-scoped, no audit log beyond `feedback-log.json`. Production would be service principal auth (HINGE_SELECT_PHI_SP), PHI-path filter, every question + SQL logged centrally, tier usage metrics per user. None of that is built yet."

**Q: What do you need from me to move this forward?**
> "Depends on your role. If you're George: a HingeSelect Genie Space conversation. If you're Shreya: a path for Catalog integration + promotion workflow. If you're Nicholas: a chat about whether the proto parser lives in `mcp-server-analytics` or a separate module. If you're leadership: ownership call — who runs this in production?"

---

## Don't do / do cheatsheet

### Don't

- Don't run `use hs_*` live — takes 15-20 min to build all 15 schemas
- Don't say "I built this in 2 days" — undersells the design thinking
- Don't say "this replaces QueryNow" — it extends, it doesn't replace
- Don't promise a ship date — Phase 1 is 3 weeks in the doc, but that depends on external teams
- Don't do a live demo of a question against a cold schema unless you have 3+ minutes to fill
- Don't show the `.env` file on screen — even if secrets are rotated
- Don't scroll through source code during the demo — the docs tell the story; code is for a separate technical session

### Do

- Do emphasize "composition, not competition" every chance you get
- Do point at the 🟢🔵🟡🟠⚪ badges repeatedly — it's the product, not a feature
- Do name-drop the four integration partners (George, Shreya, Nicholas, Robyn) — signals you've done the diligence
- Do reference actual HH users (PMs, EMs) by role — "this is what Molly Glauberman's enrollment questions look like"
- Do show `sql` and `plan` — transparency builds trust
- Do pause for questions after each Act — don't monologue for 20 minutes straight

---

## One-liner versions

**90-second elevator pitch (for a quick hallway demo):**

> "HH has several NL-to-SQL tools today — QueryNow, Genie Spaces, the Hinge Data Catalog, databricks-rules. None of them cover novel schemas automatically, and none of them route across schemas. I built a POC that does both: auto-discovers every `hs_*` schema in prod, uses our protobuf definitions as a context source, routes questions with a cheap Haiku pre-filter, and audits the generated SQL before executing. The proposal isn't to ship this standalone — it's to extract the three novel pieces into an orchestrator that composes with the existing tools, behind QueryNow's Slack shell. Trust-graded answers: verified metrics (Catalog), curated (Genie), generated + audited (new). 8-12 weeks of build, 70% reuse."

**30-second elevator pitch:**

> "A three-tier orchestrator that routes data questions to Hinge Data Catalog (verified metrics), Databricks Genie Spaces (curated answers), or a novel auto-discovering context layer (generated + audited, for uncurated schemas). One Slack entry point via QueryNow. Every answer shows its trust tier. Net new code: about 30% of this POC, productionized. The other 70% reuses existing HH tools."

---

## Post-demo follow-up

Send this email to attendees within 24 hours:

> **Subject:** Pulse POC demo — follow-up
>
> Thanks for watching. Quick recap:
>
> - **Repo:** https://github.com/hh-vijetakarani/pulse-poc (private)
> - **Architecture doc:** [link to ARCHITECTURE.md in repo] — the full build/reuse decisions + phasing
> - **Clone + try it yourself:** instructions in README.md
>
> Next steps I'm pursuing:
> 1. Conversation with George Nakhleh about a HingeSelect Genie Space
> 2. Conversation with Shreya Kuruvilla about Catalog integration + the promotion workflow
> 3. Conversation with Nicholas DiQuattro about contributing the proto parser / auto-discovery into mcp-server-analytics
> 4. Conversation with Robyn Latchford about extending QueryNow's scope
>
> Questions, pushback, "have you considered X" — all welcome. Reply to this or grab time on my calendar.
