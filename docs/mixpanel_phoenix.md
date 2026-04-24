# mixpanel_phoenix — topic doc

Context for Pulse's KG builder + SQL generator when answering questions against
`prod.mixpanel_phoenix`. This doc describes the event model, the Care Search
funnel, key identifier columns, and known data-quality issues. Loaded by the
KG builder as a parallel context source (alongside schema + samples).

> **CRITICAL — read first.** Do NOT hardcode event names anywhere. The
> canonical event taxonomy lives in `prod.mixpanel_phoenix.event_types`. The
> raw event tables (`event`, `event_90day_mv`, `event_view`) have 2.8B–39B
> rows and DISTINCT sampling is skipped — meaning Pulse cannot enumerate
> event names from them directly.
>
> **Always query `event_types` first** to discover the actual event names
> matching a feature (Care Search, Onboarding, etc.), then use those names
> to filter the event tables. Patterns below.

---

## Event model

`prod.mixpanel_phoenix.event` is the primary fact table — one row per Mixpanel
event captured by the Phoenix (HingeSelect) client, synced via Fivetran from
Mixpanel to Databricks. **Always query `event` directly for Care Search and
other product-feature questions** — the 90-day materialized view
`event_90day_mv` is currently stale (see DQ section below) and missing entire
event families.

`prod.mixpanel_phoenix.event_types` is a **lookup table** with one row per
distinct event name in the system. **This is the source of truth for the
event taxonomy.** Always query it to discover event names rather than
guessing or hardcoding strings.

### Core columns (expected — verify against actual schema)

| Column | Meaning | Notes |
|---|---|---|
| `event_name` | The event type (e.g. `Care Search Opened`) | Treat like an enum — known values enumerate the taxonomy |
| `time` | When the event was recorded on the client | May skew if client clocks are off |
| `distinct_id` | Mixpanel's anonymous visitor ID | **PHI-adjacent** — skip in sampling, OK in WHERE clauses |
| `user_id` | HH's internal user identifier when known | Joins to `hs_accounts_customer` |
| `properties` | JSON payload with event-specific fields | Event-specific; shape varies per `event_name` |
| `insert_id` | De-dup key | Use for exactly-once analysis |
| `session_id` / `$session_id` | Session grouping | Session-scoped analysis |
| `$current_url`, `$screen_name` | Where event fired | Context for drop-off diagnosis |

> **Verification:** run `table mixpanel_phoenix.event` in Pulse to see the
> actual columns. Pulse will have already sampled distinct `event_name` values
> during KG build — the `enums` command shows them.

---

## Care Search funnel — discover events first, never hardcode

The Care Search feature in Phoenix has a multi-stage user flow (entry →
search → results → click → profile → appointment). Stage names are
illustrative — the actual event names live in `event_types` and **MUST** be
discovered at query time, not assumed.

### Step 1 — discover the actual Care Search event names

```sql
SELECT name
FROM prod.mixpanel_phoenix.event_types
WHERE LOWER(name) LIKE '%care_search%'
   OR LOWER(name) LIKE '%care search%'
   OR LOWER(name) LIKE '%caresearch%'
ORDER BY name;
```

This returns whatever events match — caller should NOT assume specific names
like "Care Search Opened." HH may use snake_case (`care_search_opened`),
title case, or domain-prefixed variants. Use what the table returns.

### Step 2 — count users who used Care Search (the simple case)

For "how many customers/users used Care Search in the last N days", query the
full `event` table directly (NOT `event_90day_mv` — see DQ note):

```sql
SELECT COUNT(DISTINCT e.distinct_id) AS users_using_care_search
FROM prod.mixpanel_phoenix.event e
WHERE e.name IN (
  SELECT name
  FROM prod.mixpanel_phoenix.event_types
  WHERE LOWER(name) LIKE '%care search%'
     OR LOWER(name) LIKE '%caresearch%'
)
AND e.time >= DATEADD(DAY, -90, CURRENT_DATE());
```

The `event` table is partitioned/clustered by `time`, so the time predicate
prunes data efficiently even though the table has 39B rows. Real event names
in the data use TitleCase with spaces (e.g. "Care Search Viewed", "Care
Search Provider Clicked") — the lowered LIKE patterns above match them.

### Step 3 — drop-off / funnel analysis

For "where are users dropping off in Care Search," use a discovery-then-pivot
pattern. The exact stage events get discovered from `event_types`:

```sql
WITH discovered_events AS (
  SELECT name AS event_name
  FROM prod.mixpanel_phoenix.event_types
  WHERE LOWER(name) LIKE '%care search%'
     OR LOWER(name) LIKE '%caresearch%'
),
user_events AS (
  SELECT
    e.distinct_id,
    e.name AS event_name,
    e.time
  FROM prod.mixpanel_phoenix.event e
  WHERE e.name IN (SELECT event_name FROM discovered_events)
    AND e.time >= DATEADD(DAY, -30, CURRENT_DATE())
)
SELECT
  event_name,
  COUNT(DISTINCT distinct_id) AS unique_users,
  COUNT(*) AS total_events
FROM user_events
GROUP BY event_name
ORDER BY unique_users DESC;
```

This shows volume by event name; ordering events along a typical funnel
(entry → search → click → conversion) reveals the largest drop-off step.

For a precise stage-to-stage funnel, **first run Step 1** to see the actual
event taxonomy, then use those exact names in a `MAX(CASE WHEN name = '...' )`
pivot like a standard Mixpanel-style funnel — but only with names that
actually exist in `event_types`.

### Why we don't hardcode

Earlier versions of this doc listed presumed event names. They returned 0
rows because the actual event names use a different convention. The
`event_types` lookup is the only safe source of truth. **Always query it
first.**

---

## Known data-quality issues

### ⚠ event_90day_mv is stale and incomplete (confirmed 2026-04-24)

The 90-day materialized view `prod.mixpanel_phoenix.event_90day_mv` is
currently broken in two ways:
- **Frozen**: latest `time` value in the view is `2025-04-01` — over a year
  out of date as of this writing.
- **Missing event families**: even ignoring the time staleness, no rows
  matching `LIKE '%care search%'` exist in the view at all, while the
  underlying `event` table has 686+ "Care Search Viewed" rows in the last
  90 days alone.

**Implication for Pulse:** never use `event_90day_mv` for product-feature
analysis. Always query the full `event` table with an explicit
`time >= DATEADD(DAY, -N, CURRENT_DATE())` predicate. Partition pruning on
`time` makes the full-table query reasonably fast despite the 39B-row size.

### ⚠ Session Complete under-reporting (reported 2026-04-23)

Jessie Gao reported that `Session Complete` events show only ~1% in
`prod.mixpanel_phoenix.event` vs. 90%+ in the Mixpanel UI directly. The sync
pipeline (Mixpanel → Fivetran → Databricks) is dropping these events for
reasons currently being investigated by Data Engineering.

**Implication for Pulse:** any question that derives a completion rate from
`Session Complete` is currently wrong by ~two orders of magnitude. Flag this
to the user when relevant events appear in a query.

> **Pulse behavior:** when generating SQL that uses `Session Complete`, the
> verifier pass should warn about this known issue. For now, consider using
> `Appointment Booked` as the terminal success event instead.

### ⚠ General sync reliability

Per Jennifer Burch: "DE has a lot of complaints about Mixpanel, it's my
understanding that not all events are syncing correctly." Per Vipin Yadav:
"Mixpanel issues are a common occurrence."

Cross-check any high-stakes answer against the Mixpanel UI directly.
`data-pipeline-monitor` runs an independent Mixpanel event-volume check for
this reason.

---

## Join patterns

### Join to HS user data

```sql
-- Map Mixpanel distinct_id to HH internal user
SELECT ...
FROM prod.mixpanel_phoenix.event e
LEFT JOIN prod.hs_accounts_customer.accounts_customer_user_accounts u
  ON e.user_id = u.id
WHERE ...
```

### Join to eligibility

```sql
-- Drop-off by eligibility status
SELECT ...
FROM prod.mixpanel_phoenix.event e
JOIN prod.hs_eligibility.eligibility_eligibility_responses er
  ON e.user_id = er.subscriber_id
WHERE ...
```

---

## PHI / privacy notes

- `distinct_id`, `$device_id`, `$ip`, `$user_agent`, `$browser` are treated as
  PHI-adjacent and skipped during sampling (configured in `schemas.yaml`).
- The `properties` JSON may contain arbitrary user-entered text (search
  queries, form inputs). Be cautious when aggregating or returning raw
  `properties` values — they can contain PHI.
- When returning sample query results to users, avoid echoing `distinct_id`
  or `user_id` without aggregation.

---

## Common questions and answer patterns

| Question | Pattern |
|---|---|
| "How many customers/users use Care Search?" | COUNT DISTINCT distinct_id from `event` (NOT `event_90day_mv`) with `time >= DATEADD(DAY, -90, CURRENT_DATE())`, filtered to event names from `event_types` matching `%care search%`/`%caresearch%` |
| "Where are users dropping off in Care Search?" | Discover events from `event_types` → pivot stage counts on `event`; see Step 3 SQL above |
| "What's the most common search query?" | First find the query event name in `event_types`, then `SELECT properties:query, COUNT(*) FROM event WHERE name = <discovered> AND time >= ...` |
| "Which providers get clicked the most?" | First find the click event name in `event_types`, then `SELECT properties:provider_id, COUNT(*) FROM event WHERE name = <discovered> AND time >= ...` |
| "Session duration for Care Search users" | Requires session-boundary events; ⚠ watch for the known Session Complete sync issue (1% vs 90%+) |

**Pattern:** every Mixpanel question starts with a discovery query against
`event_types` to find the relevant event names. Only then filter the
event tables. This pattern is robust to taxonomy changes and avoids the
silent-zero failure mode caused by hardcoded names that don't exist.

---

## Updating this doc

This topic doc is loaded into the KG build as additional context whenever
`mixpanel_phoenix` is built. To refresh:

1. Update this doc (accurate event names, refined funnel stages, new DQ issues)
2. Clear the KG cache: `rm -rf cache/mixpanel_phoenix`
3. Ask any Mixpanel-related question in Pulse — it'll trigger a rebuild using
   the updated doc

Maintainers: Scott Donchak (event taxonomy), Jennifer Burch (Mixpanel sync),
Jessie Gao (Phoenix analytics).
