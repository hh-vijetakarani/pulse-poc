# mixpanel_phoenix — topic doc

Context for Pulse's KG builder + SQL generator when answering questions against
`prod.mixpanel_phoenix`. This doc describes the event model, the Care Search
funnel, key identifier columns, and known data-quality issues. Loaded by the
KG builder as a parallel context source (alongside schema + samples).

---

## Event model

`prod.mixpanel_phoenix.event` is the primary fact table. Each row is one
Mixpanel event captured by the Phoenix (HingeSelect) client, synced via
Fivetran from Mixpanel to Databricks.

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

## Care Search funnel (manually curated)

The Care Search feature in Phoenix follows this typical user flow. Each stage
corresponds to one or more `event_name` values. When Pulse answers drop-off
questions, it should compute stage-to-stage conversion rates using these
events as funnel steps.

| Stage | Event name(s) (approx — verify) | What it means |
|---|---|---|
| 1. Entry | `Care Search Opened` | User opened the Care Search surface |
| 2. Query | `Care Search Query Entered` | User typed a search query |
| 3. Filter | `Care Search Filter Applied` | User narrowed results (specialty, location, etc.) |
| 4. Results viewed | `Care Search Results Viewed` | User scrolled the result list |
| 5. Result click | `Care Search Result Clicked` | User tapped a provider card |
| 6. Profile view | `Provider Profile Viewed` | User landed on a provider detail page |
| 7. Appointment intent | `Appointment Slot Viewed` | User saw appointment options |
| 8. Booking attempt | `Appointment Booking Started` | User started the booking flow |
| 9. Booking complete | `Appointment Booked` | User completed the booking |

### SQL pattern for drop-off analysis

For "where are users dropping off in Care Search," generate SQL similar to:

```sql
WITH funnel AS (
  SELECT
    distinct_id,
    MAX(CASE WHEN event_name = 'Care Search Opened' THEN 1 ELSE 0 END) AS stage_1,
    MAX(CASE WHEN event_name = 'Care Search Query Entered' THEN 1 ELSE 0 END) AS stage_2,
    MAX(CASE WHEN event_name = 'Care Search Filter Applied' THEN 1 ELSE 0 END) AS stage_3,
    MAX(CASE WHEN event_name = 'Care Search Results Viewed' THEN 1 ELSE 0 END) AS stage_4,
    MAX(CASE WHEN event_name = 'Care Search Result Clicked' THEN 1 ELSE 0 END) AS stage_5,
    MAX(CASE WHEN event_name = 'Provider Profile Viewed' THEN 1 ELSE 0 END) AS stage_6,
    MAX(CASE WHEN event_name = 'Appointment Slot Viewed' THEN 1 ELSE 0 END) AS stage_7,
    MAX(CASE WHEN event_name = 'Appointment Booking Started' THEN 1 ELSE 0 END) AS stage_8,
    MAX(CASE WHEN event_name = 'Appointment Booked' THEN 1 ELSE 0 END) AS stage_9
  FROM prod.mixpanel_phoenix.event
  WHERE time >= DATEADD(DAY, -30, CURRENT_DATE())
    AND event_name LIKE 'Care Search%' OR event_name IN ('Provider Profile Viewed', 'Appointment Slot Viewed', 'Appointment Booking Started', 'Appointment Booked')
  GROUP BY distinct_id
)
SELECT
  SUM(stage_1) AS opened,
  SUM(stage_2) AS typed_query,
  SUM(stage_3) AS applied_filter,
  SUM(stage_4) AS viewed_results,
  SUM(stage_5) AS clicked_result,
  SUM(stage_6) AS viewed_profile,
  SUM(stage_7) AS viewed_slots,
  SUM(stage_8) AS started_booking,
  SUM(stage_9) AS booked
FROM funnel;
```

The biggest drop-off is usually identifiable by the largest percentage gap
between sequential stages.

### Variant: session-scoped funnel

For "users who complete the funnel within one session," scope the MAX()
aggregation to `session_id` instead of `distinct_id`.

---

## Known data-quality issues

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
| "Where are users dropping off in Care Search?" | Stage-over-stage conversion (SQL above) |
| "How many users completed Care Search last week?" | COUNT DISTINCT distinct_id WHERE event_name = 'Appointment Booked' AND time >= last 7d |
| "What's the most common search query?" | SELECT properties.query, COUNT(*) FROM event WHERE event_name = 'Care Search Query Entered' |
| "Which providers get clicked the most?" | SELECT properties.provider_id, COUNT(*) WHERE event_name = 'Care Search Result Clicked' |
| "Session duration for Care Search users" | Requires session boundary events; watch for the known Session Complete bug |

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
