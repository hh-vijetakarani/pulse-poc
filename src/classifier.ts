import Anthropic from "@anthropic-ai/sdk";
import { findTable } from "./knowledge.js";
import type {
  ClassificationResult,
  Correction,
  DerivedMetric,
  FleetKnowledgeGraph,
  ValidatedQuery,
} from "./types.js";

const MODEL = "claude-sonnet-4-6";
const client = new Anthropic();

const STOPWORDS = new Set([
  "what", "whats", "is", "are", "the", "a", "an", "of", "for", "to", "in", "on",
  "at", "by", "with", "and", "or", "do", "does", "did", "show", "me", "give",
  "list", "get", "find", "how", "many", "much", "which", "that", "this", "these",
  "those", "can", "you", "please", "i", "we", "our",
]);

function keywordize(text: string): Set<string> {
  const words = text
    .toLowerCase()
    .replace(/[^\w\s-]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 1 && !STOPWORDS.has(w));
  return new Set(words);
}

function keywordOverlap(a: Set<string>, b: Set<string>): number {
  if (a.size === 0 || b.size === 0) return 0;
  let shared = 0;
  for (const w of a) if (b.has(w)) shared++;
  const union = new Set([...a, ...b]);
  return shared / union.size;
}

export function findValidatedMatch(
  question: string,
  validated: ValidatedQuery[],
  activeSchemaIds: string[],
): ValidatedQuery | null {
  const active = new Set(activeSchemaIds);
  const qWords = keywordize(question);
  let best: { match: ValidatedQuery; score: number } | null = null;
  for (const v of validated) {
    // Only consider validated queries whose schemas are all in the active set
    if (!v.schemas_used.every((s) => active.has(s))) continue;
    const vWords = keywordize(v.question);
    const score = keywordOverlap(qWords, vWords);
    if (score > 0.7 && (!best || score > best.score)) {
      best = { match: v, score };
    }
  }
  return best?.match ?? null;
}

export function extractParameters(
  question: string,
  fleet: FleetKnowledgeGraph,
  activeSchemaIds: string[],
): Record<string, string> {
  const params: Record<string, string> = {};
  const lower = question.toLowerCase();

  const dateMap: [RegExp, string][] = [
    [/\b(today)\b/, "today"],
    [/\b(yesterday)\b/, "yesterday"],
    [/\b(this week|last 7 days|last week|past week)\b/, "last_7_days"],
    [/\b(this month|last 30 days|last month|past month)\b/, "last_30_days"],
    [/\b(last 90 days|last 3 months|last quarter|past quarter)\b/, "last_90_days"],
    [/\b(this quarter|current quarter)\b/, "current_quarter"],
    [/\b(ytd|year to date|this year)\b/, "ytd"],
    [/\b(all time|ever|total|overall)\b/, "all_time"],
  ];
  for (const [re, label] of dateMap) {
    if (re.test(lower)) {
      params.date_range = label;
      break;
    }
  }

  const activeSet = new Set(activeSchemaIds);
  for (const kg of fleet.schemas) {
    if (!activeSet.has(kg.schema_id)) continue;
    for (const table of kg.tables ?? []) {
      for (const ec of table.enum_columns ?? []) {
        for (const val of ec.values ?? []) {
          if (!isPlausibleEnumMatch(val, lower)) continue;
          params[`${kg.schema_id}.${ec.column}`] = val;
        }
      }
    }
  }

  return params;
}

/**
 * Decide whether an enum value looks like it was actually mentioned in the
 * question. Short values like "r" or "W" match inside ordinary words
 * ("records", "how"), so we require:
 *   - values >= 3 characters: word-boundary match
 *   - values < 3 characters: skip entirely (too noisy to be useful)
 */
function isPlausibleEnumMatch(enumValue: string, lowerQuestion: string): boolean {
  const v = enumValue.toLowerCase();
  if (v.length < 3) return false;
  const escaped = v.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(^|[^a-z0-9_])${escaped}([^a-z0-9_]|$)`, "i").test(lowerQuestion);
}

function buildCompactFleetSummary(
  fleet: FleetKnowledgeGraph,
  selectedSchemaIds: string[],
): string {
  const selected = new Set(selectedSchemaIds);
  const parts: string[] = [];
  parts.push(`Active schemas: ${selectedSchemaIds.join(", ")}`);
  parts.push("");

  for (const kg of fleet.schemas) {
    if (!selected.has(kg.schema_id)) continue;
    const tables = kg.tables ?? [];
    parts.push(`== [${kg.schema_id}] ${kg.catalog}.${kg.schema} (${tables.length} tables) ==`);
    if (kg.notes_excerpt) {
      // Short hint only — enough to nudge table selection (e.g. include
      // event_types as a lookup for Care Search). Full doc goes to SQL gen.
      const hint = kg.notes_excerpt.slice(0, 800).replace(/\s+/g, " ").trim();
      parts.push(`  Curated guidance: ${hint}`);
    }
    for (const t of tables) {
      const joinTargets = (t.join_keys ?? []).map((j) => j.joins_to).slice(0, 4).join(",");
      const keyCols = (t.key_columns ?? []).slice(0, 6).map((k) => k.column).join(",");
      const joinPart = joinTargets ? ` | joins:${joinTargets}` : "";
      const enumCols = t.enum_columns ?? [];
      const enumPart = enumCols.length
        ? ` | enums:${enumCols.map((e) => e.column).join(",")}`
        : "";
      parts.push(
        `  ${t.table_name} [${t.domain}, ${t.row_count.toLocaleString()} rows]: ${t.purpose} | cols:${keyCols}${joinPart}${enumPart}`,
      );
    }
    const metrics = kg.derivable_metrics ?? [];
    if (metrics.length) {
      parts.push("  Derivable metrics:");
      for (const m of metrics) {
        parts.push(
          `    ${m.name}: ${m.description} (tables: ${(m.tables_needed ?? []).join(",")})`,
        );
      }
    }
    if (kg.funnel && Array.isArray(kg.funnel.stages) && kg.funnel.stages.length > 0) {
      parts.push(`  Funnel: ${kg.funnel.stages.map((s) => s.name).join(" → ")}`);
    }
    parts.push("");
  }

  return parts.join("\n");
}

const CLASSIFIER_SYSTEM_PROMPT = `You are a query classifier for a fleet of Databricks schemas.
Given a natural language question and a summary of which tables are available in each schema,
determine which tables are needed to answer the question.

Respond with ONLY valid JSON — no prose, no markdown fences — matching this structure:

{
  "relevant_tables": [{"table": "<schema_id>.<table_name>", "purpose": "why needed", "relevance_score": 0.0}],
  "suggested_joins": [{"from": "<schema_id>.<from_table>", "to": "<schema_id>.<to_table>", "on": ""}],
  "suggested_filters": [{"table": "<schema_id>.<table_name>", "column": "", "condition": "", "reason": ""}],
  "matched_metric": null,
  "extracted_parameters": {},
  "confidence": 0.0,
  "answerable": true
}

Rules:
- The "table" field MUST be of the form <schema_id>.<table_name> exactly as shown in the
  headings above each schema's tables (e.g., "hs_graph.graph_nodes"). No catalog prefix.
- "from" and "to" in suggested_joins use the same <schema_id>.<table> form.
- confidence 0.8-1.0: question clearly maps to available tables
- confidence 0.5-0.8: answerable but may need assumptions
- confidence <0.5: probably not answerable with this data
- If answerable is false, set relevant_tables to []
- Questions may legitimately span multiple schemas; include tables from every relevant one.
- Include default filters from the summary for every relevant table.`;

interface RawClassification {
  relevant_tables: { table: string; purpose: string; relevance_score: number }[];
  suggested_joins: { from: string; to: string; on: string }[];
  suggested_filters: { table?: string; column: string; condition: string; reason: string }[];
  matched_metric: { name: string; sql_pattern?: string } | null;
  extracted_parameters: Record<string, string>;
  confidence: number;
  answerable: boolean;
}

function parseJson<T>(raw: string): T {
  const cleaned = raw
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/```\s*$/i, "")
    .trim();
  try {
    return JSON.parse(cleaned) as T;
  } catch {
    const firstBrace = cleaned.indexOf("{");
    const lastBrace = cleaned.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      return JSON.parse(cleaned.slice(firstBrace, lastBrace + 1)) as T;
    }
    throw new Error(`Could not parse classifier JSON. First 200 chars: ${cleaned.slice(0, 200)}`);
  }
}

function splitComposite(composite: string, activeSchemaIds: string[]): {
  schema_id: string;
  table: string;
} | null {
  const parts = composite.split(".");
  if (parts.length === 2) return { schema_id: parts[0]!, table: parts[1]! };
  // Model may have omitted schema prefix — if only one schema is active, infer.
  if (parts.length === 1 && activeSchemaIds.length === 1) {
    return { schema_id: activeSchemaIds[0]!, table: parts[0]! };
  }
  // catalog.schema.table or similar — strip down to last segment and guess schema
  const last = parts[parts.length - 1]!;
  if (activeSchemaIds.length === 1) {
    return { schema_id: activeSchemaIds[0]!, table: last };
  }
  return null;
}

export async function classifyQuestion(
  question: string,
  fleet: FleetKnowledgeGraph,
  scopeSchemaIds: string[],
  corrections: Correction[],
  validated: ValidatedQuery[],
): Promise<ClassificationResult> {
  const match = findValidatedMatch(question, validated, scopeSchemaIds);
  if (match) {
    const extracted = extractParameters(question, fleet, scopeSchemaIds);
    const tables = match.tables_used.map((composite) => {
      const split = splitComposite(composite, scopeSchemaIds);
      return {
        schema_id: split?.schema_id ?? scopeSchemaIds[0] ?? "",
        table: split?.table ?? composite,
        purpose: `Reused from validated query: ${match.description}`,
        relevance_score: 1.0,
      };
    });
    const enumCtx = enumContextFor(tables, fleet);
    return {
      relevant_tables: tables,
      suggested_joins: [],
      suggested_filters: [],
      enum_context: enumCtx,
      matched_metric: null,
      extracted_parameters: extracted,
      confidence: 1.0,
      method: "validated_reuse",
      answerable: true,
      validated_query_id: match.id,
      routed_schema_ids: match.schemas_used,
    };
  }

  const selectedSchemaIds = scopeSchemaIds;
  const summary = buildCompactFleetSummary(fleet, selectedSchemaIds);

  const activeTableRefs = new Set<string>();
  for (const kg of fleet.schemas) {
    if (!selectedSchemaIds.includes(kg.schema_id)) continue;
    for (const t of kg.tables) {
      activeTableRefs.add(`${kg.schema_id}.${t.table_name}`);
    }
  }

  const relevantCorrections = corrections.filter((c) =>
    selectedSchemaIds.includes(c.schema_id),
  );
  const correctionsBlock = relevantCorrections.length
    ? relevantCorrections.map((c) => `- [${c.schema_id}.${c.table}] ${c.rule}`).join("\n")
    : "None";

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 2048,
    system: [
      { type: "text", text: CLASSIFIER_SYSTEM_PROMPT },
      {
        type: "text",
        text: `Fleet summary:\n${summary}`,
        cache_control: { type: "ephemeral" },
      },
    ],
    messages: [
      {
        role: "user",
        content: `Question: ${question}\n\nCorrections to apply:\n${correctionsBlock}`,
      },
    ],
  });

  const textBlock = response.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") {
    throw new Error("Classifier returned no text block");
  }

  const raw = parseJson<RawClassification>(textBlock.text);

  const relevant_tables = (raw.relevant_tables ?? [])
    .map((r) => {
      const split = splitComposite(r.table, selectedSchemaIds);
      if (!split) return null;
      if (!activeTableRefs.has(`${split.schema_id}.${split.table}`)) return null;
      return {
        schema_id: split.schema_id,
        table: split.table,
        purpose: r.purpose,
        relevance_score: r.relevance_score,
      };
    })
    .filter((r): r is NonNullable<typeof r> => r !== null);

  const suggested_joins = (raw.suggested_joins ?? [])
    .map((j) => {
      const fromSplit = splitComposite(j.from, selectedSchemaIds);
      const toSplit = splitComposite(j.to, selectedSchemaIds);
      if (!fromSplit || !toSplit) return null;
      return {
        from_schema_id: fromSplit.schema_id,
        from: fromSplit.table,
        to_schema_id: toSplit.schema_id,
        to: toSplit.table,
        on: j.on,
      };
    })
    .filter((j): j is NonNullable<typeof j> => j !== null);

  const suggested_filters = (raw.suggested_filters ?? []).map((f) => {
    const split = f.table ? splitComposite(f.table, selectedSchemaIds) : null;
    return {
      schema_id: split?.schema_id ?? (selectedSchemaIds[0] ?? ""),
      column: f.column,
      condition: f.condition,
      reason: f.reason,
    };
  });

  if (!raw.answerable) {
    return {
      relevant_tables: [],
      suggested_joins: [],
      suggested_filters: [],
      enum_context: [],
      matched_metric: null,
      extracted_parameters: raw.extracted_parameters ?? {},
      confidence: raw.confidence ?? 0,
      method: "no_match",
      answerable: false,
      routed_schema_ids: selectedSchemaIds,
    };
  }

  const enumCtx = enumContextFor(relevant_tables, fleet);

  let matchedMetric: DerivedMetric | null = null;
  if (raw.matched_metric) {
    for (const kg of fleet.schemas) {
      const m = kg.derivable_metrics.find((x) => x.name === raw.matched_metric!.name);
      if (m) {
        matchedMetric = m;
        break;
      }
    }
  }

  const extracted = {
    ...extractParameters(question, fleet, selectedSchemaIds),
    ...(raw.extracted_parameters ?? {}),
  };

  return {
    relevant_tables,
    suggested_joins,
    suggested_filters,
    enum_context: enumCtx,
    matched_metric: matchedMetric,
    extracted_parameters: extracted,
    confidence: raw.confidence ?? 0.5,
    method: "claude",
    answerable: true,
    routed_schema_ids: selectedSchemaIds,
  };
}

function enumContextFor(
  tables: { schema_id: string; table: string }[],
  fleet: FleetKnowledgeGraph,
): { schema_id: string; column: string; valid_values: string[] }[] {
  const out: { schema_id: string; column: string; valid_values: string[] }[] = [];
  for (const ref of tables) {
    const t = findTable(fleet, ref.schema_id, ref.table);
    if (!t) continue;
    for (const ec of t.enum_columns ?? []) {
      out.push({
        schema_id: ref.schema_id,
        column: `${ref.table}.${ec.column}`,
        valid_values: ec.values ?? [],
      });
    }
  }
  return out;
}
