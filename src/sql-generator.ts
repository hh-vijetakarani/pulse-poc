import Anthropic from "@anthropic-ai/sdk";
import { findTable } from "./knowledge.js";
import { incrementValidatedUse } from "./learning.js";
import type {
  ClassificationResult,
  Correction,
  FleetKnowledgeGraph,
  GeneratedSQL,
  ValidatedQuery,
} from "./types.js";

const MODEL = "claude-sonnet-4-6";
const client = new Anthropic();

const SQL_SYSTEM_PROMPT = `You generate Databricks SQL queries from natural language across
potentially multiple schemas.

STRICT RULES:
1. Use ONLY the tables and columns listed below. NEVER invent columns or tables.
2. Each table's fully-qualified name is shown on its "Table:" line. Copy it exactly,
   including catalog and schema — do not swap catalogs or truncate.
3. When joining across schemas, both sides must use their OWN catalog.schema prefix.
   (e.g. \`JOIN prod.billing.invoices AS i ON i.merchant_id = m.id\`)
4. Enum values in SQL are STRING literals. Use ONLY the exact values listed in the
   "Enum context" section — never invent enum values.
5. SELECT statements ONLY. Never INSERT/UPDATE/DELETE/DROP/ALTER/CREATE.
6. Always apply the default filters listed for each table.
7. Apply all user corrections listed.
8. Add LIMIT 10000 unless the user asks for all rows or requests an aggregate.
9. Use Databricks SQL syntax: DATEADD, DATE_TRUNC, DATE_FORMAT (not SQLite/Postgres).
10. For JSON columns, use Databricks JSON path syntax:
    \`properties:merchant.name::string\` or \`get_json_object(properties, '$.merchant.name')\`.
11. Prefer aggregations over raw row dumps.
12. Alias all calculated columns with readable names (snake_case).

Respond with ONLY valid JSON — no prose, no markdown fences:

{
  "sql": "SELECT ...",
  "explanation": "one-sentence plain-English description",
  "tables_used": ["<schema_id>.<table>", ...],
  "schemas_used": ["<schema_id>", ...],
  "estimated_complexity": "low|medium|high"
}`;

export function buildFocusedContext(
  classification: ClassificationResult,
  fleet: FleetKnowledgeGraph,
): string {
  const parts: string[] = [];
  const refs = classification.relevant_tables.slice(0, 5);

  for (const ref of refs) {
    const t = findTable(fleet, ref.schema_id, ref.table);
    if (!t) continue;

    parts.push(`Table: ${t.catalog}.${t.schema}.${t.table_name}   [schema_id: ${t.schema_id}]`);
    parts.push(`Purpose: ${t.purpose}`);
    if (t.proto_message) parts.push(`Proto message: ${t.proto_message}`);
    if (t.dbt_model) parts.push(`dbt model: ${t.dbt_model}`);
    parts.push(`Row count: ${t.row_count.toLocaleString()}`);

    parts.push("Columns:");
    for (const col of t.all_columns) {
      const keyMeaning = t.key_columns.find((k) => k.column === col.column_name)?.meaning;
      const enumInfo = t.enum_columns.find((e) => e.column === col.column_name);
      const meaning = keyMeaning ?? col.comment ?? "";
      const enumPart = enumInfo
        ? ` [enum ${enumInfo.enum_name}: ${enumInfo.values.slice(0, 8).join(", ")}${enumInfo.values.length > 8 ? "..." : ""}]`
        : "";
      parts.push(`  ${col.column_name} (${col.data_type})${meaning ? " — " + meaning : ""}${enumPart}`);
    }

    if (t.join_keys.length) {
      parts.push("Join keys:");
      for (const j of t.join_keys) {
        parts.push(`  ${j.column} → ${j.joins_to}.${j.foreign_column}`);
      }
    }

    if (t.default_filters.length) {
      parts.push("Default filters (MUST apply):");
      for (const f of t.default_filters) {
        parts.push(`  ${f.column} ${f.condition}  (reason: ${f.reason})`);
      }
    }

    if (t.business_rules.length) {
      parts.push("Business rules:");
      for (const r of t.business_rules) parts.push(`  - ${r}`);
    }
    parts.push("");
  }

  if (classification.enum_context.length) {
    parts.push("Enum context (valid string values):");
    for (const e of classification.enum_context) {
      parts.push(`  [${e.schema_id}] ${e.column}: ${e.valid_values.slice(0, 10).join(", ")}`);
    }
    parts.push("");
  }

  return parts.join("\n");
}

interface RawSql {
  sql: string;
  explanation: string;
  tables_used: string[];
  schemas_used?: string[];
  estimated_complexity: "low" | "medium" | "high";
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
    throw new Error(`Could not parse SQL generator JSON. First 200 chars: ${cleaned.slice(0, 200)}`);
  }
}

function validateSelectOnly(sql: string): void {
  const upper = sql.trim().toUpperCase();
  if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
    throw new Error(`Generated SQL does not start with SELECT or WITH: ${sql.slice(0, 80)}...`);
  }
  const forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "MERGE", "GRANT", "REVOKE"];
  for (const kw of forbidden) {
    if (new RegExp(`\\b${kw}\\b`, "i").test(sql)) {
      throw new Error(`Generated SQL contains forbidden keyword: ${kw}`);
    }
  }
}

export async function generateSQL(
  question: string,
  classification: ClassificationResult,
  fleet: FleetKnowledgeGraph,
  corrections: Correction[],
  validated: ValidatedQuery[],
): Promise<GeneratedSQL> {
  if (classification.method === "validated_reuse" && classification.validated_query_id) {
    const v = validated.find((x) => x.id === classification.validated_query_id);
    if (v) {
      incrementValidatedUse(v.id);
      return {
        sql: v.sql,
        explanation: `Reused validated query: ${v.description}`,
        tables_used: v.tables_used,
        schemas_used: v.schemas_used,
        estimated_complexity: "low",
      };
    }
  }

  const tableCtx = buildFocusedContext(classification, fleet);
  const relevantSchemas = new Set(classification.relevant_tables.map((r) => r.schema_id));
  const relevantTableKeys = new Set(
    classification.relevant_tables.map((r) => `${r.schema_id}.${r.table}`),
  );

  const relevantCorrections = corrections
    .filter(
      (c) => relevantSchemas.has(c.schema_id) && (c.table === "*" || relevantTableKeys.has(`${c.schema_id}.${c.table}`)),
    )
    .map((c) => `- [${c.schema_id}.${c.table}] ${c.rule}`)
    .join("\n") || "None";

  const paramsBlock = Object.keys(classification.extracted_parameters).length
    ? `Extracted parameters to apply:\n${Object.entries(classification.extracted_parameters)
        .map(([k, v]) => `  ${k} = ${v}`)
        .join("\n")}`
    : "";

  const metricBlock = classification.matched_metric
    ? `Suggested metric pattern:\n  ${classification.matched_metric.name}: ${classification.matched_metric.sql_pattern}`
    : "";

  const userContent = [
    `Question: ${question}`,
    "",
    tableCtx,
    `Corrections:\n${relevantCorrections}`,
    paramsBlock,
    metricBlock,
  ]
    .filter(Boolean)
    .join("\n\n");

  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 4096,
    system: SQL_SYSTEM_PROMPT,
    messages: [{ role: "user", content: userContent }],
  });

  const textBlock = response.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") {
    const blockSummary = response.content.map((b) => b.type).join(", ") || "(none)";
    throw new Error(
      `SQL generator returned no text block. Stop reason: ${response.stop_reason}. Blocks: ${blockSummary}`,
    );
  }

  const raw = parseJson<RawSql>(textBlock.text);
  validateSelectOnly(raw.sql);

  return {
    sql: raw.sql.trim(),
    explanation: raw.explanation,
    tables_used: raw.tables_used ?? [],
    schemas_used: raw.schemas_used ?? [...relevantSchemas],
    estimated_complexity: raw.estimated_complexity ?? "medium",
  };
}
