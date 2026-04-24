import Anthropic from "@anthropic-ai/sdk";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import type { SchemaConfig } from "./config.js";
import { formatDbtForClaude, matchDbtToTables } from "./dbt-parser.js";
import { lintKg, normalizeKg } from "./kg-validator.js";
import { formatProtoForClaude } from "./proto-parser.js";

function loadNotesDoc(cfg: SchemaConfig): string | null {
  if (!cfg.notes_doc) return null;
  const path = resolve(cfg.notes_doc);
  if (!existsSync(path)) return null;
  try {
    // 8K is enough for the full curated docs we have today (~9KB max) while
    // keeping the SQL-gen prompt under ~2K tokens of guidance.
    return readFileSync(path, "utf-8").slice(0, 8000);
  } catch {
    return null;
  }
}
import type {
  ColumnInfo,
  DbtContext,
  FleetKnowledgeGraph,
  KnowledgeGraph,
  ProtoContext,
  Relationship,
  TableKnowledge,
  TableSample,
  TableSchema,
  TagInfo,
} from "./types.js";

const MODEL = "claude-sonnet-4-6";
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
// Schemas with more tables than SHARD_THRESHOLD are split into mini-KGs and
// merged. Set below the point where output exceeds max_tokens (~35K tokens of
// KG JSON output for Sonnet 4.6).
const SHARD_THRESHOLD = 30;
const SHARD_SIZE = 18;
// Max_tokens budget for a single KG Claude call. Sonnet 4.6 streaming ceiling
// is 64K; 32K is comfortable for up to ~50 tables of KG JSON output.
const KG_MAX_TOKENS = 32000;

const client = new Anthropic();

function schemaCacheDir(schemaId: string): string {
  return resolve("cache", schemaId);
}

function ensureDir(path: string): void {
  if (!existsSync(path)) mkdirSync(path, { recursive: true });
}

function isFresh(path: string): boolean {
  if (!existsSync(path)) return false;
  return Date.now() - statSync(path).mtimeMs < CACHE_TTL_MS;
}

export interface SchemaInputs {
  cfg: SchemaConfig;
  schema: TableSchema[];
  samples: TableSample[];
  tags: TagInfo[];
  proto: ProtoContext;
  dbt: DbtContext;
}

export async function buildFleet(
  inputs: SchemaInputs[],
  onProgress?: (msg: string) => void,
): Promise<FleetKnowledgeGraph> {
  const kgs = await Promise.all(
    inputs.map((input) => buildSchemaKg(input, onProgress)),
  );
  return {
    generated_at: new Date().toISOString(),
    schemas: kgs,
  };
}

async function buildSchemaKg(
  input: SchemaInputs,
  onProgress?: (msg: string) => void,
): Promise<KnowledgeGraph> {
  const { cfg, schema, samples, tags, proto, dbt } = input;
  const kgPath = resolve(schemaCacheDir(cfg.id), "knowledge-graph.json");

  if (isFresh(kgPath)) {
    try {
      const cached = JSON.parse(readFileSync(kgPath, "utf-8")) as KnowledgeGraph;
      // Run cached KGs through the same normalization path — protects against
      // stale cache shapes from earlier builds that predate the validator.
      const { kg: normalized } = normalizeKg(cached);
      // Reload the notes doc fresh on every hydration so edits to the topic
      // doc take effect on restart without forcing a full KG rebuild.
      normalized.notes_excerpt = loadNotesDoc(cfg) ?? undefined;
      return normalized;
    } catch {
      // rebuild
    }
  }

  if (dbt.models.length > 0) matchDbtToTables(dbt, schema);

  onProgress?.(
    `[${cfg.id}] Building knowledge graph (${schema.length} tables${proto.messages.length ? ", proto" : ""}${dbt.models.length ? ", dbt" : ""})...`,
  );

  let raw: RawKg;
  if (schema.length > SHARD_THRESHOLD) {
    onProgress?.(`[${cfg.id}]   Sharding into chunks of ${SHARD_SIZE}...`);
    raw = await shardedKgBuild(cfg, schema, samples, tags, proto, dbt, onProgress);
  } else {
    const context = buildKgContext(cfg, schema, samples, tags, proto, dbt);
    raw = await callClaudeForKg(context);
  }

  const assembled = assembleKnowledgeGraph(cfg, raw, schema, samples, proto, dbt);
  const { kg, warnings } = normalizeKg(assembled);
  for (const w of warnings) onProgress?.(`[${cfg.id}] ⚠ normalize: ${w}`);
  for (const note of lintKg(kg)) onProgress?.(`[${cfg.id}] lint: ${note}`);

  ensureDir(schemaCacheDir(cfg.id));
  writeFileSync(kgPath, JSON.stringify(kg, null, 2));

  // Attach AFTER persisting the cache file, so the doc isn't redundantly
  // duplicated on disk and edits apply on every restart.
  kg.notes_excerpt = loadNotesDoc(cfg) ?? undefined;

  onProgress?.(`[${cfg.id}] Generating seed context briefing...`);
  try {
    const seed = await generateSeedContext(kg);
    writeFileSync(resolve(schemaCacheDir(cfg.id), "seed-context.md"), seed);
  } catch (err) {
    onProgress?.(
      `[${cfg.id}]   warn: seed context generation failed: ${(err as Error).message.slice(0, 120)}`,
    );
  }

  return kg;
}

async function shardedKgBuild(
  cfg: SchemaConfig,
  schema: TableSchema[],
  samples: TableSample[],
  tags: TagInfo[],
  proto: ProtoContext,
  dbt: DbtContext,
  onProgress?: (msg: string) => void,
): Promise<RawKg> {
  const shards: TableSchema[][] = [];
  for (let i = 0; i < schema.length; i += SHARD_SIZE) {
    shards.push(schema.slice(i, i + SHARD_SIZE));
  }

  const results = await Promise.all(
    shards.map(async (shardTables, idx) => {
      onProgress?.(`[${cfg.id}]   Shard ${idx + 1}/${shards.length} (${shardTables.length} tables)`);
      const shardNames = new Set(shardTables.map((t) => t.table_name));
      const shardSamples = samples.filter((s) => shardNames.has(s.table_name));
      const shardTags = tags.filter((t) => shardNames.has(t.table_name));
      const context = buildKgContext(cfg, shardTables, shardSamples, shardTags, proto, dbt);
      return callClaudeForKg(context);
    }),
  );

  // Merge shards
  const merged: RawKg = {
    tables: [],
    relationships: [],
    funnel: null,
    derivable_metrics: [],
    sample_questions: [],
  };
  const seenTables = new Set<string>();
  const seenMetrics = new Set<string>();
  for (const r of results) {
    for (const t of r.tables) {
      if (!seenTables.has(t.table_name)) {
        seenTables.add(t.table_name);
        merged.tables.push(t);
      }
    }
    merged.relationships.push(...r.relationships);
    for (const m of r.derivable_metrics) {
      if (!seenMetrics.has(m.name)) {
        seenMetrics.add(m.name);
        merged.derivable_metrics.push(m);
      }
    }
    merged.sample_questions.push(...r.sample_questions);
  }
  merged.sample_questions = merged.sample_questions.slice(0, 20);
  return merged;
}

function buildKgContext(
  cfg: SchemaConfig,
  schema: TableSchema[],
  samples: TableSample[],
  tags: TagInfo[],
  proto: ProtoContext,
  dbt: DbtContext,
): string {
  const parts: string[] = [];

  if (proto.messages.length || proto.enums.length) {
    parts.push(formatProtoForClaude(proto));
    parts.push("");
  }

  if (dbt.models.length) {
    parts.push(formatDbtForClaude(dbt));
    parts.push("");
  }

  const notes = loadNotesDoc(cfg);
  if (notes) {
    parts.push(`=== CURATED TOPIC DOC for ${cfg.id} (hand-maintained domain context) ===`);
    parts.push(notes);
    parts.push("");
  }

  parts.push(`=== DATABRICKS SCHEMA: ${cfg.catalog}.${cfg.schema} (ground truth) ===`);
  parts.push("");

  const sampleMap = new Map(samples.map((s) => [s.table_name, s]));
  const tagsByTable = new Map<string, TagInfo[]>();
  for (const t of tags) {
    if (!tagsByTable.has(t.table_name)) tagsByTable.set(t.table_name, []);
    tagsByTable.get(t.table_name)!.push(t);
  }

  for (const tbl of schema) {
    const sample = sampleMap.get(tbl.table_name);
    const tblTags = (tagsByTable.get(tbl.table_name) ?? []).filter(
      (t) => t.object_type === "table",
    );
    const colTagsByCol = new Map<string, TagInfo[]>();
    for (const t of tagsByTable.get(tbl.table_name) ?? []) {
      if (t.object_type === "column" && t.column_name) {
        if (!colTagsByCol.has(t.column_name)) colTagsByCol.set(t.column_name, []);
        colTagsByCol.get(t.column_name)!.push(t);
      }
    }

    const header = [
      `Table: ${tbl.table_name}`,
      sample ? `(${sample.row_count.toLocaleString()} rows)` : "",
      tbl.table_comment ? `— ${tbl.table_comment}` : "",
    ]
      .filter(Boolean)
      .join(" ");
    parts.push(header);

    if (tblTags.length) {
      parts.push(`  Tags: ${tblTags.map((t) => `${t.tag_name}=${t.tag_value}`).join(", ")}`);
    }

    parts.push("  Columns:");
    const shownCols = tbl.columns.slice(0, 25);
    for (const col of shownCols) {
      const colTagStr = (colTagsByCol.get(col.column_name) ?? [])
        .map((t) => `${t.tag_name}=${t.tag_value}`)
        .join(",");
      const tagPart = colTagStr ? ` [${colTagStr}]` : "";
      const cmtPart = col.comment ? ` — ${col.comment}` : "";
      parts.push(`    ${col.column_name} (${col.data_type})${tagPart}${cmtPart}`);
    }
    if (tbl.columns.length > 25) {
      parts.push(`    ...and ${tbl.columns.length - 25} more columns`);
    }

    if (sample && Object.keys(sample.distinct_values).length > 0) {
      parts.push("  Distinct values (first 10 per column):");
      for (const [col, vals] of Object.entries(sample.distinct_values)) {
        const shown = vals.slice(0, 10).map((v) => JSON.stringify(v)).join(", ");
        parts.push(`    ${col}: [${shown}]`);
      }
    }

    if (sample && sample.sample_rows.length > 0) {
      parts.push("  Sample rows:");
      for (const row of sample.sample_rows.slice(0, 2)) {
        parts.push(`    ${JSON.stringify(row).slice(0, 400)}`);
      }
    }

    if (sample && sample.skipped_columns.length > 0) {
      parts.push(`  (PHI-skipped: ${sample.skipped_columns.join(", ")})`);
    }
    parts.push("");
  }

  return parts.join("\n");
}

const KG_SYSTEM_PROMPT = `You are a senior data analyst building a knowledge graph from a Databricks schema.

You may have up to THREE sources of information, in order of reliability:

1. PROTOBUF DEFINITIONS — source-of-truth data contracts. Field types, enum values, and
   message relationships are EXACT. Enum values marked with enum_format = "json" are stored
   as strings in SQL (e.g. \`WHERE type = 'ENUM_VALUE_NAME'\`), not integer codes.

2. DBT MODEL DOCUMENTATION — human-curated model/column descriptions, tests, and meta.
   Trust these for analytical/business meaning. Use column.tests (e.g. "unique", "not_null")
   as hints for join keys and default filters.

3. UNITY CATALOG METADATA — table/column comments and tags. Use if present.

4. SAMPLE DATA — actual values. Use to verify definitions match reality, discover filtering
   patterns, and find category values.

Build a knowledge graph with per-table business purpose, key columns, join keys, default
filters, enum columns (with valid string values), business rules, and for the schema:
relationships with cardinality, any funnel/workflow, derivable metrics, and
15 sample questions this data can answer.

Respond with ONLY valid JSON matching this exact structure. No prose before or after.
Do NOT wrap in markdown code fences.

{
  "tables": [{
    "table_name": "",
    "purpose": "",
    "domain": "providers|locations|specialties|procedures|members|eligibility|visits|billing|events|reference|analytics|knowledge_graph|metadata|other",
    "key_columns": [{"column": "", "meaning": ""}],
    "join_keys": [{"column": "", "joins_to": "", "foreign_column": ""}],
    "default_filters": [{"column": "", "condition": "", "reason": ""}],
    "enum_columns": [{"column": "", "enum_name": "", "values": [""]}],
    "business_rules": [""],
    "proto_message": null,
    "dbt_model": null
  }],
  "relationships": [{"from_table": "", "to_table": "", "join_key": "", "type": "one_to_many|many_to_one|many_to_many|one_to_one", "source": "proto|inferred|dbt"}],
  "funnel": null,
  "derivable_metrics": [{"name": "", "description": "", "tables_needed": [""], "sql_pattern": ""}],
  "sample_questions": [""]
}`;

interface RawKg {
  tables: Array<{
    table_name: string;
    purpose: string;
    domain: string;
    key_columns: { column: string; meaning: string }[];
    join_keys: { column: string; joins_to: string; foreign_column: string }[];
    default_filters: { column: string; condition: string; reason: string }[];
    enum_columns: { column: string; enum_name: string; values: string[] }[];
    business_rules: string[];
    proto_message: string | null;
    dbt_model: string | null;
  }>;
  relationships: Array<{
    from_table: string;
    to_table: string;
    join_key: string;
    type: "one_to_many" | "many_to_one" | "many_to_many" | "one_to_one";
    source: "proto" | "inferred" | "dbt";
  }>;
  funnel: {
    name: string;
    stages: { name: string; table: string; filter: string; order: number }[];
  } | null;
  derivable_metrics: Array<{
    name: string;
    description: string;
    tables_needed: string[];
    sql_pattern: string;
  }>;
  sample_questions: string[];
}

async function callClaudeForKg(context: string): Promise<RawKg> {
  const stream = client.messages.stream({
    model: MODEL,
    max_tokens: KG_MAX_TOKENS,
    system: KG_SYSTEM_PROMPT,
    messages: [{ role: "user", content: context }],
  });

  const message = await stream.finalMessage();
  const textBlock = message.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") {
    const blockSummary = message.content.map((b) => b.type).join(", ") || "(none)";
    throw new Error(
      `Claude returned no text block for knowledge graph. Stop reason: ${message.stop_reason}. Blocks: ${blockSummary}`,
    );
  }
  if (message.stop_reason === "max_tokens") {
    throw new Error(
      `KG generation hit max_tokens (${KG_MAX_TOKENS}). Lower SHARD_THRESHOLD or raise the budget.`,
    );
  }
  return parseJson<RawKg>(textBlock.text);
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
    throw new Error(`Could not parse JSON from Claude response. First 200 chars: ${cleaned.slice(0, 200)}`);
  }
}

function assembleKnowledgeGraph(
  cfg: SchemaConfig,
  raw: RawKg,
  schema: TableSchema[],
  samples: TableSample[],
  proto: ProtoContext,
  dbt: DbtContext,
): KnowledgeGraph {
  const schemaByName = new Map(schema.map((t) => [t.table_name, t]));
  const rowCountByName = new Map(samples.map((s) => [s.table_name, s.row_count]));

  const tables: TableKnowledge[] = raw.tables.map((t) => {
    const schemaEntry = schemaByName.get(t.table_name);
    const allColumns: ColumnInfo[] = schemaEntry?.columns ?? [];
    return {
      schema_id: cfg.id,
      catalog: cfg.catalog,
      schema: cfg.schema,
      table_name: t.table_name,
      purpose: t.purpose,
      domain: t.domain,
      key_columns: t.key_columns ?? [],
      join_keys: t.join_keys ?? [],
      default_filters: t.default_filters ?? [],
      enum_columns: t.enum_columns ?? [],
      business_rules: t.business_rules ?? [],
      row_count: rowCountByName.get(t.table_name) ?? 0,
      all_columns: allColumns,
      proto_message: t.proto_message ?? undefined,
      dbt_model: t.dbt_model ?? undefined,
    };
  });

  const relationships: Relationship[] = (raw.relationships ?? []).map((r) => ({
    schema_id: cfg.id,
    from_table: r.from_table,
    to_table: r.to_table,
    join_key: r.join_key,
    type: r.type,
    source: r.source,
  }));

  return {
    schema_id: cfg.id,
    catalog: cfg.catalog,
    schema: cfg.schema,
    generated_at: new Date().toISOString(),
    tables,
    relationships,
    funnel: raw.funnel
      ? { schema_id: cfg.id, name: raw.funnel.name, stages: raw.funnel.stages }
      : null,
    derivable_metrics: (raw.derivable_metrics ?? []).map((m) => ({
      schema_id: cfg.id,
      name: m.name,
      description: m.description,
      tables_needed: m.tables_needed,
      sql_pattern: m.sql_pattern,
    })),
    sample_questions: raw.sample_questions ?? [],
    proto_enriched: proto.messages.length > 0,
    dbt_enriched: dbt.models.length > 0,
  };
}

const SEED_CONTEXT_SYSTEM_PROMPT = `You are briefing a new analyst on their first day. Given a knowledge graph,
write a 1-page markdown briefing covering:

1. What this data represents (domain, primary entities)
2. Key relationships and how entities connect
3. Any workflow or funnel
4. Critical default filters and why they exist
5. Known gotchas — data quirks, polymorphic columns, enum stringification, etc.

Be specific and concrete. Use the table and column names. Target 400-600 words.
Output pure markdown — no preamble, no closing remarks.`;

async function generateSeedContext(kg: KnowledgeGraph): Promise<string> {
  const summary = summarizeKgForSeed(kg);
  const response = await client.messages.create({
    model: MODEL,
    max_tokens: 2048,
    system: SEED_CONTEXT_SYSTEM_PROMPT,
    messages: [{ role: "user", content: summary }],
  });

  const textBlock = response.content.find((b) => b.type === "text");
  if (!textBlock || textBlock.type !== "text") return "";
  return textBlock.text.trim();
}

function summarizeKgForSeed(kg: KnowledgeGraph): string {
  const parts: string[] = [];
  parts.push(
    `Schema: ${kg.catalog}.${kg.schema} — ${kg.tables.length} tables, proto-enriched: ${kg.proto_enriched}, dbt-enriched: ${kg.dbt_enriched}`,
  );
  parts.push("");
  parts.push("Tables:");
  for (const t of kg.tables) {
    parts.push(
      `- ${t.table_name} (${t.domain}, ${t.row_count.toLocaleString()} rows): ${t.purpose}`,
    );
    if (t.key_columns.length) {
      const kc = t.key_columns.slice(0, 4).map((k) => `${k.column} (${k.meaning})`).join("; ");
      parts.push(`    key columns: ${kc}`);
    }
    if (t.enum_columns.length) {
      parts.push(`    enums: ${t.enum_columns.map((e) => `${e.column}=${e.enum_name}`).join(", ")}`);
    }
    if (t.default_filters.length) {
      parts.push(`    default filters: ${t.default_filters.map((f) => `${f.column} ${f.condition}`).join("; ")}`);
    }
  }
  parts.push("");
  parts.push("Relationships:");
  for (const r of kg.relationships.slice(0, 30)) {
    parts.push(`  ${r.from_table} --[${r.join_key}]--> ${r.to_table} (${r.type}, ${r.source})`);
  }
  if (kg.funnel) {
    parts.push("");
    parts.push(`Funnel: ${kg.funnel.name}`);
    for (const s of kg.funnel.stages) {
      parts.push(`  ${s.order}. ${s.name} (${s.table}) ${s.filter}`);
    }
  }
  if (kg.derivable_metrics.length) {
    parts.push("");
    parts.push("Derivable metrics:");
    for (const m of kg.derivable_metrics.slice(0, 15)) {
      parts.push(`  - ${m.name}: ${m.description} (${m.tables_needed.join(", ")})`);
    }
  }
  return parts.join("\n");
}

export function loadSeedContext(schemaId: string): string | null {
  const path = resolve(schemaCacheDir(schemaId), "seed-context.md");
  if (!existsSync(path)) return null;
  try {
    return readFileSync(path, "utf-8");
  } catch {
    return null;
  }
}

// Convenience flatteners for downstream code that wants to see all tables/metrics/etc.
export function allTables(fleet: FleetKnowledgeGraph): TableKnowledge[] {
  return fleet.schemas.flatMap((s) => s.tables);
}

export function allRelationships(fleet: FleetKnowledgeGraph): Relationship[] {
  return fleet.schemas.flatMap((s) => s.relationships);
}

export function allMetrics(fleet: FleetKnowledgeGraph): KnowledgeGraph["derivable_metrics"] {
  return fleet.schemas.flatMap((s) => s.derivable_metrics);
}

export function schemaKgById(fleet: FleetKnowledgeGraph, id: string): KnowledgeGraph | null {
  return fleet.schemas.find((s) => s.schema_id === id) ?? null;
}

export function findTable(
  fleet: FleetKnowledgeGraph,
  schemaId: string,
  tableName: string,
): TableKnowledge | null {
  const kg = schemaKgById(fleet, schemaId);
  return kg?.tables.find((t) => t.table_name === tableName) ?? null;
}
