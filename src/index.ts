import "dotenv/config";
import { createInterface } from "node:readline";
import { existsSync, rmSync } from "node:fs";
import { resolve } from "node:path";
import { databricks } from "./databricks.js";
import {
  activeSchemas,
  expandFleetConfig,
  findSchemaBySpec,
  loadFleetConfig,
  resolveActiveIds,
  schemaById,
  type FleetConfig,
  type SchemaConfig,
} from "./config.js";
import {
  discoverSchema,
  discoverTagsAndComments,
  sampleTables,
} from "./discovery.js";
import { matchDbtToTables, parseDbt } from "./dbt-parser.js";
import { matchProtoToTables, parseProtoFiles } from "./proto-parser.js";
import { buildFleet, schemaKgById, type SchemaInputs } from "./knowledge.js";
import { classifyQuestion } from "./classifier.js";
import { routeSchemas } from "./router.js";
import { generateSQL } from "./sql-generator.js";
import { generateNarrative } from "./narrative.js";
import { executeQuery, formatResultsAsTable } from "./engine.js";
import { verifySql } from "./verifier.js";
import {
  addCorrection,
  addValidatedQuery,
  getFeedbackStats,
  loadCorrections,
  loadValidatedQueries,
  logFeedback,
  parseCorrection,
  updateLastFeedback,
} from "./learning.js";
import type {
  ClassificationResult,
  Correction,
  DbtContext,
  FleetKnowledgeGraph,
  GeneratedSQL,
  ProtoContext,
  PulseResponse,
  QueryResult,
  TableSchema,
  ValidatedQuery,
} from "./types.js";

// ANSI colors
const C = {
  reset: "\x1b[0m",
  blue: "\x1b[34m",
  green: "\x1b[32m",
  cyan: "\x1b[36m",
  gray: "\x1b[90m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  magenta: "\x1b[35m",
  bold: "\x1b[1m",
};

interface PerSchemaDiscovery {
  cfg: SchemaConfig;
  schema: TableSchema[];
  proto: ProtoContext;
  dbt: DbtContext;
}

let config: FleetConfig;
let fleet: FleetKnowledgeGraph;
const perSchema = new Map<string, PerSchemaDiscovery>();
let activeSchemaIds: string[] = [];
let corrections: Correction[];
let validatedQueries: ValidatedQuery[];
let lastResponse: PulseResponse | null = null;
let lastPlan: ClassificationResult | null = null;
let verifyEnabled = false;

function printStep(msg: string): void {
  console.log(`${C.gray}${msg}${C.reset}`);
}
function printWarn(msg: string): void {
  console.log(`${C.yellow}⚠ ${msg}${C.reset}`);
}
function printErr(msg: string): void {
  console.log(`${C.red}✖ ${msg}${C.reset}`);
}

function requireEnv(): void {
  const required = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID", "ANTHROPIC_API_KEY"];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length) {
    printErr(`Missing required env vars: ${missing.join(", ")}`);
    printErr("Populate .env and retry.");
    process.exit(1);
  }
}

async function discoverOneSchema(cfg: SchemaConfig): Promise<{
  inputs: SchemaInputs;
  perSchema: PerSchemaDiscovery;
}> {
  console.log(`\n${C.bold}▶ ${cfg.id}${C.reset} ${C.gray}(${cfg.catalog}.${cfg.schema})${C.reset}`);

  console.log(`${C.gray}  🔍 Discovering schema...${C.reset}`);
  const schema = await discoverSchema(cfg);
  if (schema.length === 0) {
    printWarn(`  No tables found in ${cfg.catalog}.${cfg.schema} — skipping`);
    return { inputs: emptyInputs(cfg), perSchema: emptyDiscovery(cfg) };
  }
  console.log(`${C.green}  ✓${C.reset} ${schema.length} tables`);

  console.log(`${C.gray}  🏷️  Tags and comments...${C.reset}`);
  const tags = await discoverTagsAndComments(cfg);
  console.log(
    tags.length ? `${C.green}  ✓${C.reset} ${tags.length} tags` : `${C.gray}    no tags (not populated)${C.reset}`,
  );

  let proto: ProtoContext = { messages: [], enums: [], services: [], source_files: [] };
  if (cfg.protos) {
    console.log(`${C.gray}  📋 Parsing protobuf from ${cfg.protos}...${C.reset}`);
    proto = await parseProtoFiles(cfg);
    if (proto.messages.length) {
      proto = matchProtoToTables(proto, schema);
      console.log(
        `${C.green}  ✓${C.reset} ${C.magenta}${proto.messages.length} messages, ${proto.enums.length} enums${C.reset}`,
      );
    } else {
      console.log(`${C.gray}    no .proto files found at ${cfg.protos}${C.reset}`);
    }
  }

  let dbt: DbtContext = { models: [], source_files: [] };
  if (cfg.dbt) {
    console.log(`${C.gray}  📚 Parsing dbt docs from ${cfg.dbt}...${C.reset}`);
    dbt = await parseDbt(cfg);
    if (dbt.models.length) {
      dbt = matchDbtToTables(dbt, schema);
      console.log(`${C.green}  ✓${C.reset} ${dbt.models.length} dbt models`);
    } else {
      console.log(`${C.gray}    no dbt YAML models found${C.reset}`);
    }
  }

  console.log(`${C.gray}  📊 Sampling tables (PHI-safe)...${C.reset}`);
  const samples = await sampleTables(cfg, schema, (m) => printStep(`    ${m}`));
  console.log(`${C.green}  ✓${C.reset} sampled ${samples.length} tables`);

  return {
    inputs: { cfg, schema, samples, tags, proto, dbt },
    perSchema: { cfg, schema, proto, dbt },
  };
}

function emptyInputs(cfg: SchemaConfig): SchemaInputs {
  return {
    cfg,
    schema: [],
    samples: [],
    tags: [],
    proto: { messages: [], enums: [], services: [], source_files: [] },
    dbt: { models: [], source_files: [] },
  };
}

function emptyDiscovery(cfg: SchemaConfig): PerSchemaDiscovery {
  return {
    cfg,
    schema: [],
    proto: { messages: [], enums: [], services: [], source_files: [] },
    dbt: { models: [], source_files: [] },
  };
}

/**
 * Runs the full per-schema pipeline (discover → sample → proto/dbt → KG) for
 * any schema in the given set that isn't already in `fleet.schemas`. Called
 * from setup() for the initial active set and from `use` when switching to a
 * schema that hasn't been built yet.
 */
async function ensureSchemasBuilt(schemaIds: string[]): Promise<void> {
  const existing = new Set(fleet?.schemas.map((s) => s.schema_id) ?? []);
  const missing = schemaIds.filter((id) => !existing.has(id));
  if (missing.length === 0) return;

  const missingCfgs = missing
    .map((id) => config.schemas.find((s) => s.id === id))
    .filter((c): c is SchemaConfig => c !== undefined);

  if (missingCfgs.length === 0) return;

  const inputsList: SchemaInputs[] = [];
  for (const cfg of missingCfgs) {
    const { inputs, perSchema: ps } = await discoverOneSchema(cfg);
    inputsList.push(inputs);
    perSchema.set(cfg.id, ps);
  }

  console.log(
    `\n${C.bold}🧠 Building knowledge graph${missing.length > 1 ? "s" : ""}...${C.reset}`,
  );
  const freshFleet = await buildFleet(inputsList, printStep);
  const mergedSchemas = [
    ...(fleet?.schemas ?? []),
    ...freshFleet.schemas,
  ];
  fleet = {
    generated_at: new Date().toISOString(),
    schemas: mergedSchemas,
  };
  for (const kg of freshFleet.schemas) {
    const badges: string[] = [];
    if (kg.proto_enriched) badges.push(`${C.magenta}proto${C.reset}`);
    if (kg.dbt_enriched) badges.push(`${C.cyan}dbt${C.reset}`);
    const badgeStr = badges.length ? ` ${badges.join(",")}` : "";
    console.log(
      `${C.green}✓${C.reset} [${kg.schema_id}] ${kg.tables.length} tables, ${kg.relationships.length} rels, ${kg.derivable_metrics.length} metrics${badgeStr}`,
    );
  }
}

async function setup(): Promise<void> {
  requireEnv();

  console.log(`\n${C.bold}⚙️  Loading configuration...${C.reset}`);
  config = loadFleetConfig();
  const explicitCount = config.schemas.length;
  const summarySuffix = config.auto_discover
    ? ` + auto-discover(${config.auto_discover.catalogs.join(",")} / ${config.auto_discover.include.join(",")})`
    : "";
  console.log(
    `${C.green}✓${C.reset} Loaded ${explicitCount} explicit schema(s) from ${config.source}${summarySuffix}`,
  );

  console.log(`\n${C.bold}🔌 Connecting to Databricks...${C.reset}`);
  const conn = await databricks.testConnection();
  if (!conn.success) {
    printErr(`Connection failed: ${conn.message}`);
    process.exit(1);
  }
  console.log(`${C.green}✓${C.reset} ${conn.message}`);

  if (config.auto_discover) {
    console.log(`\n${C.bold}🧭 Auto-discovering schemas...${C.reset}`);
    try {
      config = await expandFleetConfig(config, (sql) =>
        databricks.executeStatement(sql).then((r) => ({ data: r.data })),
      );
      const discovered = config.schemas.length - explicitCount;
      console.log(
        `${C.green}✓${C.reset} Matched ${discovered} schema(s): ${C.gray}${config.schemas.map((s) => s.id).join(", ")}${C.reset}`,
      );
    } catch (err) {
      printErr(`Auto-discovery failed: ${(err as Error).message}`);
      process.exit(1);
    }
  }
  console.log(`${C.gray}Active at startup: ${config.active_ids.join(", ") || "(none)"}${C.reset}`);
  activeSchemaIds = [...config.active_ids];

  if (config.schemas.length === 0) {
    printErr("No schemas configured and auto-discovery matched none. Check schemas.yaml.");
    process.exit(1);
  }

  // Initialize the fleet with just the active schemas. Others remain configured
  // (visible in `schemas`) but have no KG until a `use` command activates them.
  fleet = { generated_at: new Date().toISOString(), schemas: [] };

  // If the active set is a single pinned schema, build its KG now so the
  // first question is fast. Otherwise, rely on per-question routing to
  // trigger on-demand builds — avoids spending minutes at startup for a
  // fleet the user may only touch 2-3 schemas of.
  if (activeSchemaIds.length === 1) {
    await ensureSchemasBuilt(activeSchemaIds);
  } else {
    console.log(
      `${C.gray}${activeSchemaIds.length} schemas active — KGs built on first use per question via the router${C.reset}`,
    );
  }

  console.log(`\n${C.bold}📂 Loading learning data...${C.reset}`);
  corrections = loadCorrections();
  validatedQueries = loadValidatedQueries();
  console.log(
    `${C.green}✓${C.reset} ${corrections.length} corrections, ${validatedQueries.length} validated queries`,
  );

  printBanner();
}

function printBanner(): void {
  const width = 80;
  const line = "═".repeat(width - 2);
  const pad = (s: string): string => {
    const visible = s.replace(/\x1b\[[0-9;]+m/g, "");
    // -2 for the "║ " prefix and -1 for the trailing "║", then clamp.
    const space = Math.max(0, width - 3 - visible.length);
    return `║ ${s}${" ".repeat(space)}║`;
  };

  const totalTables = fleet.schemas.reduce((n, s) => n + s.tables.length, 0);
  const totalRels = fleet.schemas.reduce((n, s) => n + s.relationships.length, 0);
  const totalMetrics = fleet.schemas.reduce((n, s) => n + s.derivable_metrics.length, 0);
  const anyProto = fleet.schemas.some((s) => s.proto_enriched);
  const discovered = config.schemas.length;
  const built = fleet.schemas.length;

  console.log(`\n${C.cyan}╔${line}╗`);
  console.log(pad(`${C.bold}Pulse — Schema-Agnostic Data Intelligence${C.reset}${C.cyan}`));
  console.log(
    pad(
      `${C.gray}${discovered} schemas discovered │ ${activeSchemaIds.length} active │ ${built} built${C.reset}${C.cyan}`,
    ),
  );
  console.log(pad(`${totalTables} tables │ ${totalRels} relationships │ ${totalMetrics} metrics (built only)`));
  console.log(
    pad(`Proto: ${anyProto ? "yes" : "no"} │ ${corrections.length} corrections │ ${validatedQueries.length} validated Qs`),
  );
  console.log(`╠${line}╣`);
  const cmds: [string, string][] = [
    ["schemas", "list configured schemas (active marked)"],
    ["use <id|all>", "switch active schema(s) — e.g. use hs_graph or use all"],
    ["tables", "list tables across active schemas"],
    ["table <id.name>", "full table details (schema_id optional if unique)"],
    ["metrics", "derivable metrics across active schemas"],
    ["funnel", "detected funnels"],
    ["relationships", "entity relationships"],
    ["enums", "proto enum types + values"],
    ["graph", "full knowledge graph summary"],
    ["validated", "saved validated queries"],
    ["stats", "feedback accuracy stats"],
    ["correct: <rule>", "add a correction rule"],
    ["good / bad / save", "rate last answer / save as validated query"],
    ["sql", "show last executed SQL"],
    ["plan <question>", "classification only (don't execute)"],
    ["verify on|off", "toggle Haiku SQL-audit before execution"],
    ["refresh", "rebuild everything from scratch"],
    ["exit", "quit"],
  ];
  for (const [cmd, desc] of cmds) {
    console.log(pad(`  ${C.yellow}${cmd.padEnd(20)}${C.reset}${C.cyan} ${desc}`));
  }
  console.log(`╚${line}╝${C.reset}`);
}

// =============== UTILITIES ===============

function activeSet(): Set<string> {
  return new Set(activeSchemaIds);
}

function isActive(schemaId: string): boolean {
  return activeSchemaIds.includes(schemaId);
}

function allActiveTables(): { kg: (typeof fleet.schemas)[number]; table: (typeof fleet.schemas)[number]["tables"][number] }[] {
  const out: { kg: (typeof fleet.schemas)[number]; table: (typeof fleet.schemas)[number]["tables"][number] }[] = [];
  for (const kg of fleet.schemas) {
    if (!isActive(kg.schema_id)) continue;
    for (const t of kg.tables) out.push({ kg, table: t });
  }
  return out;
}

function resolveTableRef(ref: string): { schemaId: string; tableName: string } | null {
  // Accept "schema_id.table_name" or bare "table_name"
  const parts = ref.split(".");
  if (parts.length === 2) {
    const kg = fleet.schemas.find((s) => s.schema_id === parts[0]);
    if (!kg) return null;
    const t = kg.tables.find((x) => x.table_name === parts[1]);
    return t ? { schemaId: parts[0]!, tableName: parts[1]! } : null;
  }
  // Bare — search active schemas only
  const hits: { schemaId: string; tableName: string }[] = [];
  for (const kg of fleet.schemas) {
    if (!isActive(kg.schema_id)) continue;
    const t = kg.tables.find((x) => x.table_name === ref);
    if (t) hits.push({ schemaId: kg.schema_id, tableName: ref });
  }
  if (hits.length === 1) return hits[0]!;
  return null;
}

// =============== COMMANDS ===============

function cmdSchemas(): void {
  console.log();
  console.log(`${C.bold}${"ID".padEnd(20)} ${"CATALOG.SCHEMA".padEnd(36)} ${"TABLES".padStart(8)}  FLAGS${C.reset}`);
  console.log(C.gray + "─".repeat(82) + C.reset);
  for (const cfg of config.schemas) {
    const kg = schemaKgById(fleet, cfg.id);
    const activeMark = isActive(cfg.id) ? `${C.green}●${C.reset}` : " ";
    const qualified = `${cfg.catalog}.${cfg.schema}`;
    const tables = kg ? kg.tables.length.toString() : "—";
    const flags: string[] = [];
    if (kg?.proto_enriched) flags.push(`${C.magenta}proto${C.reset}`);
    if (kg?.dbt_enriched) flags.push(`${C.cyan}dbt${C.reset}`);
    if (cfg.aliases.length) flags.push(`${C.gray}aliases: ${cfg.aliases.join(",")}${C.reset}`);
    console.log(
      `${activeMark} ${cfg.id.padEnd(18)} ${qualified.padEnd(36)} ${tables.padStart(8)}  ${flags.join("  ")}`,
    );
  }
  console.log();
}

async function cmdUse(spec: string): Promise<void> {
  const clean = spec.trim();
  if (!clean) {
    printWarn("Usage: use all | <schema_id> | <id1>,<id2> | <catalog>.* | <glob>");
    return;
  }
  const resolved = resolveActiveIds(clean, config.schemas);
  if (resolved.length === 0) {
    printWarn(`No schemas matched "${clean}". Try "schemas" to see options.`);
    return;
  }

  const needsBuild = resolved.filter(
    (id) => !fleet.schemas.some((s) => s.schema_id === id),
  );
  if (needsBuild.length > 0) {
    console.log(
      `${C.gray}First use of ${needsBuild.join(", ")} — building knowledge graph(s)...${C.reset}`,
    );
    await ensureSchemasBuilt(needsBuild);
  }

  activeSchemaIds = resolved;
  console.log(`${C.green}✓${C.reset} Active: ${C.bold}${resolved.join(", ")}${C.reset}`);
}

function cmdTables(): void {
  const rows = allActiveTables();
  if (rows.length === 0) {
    printWarn("No tables in active schemas.");
    return;
  }
  console.log();
  console.log(
    `${C.bold}${"SCHEMA".padEnd(16)} ${"TABLE".padEnd(34)} ${"ROWS".padStart(12)}  PURPOSE${C.reset}`,
  );
  console.log(C.gray + "─".repeat(100) + C.reset);
  for (const { kg, table } of rows) {
    const proto = table.proto_message ? ` ${C.magenta}⊚${C.reset}` : "";
    const dbt = table.dbt_model ? ` ${C.cyan}⊙${C.reset}` : "";
    console.log(
      `${C.gray}${kg.schema_id.padEnd(16)}${C.reset} ${table.table_name.padEnd(34)} ${table.row_count.toLocaleString().padStart(12)}  ${table.purpose.slice(0, 40)}${proto}${dbt}`,
    );
  }
  console.log();
}

function cmdTable(ref: string): void {
  const resolved = resolveTableRef(ref);
  if (!resolved) {
    printWarn(`Table "${ref}" not found in active schemas. Try "schema_id.table_name" to disambiguate.`);
    return;
  }
  const kg = schemaKgById(fleet, resolved.schemaId);
  const t = kg?.tables.find((x) => x.table_name === resolved.tableName);
  if (!t) return;

  console.log();
  console.log(
    `${C.bold}${t.catalog}.${t.schema}.${t.table_name}${C.reset} ${C.gray}(${t.domain}, ${t.row_count.toLocaleString()} rows)${C.reset}`,
  );
  console.log(`${C.gray}Purpose:${C.reset} ${t.purpose}`);
  if (t.proto_message) {
    console.log(`${C.gray}Proto:${C.reset} ${C.magenta}${t.proto_message}${C.reset}`);
  }
  if (t.dbt_model) {
    console.log(`${C.gray}dbt model:${C.reset} ${C.cyan}${t.dbt_model}${C.reset}`);
  }
  console.log();
  console.log(`${C.bold}Columns:${C.reset}`);
  for (const col of t.all_columns) {
    const keyMeaning = t.key_columns.find((k) => k.column === col.column_name)?.meaning;
    const enumInfo = t.enum_columns.find((e) => e.column === col.column_name);
    const meaningPart = keyMeaning
      ? ` — ${keyMeaning}`
      : col.comment
        ? ` — ${C.gray}${col.comment}${C.reset}`
        : "";
    const enumPart = enumInfo
      ? ` ${C.magenta}[ENUM ${enumInfo.enum_name}: ${enumInfo.values.slice(0, 5).join(", ")}${enumInfo.values.length > 5 ? "..." : ""}]${C.reset}`
      : "";
    console.log(`  ${col.column_name} ${C.gray}(${col.data_type})${C.reset}${meaningPart}${enumPart}`);
  }
  if (t.join_keys.length) {
    console.log();
    console.log(`${C.bold}Join keys:${C.reset}`);
    for (const j of t.join_keys) {
      console.log(`  ${j.column} ${C.gray}→${C.reset} ${j.joins_to}.${j.foreign_column}`);
    }
  }
  if (t.default_filters.length) {
    console.log();
    console.log(`${C.bold}Default filters:${C.reset}`);
    for (const f of t.default_filters) {
      console.log(`  ${C.yellow}${f.column} ${f.condition}${C.reset} ${C.gray}(${f.reason})${C.reset}`);
    }
  }
  if (t.business_rules.length) {
    console.log();
    console.log(`${C.bold}Business rules:${C.reset}`);
    for (const r of t.business_rules) console.log(`  • ${r}`);
  }
  console.log();
}

function cmdMetrics(): void {
  const active = activeSet();
  const metrics = fleet.schemas
    .filter((s) => active.has(s.schema_id))
    .flatMap((s) => s.derivable_metrics.map((m) => ({ ...m, _sid: s.schema_id })));
  if (metrics.length === 0) {
    printWarn("No derivable metrics in active schemas.");
    return;
  }
  console.log();
  for (const m of metrics) {
    console.log(`${C.bold}${m.name}${C.reset} ${C.gray}[${m._sid}]${C.reset}`);
    console.log(`  ${m.description}`);
    console.log(`  ${C.gray}Tables: ${m.tables_needed.join(", ")}${C.reset}`);
    console.log();
  }
}

function cmdFunnel(): void {
  const funnels = fleet.schemas.filter((s) => isActive(s.schema_id) && s.funnel);
  if (funnels.length === 0) {
    console.log(C.gray + "No funnels detected in active schemas." + C.reset);
    return;
  }
  console.log();
  for (const s of funnels) {
    console.log(`${C.bold}${s.funnel!.name}${C.reset} ${C.gray}[${s.schema_id}]${C.reset}`);
    for (const stage of s.funnel!.stages) {
      console.log(
        `  ${stage.order}. ${C.cyan}${stage.name}${C.reset} ${C.gray}(${stage.table})${C.reset} ${C.yellow}${stage.filter}${C.reset}`,
      );
    }
    console.log();
  }
}

function cmdRelationships(): void {
  const rels = fleet.schemas
    .filter((s) => isActive(s.schema_id))
    .flatMap((s) => s.relationships);
  if (rels.length === 0) {
    printWarn("No relationships in active schemas.");
    return;
  }
  console.log();
  for (const r of rels) {
    const srcBadge = r.source === "proto"
      ? `${C.magenta}[proto]${C.reset}`
      : r.source === "dbt"
        ? `${C.cyan}[dbt]${C.reset}`
        : `${C.gray}[inferred]${C.reset}`;
    console.log(
      `  ${C.gray}[${r.schema_id}]${C.reset} ${r.from_table} ${C.gray}──[${r.join_key}]──▶${C.reset} ${r.to_table} ${C.gray}(${r.type})${C.reset} ${srcBadge}`,
    );
  }
  console.log();
}

function cmdEnums(): void {
  const rows: { sid: string; enumName: string; enumFullName: string; values: { name: string; number: number; comment?: string }[]; comment?: string }[] = [];
  for (const [sid, ps] of perSchema.entries()) {
    if (!isActive(sid)) continue;
    for (const en of ps.proto.enums) {
      rows.push({ sid, enumName: en.name, enumFullName: en.full_name, values: en.values, comment: en.comment });
    }
  }
  if (rows.length === 0) {
    console.log(
      C.gray + "No proto enums available in active schemas (configure `protos:` in schemas.yaml to enable)." + C.reset,
    );
    return;
  }
  console.log();
  for (const r of rows) {
    console.log(
      `${C.magenta}${C.bold}${r.enumName}${C.reset} ${C.gray}[${r.sid}] (${r.enumFullName})${C.reset}`,
    );
    if (r.comment) console.log(`  ${C.gray}${r.comment}${C.reset}`);
    for (const v of r.values) {
      const cmt = v.comment ? ` ${C.gray}— ${v.comment}${C.reset}` : "";
      console.log(`  ${v.name} ${C.gray}= ${v.number}${C.reset}${cmt}`);
    }
    console.log();
  }
}

function cmdGraph(): void {
  console.log();
  console.log(`${C.bold}Fleet summary${C.reset} ${C.gray}(generated ${fleet.generated_at})${C.reset}`);
  for (const kg of fleet.schemas) {
    const activeMark = isActive(kg.schema_id) ? `${C.green}●${C.reset}` : " ";
    console.log(
      `  ${activeMark} [${kg.schema_id}] ${kg.catalog}.${kg.schema}: ${kg.tables.length} tables, ${kg.relationships.length} rels, ${kg.derivable_metrics.length} metrics${kg.proto_enriched ? ` ${C.magenta}proto${C.reset}` : ""}${kg.dbt_enriched ? ` ${C.cyan}dbt${C.reset}` : ""}`,
    );
  }
  const activeKgs = fleet.schemas.filter((s) => isActive(s.schema_id));
  const sampleQs = activeKgs.flatMap((s) => s.sample_questions).slice(0, 10);
  if (sampleQs.length) {
    console.log();
    console.log(`${C.bold}Sample questions:${C.reset}`);
    for (const q of sampleQs) console.log(`  • ${q}`);
  }
  console.log();
}

function cmdValidated(): void {
  if (validatedQueries.length === 0) {
    console.log(C.gray + "No validated queries yet. Use `save` after a good answer to save one." + C.reset);
    return;
  }
  console.log();
  for (const v of validatedQueries) {
    console.log(`${C.bold}${v.description}${C.reset} ${C.gray}(used ${v.use_count}× since ${v.validated_at.slice(0, 10)})${C.reset}`);
    console.log(`  Q: ${v.question}`);
    console.log(`  Schemas: ${v.schemas_used.join(", ") || "(none)"}`);
    console.log();
  }
}

function cmdVerify(spec: string): void {
  const lower = spec.trim().toLowerCase();
  if (lower === "on" || lower === "true" || lower === "1") {
    verifyEnabled = true;
    console.log(
      `${C.green}✓ Self-critique enabled${C.reset} ${C.gray}(+1 Haiku call, ~$0.002, ~2s per question)${C.reset}`,
    );
  } else if (lower === "off" || lower === "false" || lower === "0") {
    verifyEnabled = false;
    console.log(`${C.gray}Self-critique disabled.${C.reset}`);
  } else if (lower === "" || lower === "status") {
    console.log(
      `Self-critique is currently ${verifyEnabled ? C.green + "on" + C.reset : C.gray + "off" + C.reset}. Toggle with \`verify on\` or \`verify off\`.`,
    );
  } else {
    printWarn("Usage: verify [on|off|status]");
  }
}

function cmdStats(): void {
  const s = getFeedbackStats();
  console.log();
  console.log(`${C.bold}Feedback stats:${C.reset}`);
  console.log(`  Total: ${s.total} | ${C.green}Good: ${s.good}${C.reset} | ${C.red}Bad: ${s.bad}${C.reset} | ${C.yellow}Corrected: ${s.corrected}${C.reset} | Unrated: ${s.unrated}`);
  console.log(`  Accuracy (of rated): ${s.accuracy_pct}%`);
  console.log();
}

function knownTablesBySchema(): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const kg of fleet.schemas) {
    out[kg.schema_id] = kg.tables.map((t) => t.table_name);
  }
  return out;
}

function cmdCorrect(input: string): void {
  const defaultSchemaId = activeSchemaIds[0] ?? config.schemas[0]!.id;
  const parsed = parseCorrection(input, knownTablesBySchema(), defaultSchemaId);
  if (!parsed) {
    printWarn("Usage: correct: <rule>  OR  correct <schema_id>.<table>: <rule>");
    return;
  }
  const entry = addCorrection(parsed.schema_id, parsed.table, parsed.rule);
  corrections = loadCorrections();
  console.log(
    `${C.green}✓${C.reset} Correction saved: ${C.gray}[${entry.schema_id}.${entry.table}]${C.reset} ${entry.rule}`,
  );
}

function cmdGood(): void {
  if (!lastResponse) {
    printWarn("No query to rate yet.");
    return;
  }
  const ok = updateLastFeedback("good");
  console.log(ok ? `${C.green}✓ Marked as good.${C.reset}` : `${C.gray}No feedback entry to update.${C.reset}`);
}

async function cmdBad(rl: ReturnType<typeof createInterface>): Promise<void> {
  if (!lastResponse) {
    printWarn("No query to rate yet.");
    return;
  }
  const note = (await promptLine(rl, `${C.yellow}What was wrong? ${C.reset}`)) ?? "";
  updateLastFeedback("bad", note);
  const saveAsCorrection = (await promptLine(rl, `${C.yellow}Save as correction rule? (y/N): ${C.reset}`)) ?? "";
  if (saveAsCorrection.trim().toLowerCase() === "y") {
    const defaultSchemaId = lastResponse.classification.relevant_tables[0]?.schema_id
      ?? (activeSchemaIds[0] ?? config.schemas[0]!.id);
    const parsed = parseCorrection(note, knownTablesBySchema(), defaultSchemaId);
    if (parsed) {
      addCorrection(parsed.schema_id, parsed.table, parsed.rule);
      corrections = loadCorrections();
      console.log(`${C.green}✓ Correction saved.${C.reset}`);
    } else {
      printWarn("Could not parse correction. Use `correct: <rule>` explicitly.");
    }
  }
}

async function cmdSave(rl: ReturnType<typeof createInterface>): Promise<void> {
  if (!lastResponse) {
    printWarn("No query to save yet.");
    return;
  }
  const desc = await promptLine(rl, `${C.yellow}Brief description: ${C.reset}`);
  if (!desc || !desc.trim()) return;
  addValidatedQuery(
    lastResponse.question,
    lastResponse.generated_sql.sql,
    lastResponse.generated_sql.schemas_used,
    lastResponse.generated_sql.tables_used,
    desc.trim(),
  );
  validatedQueries = loadValidatedQueries();
  console.log(`${C.green}✓ Saved as validated query.${C.reset}`);
}

function cmdSql(): void {
  if (!lastResponse) {
    printWarn("No SQL to show yet.");
    return;
  }
  console.log(`\n${C.cyan}${lastResponse.generated_sql.sql}${C.reset}\n`);
}

async function cmdPlan(question: string): Promise<void> {
  const { scopeIds, routedIds } = await resolveScope(question);
  console.log(`${C.gray}🔎 Classifying (plan only)...${C.reset}`);
  const classification = await classifyQuestion(
    question,
    fleet,
    scopeIds,
    corrections,
    validatedQueries,
  );
  if (routedIds) classification.routed_schema_ids = routedIds;
  lastPlan = classification;

  if (!classification.answerable || classification.relevant_tables.length === 0) {
    printWarn(
      `Question likely not answerable (confidence ${(classification.confidence * 100).toFixed(0)}%).`,
    );
    return;
  }

  console.log();
  console.log(
    `${C.bold}Classification${C.reset} ${C.gray}(method: ${classification.method}, confidence: ${Math.round(classification.confidence * 100)}%${classification.routed_schema_ids ? `, routed to: ${classification.routed_schema_ids.join(",")}` : ""})${C.reset}`,
  );
  console.log(`${C.bold}Relevant tables:${C.reset}`);
  for (const t of classification.relevant_tables) {
    console.log(
      `  ${C.green}${t.schema_id}.${t.table}${C.reset} ${C.gray}(${Math.round(t.relevance_score * 100)}%)${C.reset} — ${t.purpose}`,
    );
  }
  if (classification.suggested_joins.length) {
    console.log(`${C.bold}Suggested joins:${C.reset}`);
    for (const j of classification.suggested_joins) {
      console.log(`  ${j.from_schema_id}.${j.from} ⟕ ${j.to_schema_id}.${j.to} ON ${j.on}`);
    }
  }
  if (classification.suggested_filters.length) {
    console.log(`${C.bold}Suggested filters:${C.reset}`);
    for (const f of classification.suggested_filters) {
      console.log(`  ${C.yellow}[${f.schema_id}] ${f.column} ${f.condition}${C.reset} ${C.gray}— ${f.reason}${C.reset}`);
    }
  }
  if (classification.enum_context.length) {
    console.log(`${C.bold}Enum context:${C.reset}`);
    for (const e of classification.enum_context) {
      console.log(
        `  ${C.magenta}[${e.schema_id}] ${e.column}${C.reset}: ${e.valid_values.slice(0, 6).join(", ")}${e.valid_values.length > 6 ? "..." : ""}`,
      );
    }
  }
  if (Object.keys(classification.extracted_parameters).length) {
    console.log(`${C.bold}Extracted parameters:${C.reset}`);
    for (const [k, v] of Object.entries(classification.extracted_parameters)) {
      console.log(`  ${k} = ${C.yellow}${v}${C.reset}`);
    }
  }
  console.log();
}

async function cmdRefresh(): Promise<void> {
  const cacheDir = resolve("cache");
  if (existsSync(cacheDir)) {
    rmSync(cacheDir, { recursive: true, force: true });
    console.log(`${C.yellow}✓ Cache cleared.${C.reset}`);
  }
  perSchema.clear();
  await setup();
}

/**
 * Pick the subset of active schemas relevant to a question, building any
 * picked-but-unbuilt schemas on demand. Returns both the routed set (for
 * display) and the final scope passed to the classifier.
 */
async function resolveScope(
  question: string,
): Promise<{ scopeIds: string[]; routedIds?: string[] }> {
  if (activeSchemaIds.length <= 1) {
    // Single active schema — user has pinned scope; no routing needed.
    return { scopeIds: activeSchemaIds };
  }

  console.log(`${C.gray}🧭 Routing...${C.reset}`);
  const picked = await routeSchemas(question, fleet, config.schemas, activeSchemaIds);

  const unbuilt = picked.filter(
    (id) => !fleet.schemas.some((s) => s.schema_id === id),
  );
  if (unbuilt.length > 0) {
    console.log(
      `${C.gray}  first use of ${unbuilt.join(", ")} — building knowledge graph${unbuilt.length > 1 ? "s" : ""}...${C.reset}`,
    );
    await ensureSchemasBuilt(unbuilt);
  }

  const routedSummary = picked.length === activeSchemaIds.length
    ? `no narrowing (${picked.length})`
    : `${picked.length} of ${activeSchemaIds.length}: ${picked.join(", ")}`;
  console.log(`${C.gray}  routed: ${routedSummary}${C.reset}`);

  return { scopeIds: picked, routedIds: picked };
}

async function runQuestion(question: string): Promise<void> {
  const start = Date.now();

  let scopeIds: string[] = activeSchemaIds;
  let routedIds: string[] | undefined;
  try {
    const r = await resolveScope(question);
    scopeIds = r.scopeIds;
    routedIds = r.routedIds;
  } catch (err) {
    printErr(`Routing failed: ${(err as Error).message}`);
    return;
  }

  console.log(`${C.gray}🔎 Classifying...${C.reset}`);
  let classification: ClassificationResult;
  try {
    classification = await classifyQuestion(
      question,
      fleet,
      scopeIds,
      corrections,
      validatedQueries,
    );
    if (routedIds) classification.routed_schema_ids = routedIds;
  } catch (err) {
    const e = err as Error;
    printErr(`Classification failed: ${e.message}`);
    console.error(`${C.gray}  scope: ${scopeIds.join(", ")}${C.reset}`);
    if (e.stack) console.error(`${C.gray}${e.stack.split("\n").slice(0, 4).join("\n")}${C.reset}`);
    return;
  }

  if (!classification.answerable || classification.relevant_tables.length === 0) {
    printWarn(
      `Question probably isn't answerable with the active schema(s) (confidence ${(classification.confidence * 100).toFixed(0)}%).`,
    );
    logFeedback({
      question,
      sql: "",
      narrative: "Not answerable",
      feedback: "unrated",
      timestamp: new Date().toISOString(),
      schemas_used: [],
    });
    return;
  }

  const tableLabels = classification.relevant_tables
    .map((t) => `${t.schema_id}.${t.table}`)
    .join(", ");
  const routedLabel = classification.routed_schema_ids
    ? ` | routed: ${classification.routed_schema_ids.join(",")}`
    : "";
  console.log(
    `${C.gray}  tables: ${tableLabels} | ${classification.method} | ${Math.round(classification.confidence * 100)}%${routedLabel}${C.reset}`,
  );

  console.log(`${C.gray}✏️  Planning query...${C.reset}`);
  let sql: GeneratedSQL;
  try {
    sql = await generateSQL(question, classification, fleet, corrections, validatedQueries);
  } catch (err) {
    printErr(`Query planning failed: ${(err as Error).message}`);
    return;
  }
  console.log(`${C.gray}  ${sql.explanation}${C.reset}`);

  if (verifyEnabled) {
    console.log(`${C.gray}🔍 Verifying SQL...${C.reset}`);
    try {
      const verification = await verifySql(question, sql, classification, fleet);
      if (!verification.looks_correct && verification.issues.length > 0) {
        console.log(`${C.yellow}  ⚠ Verifier flagged ${verification.issues.length} issue(s):${C.reset}`);
        for (const issue of verification.issues) {
          console.log(`${C.yellow}    - ${issue}${C.reset}`);
        }
        if (verification.suggested_fix) {
          console.log(`${C.gray}  Retrying with verifier's suggested fix...${C.reset}`);
          sql = { ...sql, sql: verification.suggested_fix };
        } else {
          printWarn(
            "Execution blocked. Type `sql` to inspect, or `verify off` to bypass.",
          );
          logFeedback({
            question,
            sql: sql.sql,
            narrative: `Blocked by verifier: ${verification.issues.join("; ")}`,
            feedback: "bad",
            timestamp: new Date().toISOString(),
            schemas_used: sql.schemas_used,
          });
          return;
        }
      } else {
        console.log(`${C.green}  ✓ verified${C.reset}`);
      }
    } catch (err) {
      printWarn(`Verifier failed (continuing anyway): ${(err as Error).message.slice(0, 100)}`);
    }
  }

  console.log(`${C.gray}⚡ Executing...${C.reset}`);
  let result: QueryResult;
  try {
    result = await executeQuery(sql);
  } catch (err) {
    printErr(`Execution failed: ${(err as Error).message}`);
    logFeedback({
      question,
      sql: sql.sql,
      narrative: `Execution error: ${(err as Error).message}`,
      feedback: "bad",
      timestamp: new Date().toISOString(),
      schemas_used: sql.schemas_used,
    });
    return;
  }

  console.log(`${C.gray}💬 Summarizing...${C.reset}`);
  const narrative = await generateNarrative(question, result, sql, classification, fleet);
  const total = Date.now() - start;

  const response: PulseResponse = {
    question,
    classification,
    generated_sql: sql,
    result,
    narrative,
    total_time_ms: total,
    timestamp: new Date().toISOString(),
  };
  lastResponse = response;

  console.log(`\n${C.gray}${"─".repeat(70)}${C.reset}`);
  console.log(`${C.green}${narrative}${C.reset}`);
  console.log();
  console.log(formatResultsAsTable(result, 15));
  console.log();
  console.log(
    `${C.gray}[${classification.method} | ${Math.round(classification.confidence * 100)}% | ${result.row_count.toLocaleString()} rows | ${total}ms]${C.reset}`,
  );
  console.log(`${C.gray}${"─".repeat(70)}${C.reset}\n`);

  logFeedback({
    question,
    sql: sql.sql,
    narrative,
    feedback: "unrated",
    timestamp: new Date().toISOString(),
    schemas_used: sql.schemas_used,
  });
}

// =============== REPL ===============

function promptLine(
  rl: ReturnType<typeof createInterface>,
  prompt: string,
): Promise<string | null> {
  return new Promise((resolve) => {
    let resolved = false;
    const onClose = (): void => finish(null);
    const finish = (value: string | null): void => {
      if (resolved) return;
      resolved = true;
      rl.off("close", onClose);
      resolve(value);
    };
    rl.once("close", onClose);
    try {
      rl.question(prompt, (answer) => finish(answer));
    } catch {
      finish(null);
    }
  });
}

async function repl(): Promise<void> {
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  const prompt = `${C.blue}pulse>${C.reset} `;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const line = await promptLine(rl, prompt);
    if (line === null) {
      rl.close();
      return;
    }
    const raw = line.trim();
    if (!raw) continue;
    const lower = raw.toLowerCase();

    try {
      if (lower === "exit" || lower === "quit") {
        rl.close();
        return;
      }
      if (lower === "schemas") { cmdSchemas(); continue; }
      if (lower.startsWith("use ")) { await cmdUse(raw.slice(4).trim()); continue; }
      if (lower === "tables") { cmdTables(); continue; }
      if (lower.startsWith("table ")) { cmdTable(raw.slice(6).trim()); continue; }
      if (lower === "metrics") { cmdMetrics(); continue; }
      if (lower === "funnel") { cmdFunnel(); continue; }
      if (lower === "relationships") { cmdRelationships(); continue; }
      if (lower === "enums") { cmdEnums(); continue; }
      if (lower === "graph") { cmdGraph(); continue; }
      if (lower === "validated") { cmdValidated(); continue; }
      if (lower === "stats") { cmdStats(); continue; }
      if (lower === "verify") { cmdVerify(""); continue; }
      if (lower.startsWith("verify ")) { cmdVerify(raw.slice(7).trim()); continue; }
      if (lower === "good") { cmdGood(); continue; }
      if (lower === "sql") { cmdSql(); continue; }
      if (lower === "bad") { await cmdBad(rl); continue; }
      if (lower === "save") { await cmdSave(rl); continue; }
      if (lower === "refresh") { await cmdRefresh(); continue; }
      if (lower.startsWith("correct:") || lower.startsWith("correct ")) { cmdCorrect(raw); continue; }
      if (lower.startsWith("plan ")) { await cmdPlan(raw.slice(5).trim()); continue; }
      if (lower === "plan" && lastResponse) { await cmdPlan(lastResponse.question); continue; }

      await runQuestion(raw);
    } catch (err) {
      printErr(`Unhandled error: ${(err as Error).message}`);
    }
  }
  // reference to silence "unused" warnings in minified builds; last-plan is exposed for debug
  void lastPlan;
  void findSchemaBySpec;
  void schemaById;
  void activeSchemas;
}

async function main(): Promise<void> {
  process.on("SIGINT", () => {
    console.log(`\n${C.gray}Goodbye.${C.reset}`);
    process.exit(0);
  });
  try {
    await setup();
    await repl();
  } catch (err) {
    printErr(`Fatal: ${(err as Error).message}`);
    if ((err as Error).stack) console.error((err as Error).stack);
    process.exit(1);
  }
  console.log(`${C.gray}Goodbye.${C.reset}`);
}

main();
