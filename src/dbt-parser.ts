import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { resolve, join, dirname } from "node:path";
import yaml from "js-yaml";
import type { SchemaConfig } from "./config.js";
import type { DbtContext, DbtModel, TableSchema } from "./types.js";

function schemaCacheFile(schemaId: string): string {
  return resolve("cache", schemaId, "dbt-context.json");
}

function ensureParent(path: string): void {
  const dir = dirname(path);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

function findDbtYamls(modelsDir: string): string[] {
  if (!existsSync(modelsDir)) return [];
  const out: string[] = [];
  const walk = (d: string): void => {
    for (const entry of readdirSync(d)) {
      const full = join(d, entry);
      const st = statSync(full);
      if (st.isDirectory()) walk(full);
      else if (entry.endsWith(".yml") || entry.endsWith(".yaml")) out.push(full);
    }
  };
  walk(modelsDir);
  return out.sort();
}

interface RawDbtYaml {
  version?: number;
  models?: Array<{
    name: string;
    description?: string;
    database?: string;
    schema?: string;
    meta?: Record<string, unknown>;
    columns?: Array<{
      name: string;
      description?: string;
      tests?: unknown[];
      meta?: Record<string, unknown>;
    }>;
  }>;
}

function isCacheValid(cacheFile: string, files: string[]): boolean {
  if (!existsSync(cacheFile)) return false;
  const cacheMtime = statSync(cacheFile).mtimeMs;
  for (const f of files) {
    if (statSync(f).mtimeMs > cacheMtime) return false;
  }
  return true;
}

export async function parseDbt(cfg: SchemaConfig): Promise<DbtContext> {
  if (!cfg.dbt) return emptyContext();

  const projectPath = resolve(cfg.dbt);
  if (!existsSync(projectPath)) return emptyContext();

  const projectDir = projectPath.endsWith("dbt_project.yml")
    ? dirname(projectPath)
    : projectPath;

  const modelsDir = join(projectDir, "models");
  const yamls = findDbtYamls(modelsDir);
  if (yamls.length === 0) return emptyContext();

  const cacheFile = schemaCacheFile(cfg.id);
  if (isCacheValid(cacheFile, yamls)) {
    try {
      return JSON.parse(readFileSync(cacheFile, "utf-8")) as DbtContext;
    } catch {
      // rebuild
    }
  }

  const models: DbtModel[] = [];
  for (const yml of yamls) {
    try {
      const raw = yaml.load(readFileSync(yml, "utf-8")) as RawDbtYaml | null;
      if (!raw?.models) continue;
      for (const m of raw.models) {
        if (!m.name) continue;
        const testsOf = (t: unknown[] | undefined): string[] =>
          (t ?? []).map((x) => (typeof x === "string" ? x : JSON.stringify(x)));
        models.push({
          name: m.name,
          description: m.description,
          database: m.database,
          schema: m.schema,
          meta: m.meta,
          columns: (m.columns ?? []).map((c) => ({
            name: c.name,
            description: c.description,
            tests: testsOf(c.tests),
            meta: c.meta,
          })),
        });
      }
    } catch {
      // Skip malformed YAML
    }
  }

  const ctx: DbtContext = {
    models,
    source_files: yamls.map((f) => f.replace(projectDir + "/", "")),
  };

  ensureParent(cacheFile);
  writeFileSync(cacheFile, JSON.stringify(ctx, null, 2));
  return ctx;
}

function emptyContext(): DbtContext {
  return { models: [], source_files: [] };
}

export function matchDbtToTables(ctx: DbtContext, tables: TableSchema[]): DbtContext {
  const byName = new Map(tables.map((t) => [t.table_name.toLowerCase(), t]));
  for (const m of ctx.models) {
    const match = byName.get(m.name.toLowerCase());
    if (match) m.likely_table_match = match.table_name;
  }
  return ctx;
}

const MAX_MODELS_SHOWN = 40;
const MAX_COLS_PER_MODEL = 15;

export function formatDbtForClaude(ctx: DbtContext): string {
  if (ctx.models.length === 0) return "";
  const parts: string[] = [];
  parts.push("=== DBT MODEL DOCUMENTATION (human-curated analytics semantics) ===");
  parts.push(`Source files: ${ctx.source_files.slice(0, 5).join(", ")}${ctx.source_files.length > 5 ? `, +${ctx.source_files.length - 5} more` : ""}`);
  parts.push("");

  const shown = ctx.models.slice(0, MAX_MODELS_SHOWN);
  for (const m of shown) {
    const match = m.likely_table_match ? ` → table \`${m.likely_table_match}\`` : "";
    parts.push(`Model: ${m.name}${match}`);
    if (m.description) parts.push(`  // ${m.description}`);
    const cols = m.columns.slice(0, MAX_COLS_PER_MODEL);
    for (const c of cols) {
      const desc = c.description ? ` — ${c.description}` : "";
      const tests = c.tests && c.tests.length ? ` [tests: ${c.tests.join(",")}]` : "";
      parts.push(`  ${c.name}${desc}${tests}`);
    }
    if (m.columns.length > MAX_COLS_PER_MODEL) {
      parts.push(`  ...and ${m.columns.length - MAX_COLS_PER_MODEL} more columns`);
    }
    parts.push("");
  }
  if (ctx.models.length > MAX_MODELS_SHOWN) {
    parts.push(`(${ctx.models.length - MAX_MODELS_SHOWN} more models not shown)`);
  }

  return parts.join("\n").slice(0, 10000);
}
