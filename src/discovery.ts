import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { databricks } from "./databricks.js";
import type { SchemaConfig } from "./config.js";
import type {
  ColumnInfo,
  TableSample,
  TableSchema,
  TagInfo,
} from "./types.js";

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

// Above this row count, skip expensive DISTINCT sampling (the LIMIT 5 still runs).
// System metadata tables can have tens of millions of rows and a DISTINCT query
// can run for minutes — not worth the wait for slightly richer value examples.
const DISTINCT_SAMPLING_ROW_CAP = 1_000_000;

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

function readCache<T>(path: string): T | null {
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf-8")) as T;
  } catch {
    return null;
  }
}

function writeCache(path: string, data: unknown): void {
  ensureDir(resolve(path, ".."));
  writeFileSync(path, JSON.stringify(data, null, 2));
}

function isPhiColumn(columnName: string, patterns: string[]): boolean {
  const lower = columnName.toLowerCase();
  return patterns.some((p) => lower.includes(p));
}

function quoteIdent(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
}

export async function discoverSchema(cfg: SchemaConfig): Promise<TableSchema[]> {
  const cachePath = resolve(schemaCacheDir(cfg.id), "schema.json");
  if (isFresh(cachePath)) {
    const cached = readCache<TableSchema[]>(cachePath);
    if (cached) return cached;
  }

  const colSql = `
    SELECT table_name, column_name, data_type, comment
    FROM ${cfg.catalog}.information_schema.columns
    WHERE table_schema = '${cfg.schema}'
    ORDER BY table_name, ordinal_position
  `;
  const tblSql = `
    SELECT table_name, comment
    FROM ${cfg.catalog}.information_schema.tables
    WHERE table_schema = '${cfg.schema}'
  `;

  const [colRes, tblRes] = await Promise.all([
    databricks.executeStatement(colSql),
    databricks.executeStatement(tblSql),
  ]);

  const tableCommentMap = new Map<string, string>();
  for (const row of tblRes.data) {
    const tn = String(row.table_name);
    const cm = row.comment;
    if (cm) tableCommentMap.set(tn, String(cm));
  }

  const grouped = new Map<string, ColumnInfo[]>();
  for (const row of colRes.data) {
    const tn = String(row.table_name);
    if (!grouped.has(tn)) grouped.set(tn, []);
    grouped.get(tn)!.push({
      column_name: String(row.column_name),
      data_type: String(row.data_type),
      comment: row.comment === null || row.comment === undefined ? null : String(row.comment),
    });
  }

  const tables: TableSchema[] = Array.from(grouped.entries())
    .map(([table_name, columns]) => ({
      schema_id: cfg.id,
      catalog: cfg.catalog,
      schema: cfg.schema,
      table_name,
      columns,
      table_comment: tableCommentMap.get(table_name),
    }))
    .sort((a, b) => a.table_name.localeCompare(b.table_name));

  writeCache(cachePath, tables);
  return tables;
}

export async function discoverTagsAndComments(cfg: SchemaConfig): Promise<TagInfo[]> {
  const cachePath = resolve(schemaCacheDir(cfg.id), "tags-comments.json");
  if (isFresh(cachePath)) {
    const cached = readCache<TagInfo[]>(cachePath);
    if (cached) return cached;
  }

  const results: TagInfo[] = [];

  try {
    const tableSql = `
      SELECT table_name, tag_name, tag_value
      FROM ${cfg.catalog}.information_schema.table_tags
      WHERE schema_name = '${cfg.schema}'
    `;
    const tableRes = await databricks.executeStatement(tableSql);
    for (const row of tableRes.data) {
      results.push({
        schema_id: cfg.id,
        object_type: "table",
        table_name: String(row.table_name),
        tag_name: String(row.tag_name),
        tag_value: String(row.tag_value ?? ""),
      });
    }
  } catch {
    // table_tags unavailable — fine
  }

  try {
    const colSql = `
      SELECT table_name, column_name, tag_name, tag_value
      FROM ${cfg.catalog}.information_schema.column_tags
      WHERE schema_name = '${cfg.schema}'
    `;
    const colRes = await databricks.executeStatement(colSql);
    for (const row of colRes.data) {
      results.push({
        schema_id: cfg.id,
        object_type: "column",
        table_name: String(row.table_name),
        column_name: String(row.column_name),
        tag_name: String(row.tag_name),
        tag_value: String(row.tag_value ?? ""),
      });
    }
  } catch {
    // column_tags unavailable — fine
  }

  writeCache(cachePath, results);
  return results;
}

export async function sampleTables(
  cfg: SchemaConfig,
  tables: TableSchema[],
  onProgress?: (msg: string) => void,
): Promise<TableSample[]> {
  const cachePath = resolve(schemaCacheDir(cfg.id), "samples.json");
  if (isFresh(cachePath)) {
    const cached = readCache<TableSample[]>(cachePath);
    if (cached) return cached;
  }

  const samples: TableSample[] = [];
  const fq = (t: string): string =>
    `${cfg.catalog}.${cfg.schema}.${quoteIdent(t)}`;

  for (let i = 0; i < tables.length; i++) {
    const table = tables[i]!;
    const tn = table.table_name;
    onProgress?.(`[${cfg.id}] Sampling ${i + 1}/${tables.length}: ${tn}...`);

    const safeColumns = table.columns.filter(
      (c) => !isPhiColumn(c.column_name, cfg.phi_skip_patterns),
    );
    const skippedColumns = table.columns
      .filter((c) => isPhiColumn(c.column_name, cfg.phi_skip_patterns))
      .map((c) => c.column_name);

    if (safeColumns.length === 0) {
      samples.push({
        schema_id: cfg.id,
        table_name: tn,
        sample_rows: [],
        distinct_values: {},
        row_count: 0,
        sampled_columns: [],
        skipped_columns: skippedColumns,
      });
      continue;
    }

    const colList = safeColumns.map((c) => quoteIdent(c.column_name)).join(", ");
    let sampleRows: Record<string, unknown>[] = [];
    const distinctValues: Record<string, string[]> = {};
    let rowCount = 0;

    try {
      const sampleRes = await databricks.executeStatement(
        `SELECT ${colList} FROM ${fq(tn)} LIMIT 5`,
      );
      sampleRows = sampleRes.data;
    } catch (err) {
      onProgress?.(
        `  warn: sample failed for ${cfg.id}.${tn}: ${(err as Error).message.slice(0, 80)}`,
      );
    }

    try {
      const countRes = await databricks.executeStatement(
        `SELECT COUNT(*) AS n FROM ${fq(tn)}`,
      );
      const raw = countRes.data[0]?.n;
      rowCount = typeof raw === "number" ? raw : Number(raw ?? 0);
    } catch {
      rowCount = 0;
    }

    if (rowCount <= DISTINCT_SAMPLING_ROW_CAP) {
      for (const col of safeColumns) {
        if (!isStringLike(col.data_type)) continue;
        try {
          const distRes = await databricks.executeStatement(
            `SELECT DISTINCT ${quoteIdent(col.column_name)} AS v FROM ${fq(tn)} WHERE ${quoteIdent(col.column_name)} IS NOT NULL LIMIT 30`,
          );
          const vals = distRes.data
            .map((r) => (r.v === null || r.v === undefined ? null : String(r.v)))
            .filter((v): v is string => v !== null && v.length < 200);
          if (vals.length > 0) distinctValues[col.column_name] = vals;
        } catch {
          // Skip distinct sampling failures
        }
      }
    } else {
      onProgress?.(
        `      skipping DISTINCT sampling for ${tn} (${rowCount.toLocaleString()} rows > ${DISTINCT_SAMPLING_ROW_CAP.toLocaleString()} cap)`,
      );
    }

    samples.push({
      schema_id: cfg.id,
      table_name: tn,
      sample_rows: sampleRows,
      distinct_values: distinctValues,
      row_count: rowCount,
      sampled_columns: safeColumns.map((c) => c.column_name),
      skipped_columns: skippedColumns,
    });
  }

  writeCache(cachePath, samples);
  return samples;
}

function isStringLike(dataType: string): boolean {
  const t = dataType.toUpperCase();
  return t.includes("STRING") || t.includes("VARCHAR") || t === "CHAR";
}
