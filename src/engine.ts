import { databricks } from "./databricks.js";
import type { GeneratedSQL, QueryResult } from "./types.js";

const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "DROP",
  "ALTER",
  "CREATE",
  "TRUNCATE",
  "MERGE",
  "GRANT",
  "REVOKE",
  "REPLACE",
];

export async function executeQuery(sql: GeneratedSQL): Promise<QueryResult> {
  const safe = enforceSafety(sql.sql);
  return databricks.executeStatement(safe);
}

function enforceSafety(rawSql: string): string {
  const trimmed = rawSql.trim().replace(/;\s*$/, "");
  const upper = trimmed.toUpperCase();

  if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) {
    throw new Error(
      `Query rejected: must start with SELECT or WITH, got: ${trimmed.slice(0, 40)}...`,
    );
  }

  for (const kw of FORBIDDEN_KEYWORDS) {
    const pattern = new RegExp(`\\b${kw}\\b`, "i");
    if (pattern.test(trimmed)) {
      throw new Error(`Query rejected: contains forbidden keyword "${kw}"`);
    }
  }

  if (!/\bLIMIT\s+\d+/i.test(trimmed)) {
    return `${trimmed}\nLIMIT 10000`;
  }

  return trimmed;
}

export function formatResultsAsTable(result: QueryResult, maxRows = 15): string {
  if (result.data.length === 0) return "(no rows returned)";

  const cols = result.columns;
  const rows = result.data.slice(0, maxRows).map((r) =>
    cols.map((c) => stringifyCell(r[c])),
  );

  const widths = cols.map((c, i) => {
    const colValues = [c, ...rows.map((r) => r[i])];
    return Math.min(30, Math.max(...colValues.map((v) => v.length)));
  });

  const numericCols = new Set<number>();
  cols.forEach((_, i) => {
    const vals = rows.map((r) => r[i]).filter((v) => v && v !== "null");
    if (vals.length > 0 && vals.every((v) => /^-?\d+(\.\d+)?$/.test(v))) {
      numericCols.add(i);
    }
  });

  const fmt = (cells: string[]): string =>
    cells
      .map((v, i) => {
        const w = widths[i]!;
        const truncated = v.length > w ? v.slice(0, w - 1) + "…" : v;
        return numericCols.has(i) ? truncated.padStart(w) : truncated.padEnd(w);
      })
      .join(" │ ");

  const sep = widths.map((w) => "─".repeat(w)).join("─┼─");

  const lines = [fmt(cols), sep, ...rows.map(fmt)];

  if (result.row_count > maxRows) {
    lines.push("", `(showing ${maxRows} of ${result.row_count} rows)`);
  }

  return lines.join("\n");
}

export function formatResultsForClaude(result: QueryResult, maxRows = 25): string {
  if (result.data.length === 0) return "No rows returned.";

  const cols = result.columns;
  const header = cols.join(" | ");
  const rows = result.data.slice(0, maxRows).map((r) =>
    cols.map((c) => stringifyCell(r[c])).join(" | "),
  );

  const footer =
    result.row_count > maxRows
      ? `\n(Showing ${maxRows} of ${result.row_count} total rows.)`
      : "";

  return [header, "-".repeat(header.length), ...rows].join("\n") + footer;
}

function stringifyCell(v: unknown): string {
  if (v === null || v === undefined) return "null";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
