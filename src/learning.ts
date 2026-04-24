import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import type {
  Correction,
  FeedbackEntry,
  ValidatedQuery,
} from "./types.js";

const LEARNING_DIR = resolve("learning");
const CORRECTIONS = resolve(LEARNING_DIR, "corrections.json");
const VALIDATED = resolve(LEARNING_DIR, "validated-queries.json");
const FEEDBACK = resolve(LEARNING_DIR, "feedback-log.json");

ensureDir(LEARNING_DIR);
ensureFile(CORRECTIONS, []);
ensureFile(VALIDATED, []);
ensureFile(FEEDBACK, []);

function ensureDir(path: string): void {
  if (!existsSync(path)) mkdirSync(path, { recursive: true });
}

function ensureFile(path: string, defaultValue: unknown): void {
  ensureDir(dirname(path));
  if (!existsSync(path)) {
    writeFileSync(path, JSON.stringify(defaultValue, null, 2));
  }
}

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

function writeJson(path: string, data: unknown): void {
  writeFileSync(path, JSON.stringify(data, null, 2));
}

export function loadCorrections(): Correction[] {
  const raw = readJson<unknown[]>(CORRECTIONS);
  // Migrate any legacy entries missing schema_id to a placeholder
  return raw.map((entry) => {
    const e = entry as Partial<Correction> & Record<string, unknown>;
    return {
      schema_id: e.schema_id ?? "*",
      table: String(e.table ?? "*"),
      rule: String(e.rule ?? ""),
      added_at: String(e.added_at ?? new Date().toISOString()),
    };
  });
}

export function addCorrection(schemaId: string, table: string, rule: string): Correction {
  const entry: Correction = {
    schema_id: schemaId,
    table,
    rule,
    added_at: new Date().toISOString(),
  };
  const all = loadCorrections();
  all.push(entry);
  writeJson(CORRECTIONS, all);
  return entry;
}

export function loadValidatedQueries(): ValidatedQuery[] {
  const raw = readJson<unknown[]>(VALIDATED);
  return raw.map((entry) => {
    const e = entry as Partial<ValidatedQuery> & Record<string, unknown>;
    return {
      id: String(e.id ?? randomUUID()),
      question: String(e.question ?? ""),
      sql: String(e.sql ?? ""),
      schemas_used: Array.isArray(e.schemas_used) ? e.schemas_used.map(String) : [],
      tables_used: Array.isArray(e.tables_used) ? e.tables_used.map(String) : [],
      description: String(e.description ?? ""),
      validated_at: String(e.validated_at ?? new Date().toISOString()),
      use_count: typeof e.use_count === "number" ? e.use_count : 0,
    };
  });
}

export function addValidatedQuery(
  question: string,
  sql: string,
  schemas_used: string[],
  tables_used: string[],
  description: string,
): ValidatedQuery {
  const entry: ValidatedQuery = {
    id: randomUUID(),
    question,
    sql,
    schemas_used,
    tables_used,
    description,
    validated_at: new Date().toISOString(),
    use_count: 0,
  };
  const all = loadValidatedQueries();
  all.push(entry);
  writeJson(VALIDATED, all);
  return entry;
}

export function incrementValidatedUse(id: string): void {
  const all = loadValidatedQueries();
  const q = all.find((v) => v.id === id);
  if (q) {
    q.use_count++;
    writeJson(VALIDATED, all);
  }
}

export function loadFeedbackLog(): FeedbackEntry[] {
  return readJson<FeedbackEntry[]>(FEEDBACK);
}

export function logFeedback(entry: FeedbackEntry): void {
  const all = loadFeedbackLog();
  all.push(entry);
  writeJson(FEEDBACK, all);
}

export function updateLastFeedback(
  rating: "good" | "bad" | "corrected",
  note?: string,
): boolean {
  const all = loadFeedbackLog();
  if (all.length === 0) return false;
  const last = all[all.length - 1]!;
  last.feedback = rating;
  if (note) last.correction_note = note;
  writeJson(FEEDBACK, all);
  return true;
}

/**
 * Parses a user-typed correction like:
 *   "correct: rule text"                     → inferred schema + table (if mentioned)
 *   "correct <schema>: rule text"            → schema-scoped, wildcard table
 *   "correct <schema>.<table>: rule text"    → schema + table explicit
 * Falls back to the supplied default schema id when none is mentioned.
 */
export function parseCorrection(
  input: string,
  knownTablesBySchema: Record<string, string[]>,
  defaultSchemaId: string,
): { schema_id: string; table: string; rule: string } | null {
  const trimmed = input.replace(/^correct:?\s*/i, "").trim();
  if (!trimmed) return null;

  const knownSchemas = Object.keys(knownTablesBySchema);

  // "correct <schema.table>: rule"
  const colonMatch = trimmed.match(/^(\S+)\s*:\s*(.+)$/);
  if (colonMatch) {
    const head = colonMatch[1]!;
    const rule = colonMatch[2]!.trim();
    const parts = head.split(".");
    if (parts.length === 2 && knownSchemas.includes(parts[0]!)) {
      const schemaId = parts[0]!;
      const table = parts[1]!;
      const tables = knownTablesBySchema[schemaId] ?? [];
      if (tables.includes(table)) return { schema_id: schemaId, table, rule };
    }
    if (parts.length === 1 && knownSchemas.includes(parts[0]!)) {
      return { schema_id: parts[0]!, table: "*", rule };
    }
    for (const [sid, tables] of Object.entries(knownTablesBySchema)) {
      if (tables.includes(head)) return { schema_id: sid, table: head, rule };
    }
  }

  // Try to infer a table mention in the body
  for (const [sid, tables] of Object.entries(knownTablesBySchema)) {
    for (const table of tables) {
      if (new RegExp(`\\b${table}\\b`, "i").test(trimmed)) {
        return { schema_id: sid, table, rule: trimmed };
      }
    }
  }

  return { schema_id: defaultSchemaId, table: "*", rule: trimmed };
}

export interface FeedbackStats {
  total: number;
  good: number;
  bad: number;
  corrected: number;
  unrated: number;
  accuracy_pct: number;
}

export function getFeedbackStats(): FeedbackStats {
  const all = loadFeedbackLog();
  const good = all.filter((f) => f.feedback === "good").length;
  const bad = all.filter((f) => f.feedback === "bad").length;
  const corrected = all.filter((f) => f.feedback === "corrected").length;
  const unrated = all.filter((f) => f.feedback === "unrated").length;
  const rated = good + bad + corrected;
  const accuracy_pct = rated === 0 ? 0 : Math.round((good / rated) * 100);
  return { total: all.length, good, bad, corrected, unrated, accuracy_pct };
}
