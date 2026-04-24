import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";
import type { SchemaConfig } from "./config.js";
import type {
  ProtoContext,
  ProtoEnum,
  ProtoField,
  ProtoMessage,
  ProtoService,
  TableSchema,
} from "./types.js";

function schemaCacheFile(schemaId: string): string {
  return resolve("cache", schemaId, "proto-context.json");
}

function ensureParent(path: string): void {
  const dir = resolve(path, "..");
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

function findProtoFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  const out: string[] = [];
  const walk = (d: string): void => {
    for (const entry of readdirSync(d)) {
      const full = join(d, entry);
      const st = statSync(full);
      if (st.isDirectory()) walk(full);
      else if (entry.endsWith(".proto")) out.push(full);
    }
  };
  walk(dir);
  return out.sort();
}

function isCacheValid(cacheFile: string, files: string[]): boolean {
  if (!existsSync(cacheFile)) return false;
  const cacheMtime = statSync(cacheFile).mtimeMs;
  for (const f of files) {
    if (statSync(f).mtimeMs > cacheMtime) return false;
  }
  return true;
}

export async function parseProtoFiles(cfg: SchemaConfig): Promise<ProtoContext> {
  if (!cfg.protos) return emptyContext();
  const protosDir = resolve(cfg.protos);
  const files = findProtoFiles(protosDir);
  if (files.length === 0) return emptyContext();

  const cacheFile = schemaCacheFile(cfg.id);
  if (isCacheValid(cacheFile, files)) {
    try {
      return JSON.parse(readFileSync(cacheFile, "utf-8")) as ProtoContext;
    } catch {
      // rebuild
    }
  }

  const messages: ProtoMessage[] = [];
  const enums: ProtoEnum[] = [];
  const services: ProtoService[] = [];

  for (const file of files) {
    const source = readFileSync(file, "utf-8");
    const pkg = extractPackage(source);
    const parsed = parseProtoSource(source, pkg);
    messages.push(...parsed.messages);
    enums.push(...parsed.enums);
    services.push(...parsed.services);
  }

  const ctx: ProtoContext = {
    messages,
    enums,
    services,
    source_files: files.map((f) => f.replace(protosDir + "/", cfg.protos + "/")),
  };

  ensureParent(cacheFile);
  writeFileSync(cacheFile, JSON.stringify(ctx, null, 2));
  return ctx;
}

function emptyContext(): ProtoContext {
  return { messages: [], enums: [], services: [], source_files: [] };
}

function extractPackage(source: string): string {
  const m = source.match(/^\s*package\s+([\w.]+)\s*;/m);
  return m?.[1] ?? "";
}

interface ParsedFile {
  messages: ProtoMessage[];
  enums: ProtoEnum[];
  services: ProtoService[];
}

function parseProtoSource(source: string, pkg: string): ParsedFile {
  const lines = source.split("\n");
  const messages: ProtoMessage[] = [];
  const enums: ProtoEnum[] = [];
  const services: ProtoService[] = [];

  const stack: Array<{ kind: "message" | "enum" | "service" | "oneof"; ref: unknown; name: string }> = [];
  let pendingComment = "";

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i]!;
    const line = raw.trim();

    if (!line) continue;

    if (line.startsWith("//")) {
      const text = line.slice(2).trim();
      pendingComment = pendingComment ? `${pendingComment} ${text}` : text;
      continue;
    }

    if (line.startsWith("/*")) {
      let block = line.slice(2);
      let j = i;
      while (!block.includes("*/") && j < lines.length - 1) {
        j++;
        block += "\n" + lines[j];
      }
      const cleaned = block.replace(/\*\/$/, "").replace(/^\s*\*\s?/gm, "").trim();
      pendingComment = pendingComment ? `${pendingComment} ${cleaned}` : cleaned;
      i = j;
      continue;
    }

    if (/^(syntax|package|import|option)\b/.test(line)) {
      pendingComment = "";
      continue;
    }

    const msgMatch = line.match(/^message\s+(\w+)\s*\{/);
    if (msgMatch) {
      const name = msgMatch[1]!;
      const full_name = pkg ? `${pkg}.${name}` : name;
      const msg: ProtoMessage = {
        name,
        full_name,
        comment: pendingComment || undefined,
        fields: [],
      };
      messages.push(msg);
      stack.push({ kind: "message", ref: msg, name });
      pendingComment = "";
      continue;
    }

    const enumMatch = line.match(/^enum\s+(\w+)\s*\{/);
    if (enumMatch) {
      const name = enumMatch[1]!;
      const full_name = pkg ? `${pkg}.${name}` : name;
      const en: ProtoEnum = {
        name,
        full_name,
        comment: pendingComment || undefined,
        values: [],
      };
      enums.push(en);
      stack.push({ kind: "enum", ref: en, name });
      pendingComment = "";
      continue;
    }

    const svcMatch = line.match(/^service\s+(\w+)\s*\{/);
    if (svcMatch) {
      const svc: ProtoService = { name: svcMatch[1]!, methods: [] };
      services.push(svc);
      stack.push({ kind: "service", ref: svc, name: svcMatch[1]! });
      pendingComment = "";
      continue;
    }

    const oneofMatch = line.match(/^oneof\s+(\w+)\s*\{/);
    if (oneofMatch) {
      stack.push({ kind: "oneof", ref: null, name: oneofMatch[1]! });
      pendingComment = "";
      continue;
    }

    if (line === "}" || line.startsWith("}")) {
      stack.pop();
      pendingComment = "";
      continue;
    }

    const top = stack[stack.length - 1];
    if (!top) {
      pendingComment = "";
      continue;
    }

    const containingMessage = [...stack].reverse().find((s) => s.kind === "message");
    const containingEnum = [...stack].reverse().find((s) => s.kind === "enum");

    if (containingMessage && top.kind !== "enum") {
      const field = parseFieldLine(line);
      if (field) {
        if (pendingComment) field.comment = pendingComment;
        (containingMessage.ref as ProtoMessage).fields.push(field);
        pendingComment = "";
        continue;
      }
    }

    if (containingEnum && top.kind === "enum") {
      const enumVal = line.match(/^(\w+)\s*=\s*(-?\d+)\s*(?:\[[^\]]*\])?\s*;/);
      if (enumVal) {
        (containingEnum.ref as ProtoEnum).values.push({
          name: enumVal[1]!,
          number: parseInt(enumVal[2]!, 10),
          comment: pendingComment || undefined,
        });
        pendingComment = "";
        continue;
      }
    }

    if (top.kind === "service") {
      const rpc = line.match(/^rpc\s+(\w+)\s*\(\s*([\w.]+)\s*\)\s+returns\s*\(\s*([\w.]+)\s*\)/);
      if (rpc) {
        (top.ref as ProtoService).methods.push({
          name: rpc[1]!,
          input_type: rpc[2]!,
          output_type: rpc[3]!,
          comment: pendingComment || undefined,
        });
        pendingComment = "";
        continue;
      }
    }

    pendingComment = "";
  }

  return { messages, enums, services };
}

function parseFieldLine(line: string): ProtoField | null {
  const fieldRe =
    /^(?:(repeated|optional)\s+)?([\w.]+)\s+(\w+)\s*=\s*\d+(?:\s*\[[^\]]*\])?\s*;/;
  const m = line.match(fieldRe);
  if (!m) return null;
  const modifier = m[1];
  const type = m[2]!;
  const name = m[3]!;
  return {
    name,
    type,
    is_repeated: modifier === "repeated",
    is_optional: modifier === "optional",
    enum_ref: undefined,
  };
}

export function matchProtoToTables(
  proto: ProtoContext,
  tables: TableSchema[],
): ProtoContext {
  const enumFullNames = new Set(proto.enums.map((e) => e.full_name));
  const enumShortNames = new Map(proto.enums.map((e) => [e.name, e.full_name]));

  for (const msg of proto.messages) {
    for (const field of msg.fields) {
      if (enumShortNames.has(field.type) || enumFullNames.has(field.type)) {
        field.enum_ref = enumShortNames.get(field.type) ?? field.type;
      }
    }
  }

  for (const msg of proto.messages) {
    const candidates = tables.map((t) => ({
      table: t,
      score: computeMatchScore(msg, t),
    }));
    candidates.sort((a, b) => b.score - a.score);
    const best = candidates[0];
    if (best && best.score >= 0.4) {
      msg.likely_table_match = best.table.table_name;
      msg.match_score = Math.round(best.score * 100) / 100;
    }
  }

  return proto;
}

function computeMatchScore(msg: ProtoMessage, table: TableSchema): number {
  const msgNorm = toSnake(msg.name);
  const tblNorm = table.table_name.toLowerCase();

  let nameScore = 0;
  if (tblNorm === msgNorm || tblNorm === msgNorm + "s") nameScore = 1;
  else if (tblNorm.includes(msgNorm) || msgNorm.includes(tblNorm)) nameScore = 0.6;
  else {
    const msgRoot = msgNorm.replace(/_properties$/, "");
    if (tblNorm === msgRoot || tblNorm === msgRoot + "s") nameScore = 0.9;
    else if (tblNorm.includes(msgRoot)) nameScore = 0.4;
  }

  const fieldNames = new Set(msg.fields.map((f) => f.name.toLowerCase()));
  const colNames = new Set(table.columns.map((c) => c.column_name.toLowerCase()));
  let overlap = 0;
  for (const f of fieldNames) if (colNames.has(f)) overlap++;
  const overlapScore =
    fieldNames.size === 0 ? 0 : overlap / Math.min(fieldNames.size, colNames.size);

  return nameScore * 0.6 + overlapScore * 0.4;
}

function toSnake(s: string): string {
  return s
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .toLowerCase();
}

const MAX_FIELDS_SHOWN = 20;

export function formatProtoForClaude(proto: ProtoContext): string {
  if (proto.messages.length === 0 && proto.enums.length === 0) return "";

  const parts: string[] = [];
  parts.push("=== PROTOBUF DEFINITIONS (source-of-truth data contracts) ===");
  parts.push(`Source files: ${proto.source_files.join(", ")}`);
  parts.push("");

  for (const en of proto.enums) {
    parts.push(`Enum: ${en.name}`);
    if (en.comment) parts.push(`  // ${en.comment}`);
    const vals = en.values
      .map((v) => {
        const c = v.comment ? ` — ${v.comment}` : "";
        return `  ${v.name} = ${v.number}${c}`;
      })
      .join("\n");
    parts.push(vals);
    parts.push("");
  }

  for (const msg of proto.messages) {
    const matchStr = msg.likely_table_match
      ? ` → table \`${msg.likely_table_match}\` (match: ${(msg.match_score ?? 0) * 100}%)`
      : "";
    parts.push(`Message: ${msg.name}${matchStr}`);
    if (msg.comment) parts.push(`  // ${msg.comment}`);
    const shown = msg.fields.slice(0, MAX_FIELDS_SHOWN);
    for (const f of shown) {
      const mod = f.is_repeated ? "[repeated] " : f.is_optional ? "[optional] " : "";
      const enumHint = f.enum_ref ? ` [enum=${f.enum_ref}]` : "";
      const cmt = f.comment ? ` — ${f.comment}` : "";
      parts.push(`  ${mod}${f.type} ${f.name}${enumHint}${cmt}`);
    }
    if (msg.fields.length > MAX_FIELDS_SHOWN) {
      parts.push(`  ...and ${msg.fields.length - MAX_FIELDS_SHOWN} more fields`);
    }
    parts.push("");
  }

  return parts.join("\n").slice(0, 12000);
}
