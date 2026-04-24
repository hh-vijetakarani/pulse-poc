import type {
  FleetKnowledgeGraph,
  FunnelDefinition,
  KnowledgeGraph,
  Relationship,
  TableKnowledge,
} from "./types.js";

/**
 * Normalizes a freshly-assembled KnowledgeGraph so downstream code can rely
 * on the shape. Guards against LLM output variance — missing arrays, partial
 * funnel definitions, nulled-out fields, etc. Returns a second-pass KG with
 * the same schema_id and a parallel list of warnings about what was fixed.
 */
export interface NormalizationResult {
  kg: KnowledgeGraph;
  warnings: string[];
}

export function normalizeKg(kg: KnowledgeGraph): NormalizationResult {
  const warnings: string[] = [];
  const prefix = `[${kg.schema_id}]`;

  const tables: TableKnowledge[] = (kg.tables ?? []).map((t, idx) => {
    const tn = t.table_name ?? `(unknown_table_${idx})`;
    if (!t.table_name) warnings.push(`${prefix} table[${idx}] missing table_name`);
    return {
      schema_id: t.schema_id ?? kg.schema_id,
      catalog: t.catalog ?? kg.catalog,
      schema: t.schema ?? kg.schema,
      table_name: tn,
      purpose: t.purpose ?? "",
      domain: t.domain ?? "other",
      key_columns: ensureArray(t.key_columns).filter((k) => k && typeof k.column === "string"),
      join_keys: ensureArray(t.join_keys).filter(
        (j) => j && typeof j.column === "string" && typeof j.joins_to === "string",
      ),
      default_filters: ensureArray(t.default_filters).filter(
        (f) => f && typeof f.column === "string",
      ),
      enum_columns: ensureArray(t.enum_columns)
        .filter((ec) => ec && typeof ec.column === "string")
        .map((ec) => ({
          column: ec.column,
          enum_name: ec.enum_name ?? "",
          values: ensureArray(ec.values).filter((v): v is string => typeof v === "string"),
        })),
      business_rules: ensureArray(t.business_rules).filter(
        (r): r is string => typeof r === "string",
      ),
      row_count: typeof t.row_count === "number" ? t.row_count : 0,
      all_columns: ensureArray(t.all_columns),
      proto_message: t.proto_message ?? undefined,
      dbt_model: t.dbt_model ?? undefined,
    };
  });

  const relationships: Relationship[] = ensureArray(kg.relationships)
    .filter(
      (r) =>
        r &&
        typeof r.from_table === "string" &&
        typeof r.to_table === "string" &&
        typeof r.join_key === "string",
    )
    .map((r) => ({
      schema_id: r.schema_id ?? kg.schema_id,
      from_table: r.from_table,
      to_table: r.to_table,
      join_key: r.join_key,
      type: r.type ?? "many_to_one",
      source: r.source ?? "inferred",
    }));

  const funnel = normalizeFunnel(kg.funnel, kg.schema_id, warnings);

  const derivable_metrics = ensureArray(kg.derivable_metrics).map((m) => ({
    schema_id: m.schema_id ?? kg.schema_id,
    name: m.name ?? "",
    description: m.description ?? "",
    tables_needed: ensureArray(m.tables_needed).filter((s): s is string => typeof s === "string"),
    sql_pattern: m.sql_pattern ?? "",
  }));

  const sample_questions = ensureArray(kg.sample_questions).filter(
    (q): q is string => typeof q === "string",
  );

  return {
    kg: {
      schema_id: kg.schema_id,
      catalog: kg.catalog,
      schema: kg.schema,
      generated_at: kg.generated_at ?? new Date().toISOString(),
      tables,
      relationships,
      funnel,
      derivable_metrics,
      sample_questions,
      proto_enriched: !!kg.proto_enriched,
      dbt_enriched: !!kg.dbt_enriched,
    },
    warnings,
  };
}

function normalizeFunnel(
  raw: FunnelDefinition | null | undefined,
  schema_id: string,
  warnings: string[],
): FunnelDefinition | null {
  if (!raw) return null;
  if (!raw.name || typeof raw.name !== "string") {
    warnings.push(`[${schema_id}] funnel present but missing name — discarding`);
    return null;
  }
  const stages = ensureArray(raw.stages).filter(
    (s) => s && typeof s.name === "string" && typeof s.table === "string",
  );
  if (stages.length === 0) {
    warnings.push(
      `[${schema_id}] funnel "${raw.name}" has no valid stages — discarding funnel`,
    );
    return null;
  }
  return {
    schema_id: raw.schema_id ?? schema_id,
    name: raw.name,
    stages,
  };
}

function ensureArray<T>(v: T[] | null | undefined): T[] {
  if (Array.isArray(v)) return v;
  return [];
}

export function normalizeFleet(fleet: FleetKnowledgeGraph): {
  fleet: FleetKnowledgeGraph;
  warnings: string[];
} {
  const warnings: string[] = [];
  const schemas = (fleet.schemas ?? []).map((kg) => {
    const r = normalizeKg(kg);
    warnings.push(...r.warnings);
    return r.kg;
  });
  return {
    fleet: { generated_at: fleet.generated_at, schemas },
    warnings,
  };
}

/**
 * Lint a normalized KG for things that are shape-valid but unusual, and worth
 * surfacing to the operator. Non-fatal — just returns advisory notes.
 */
export function lintKg(kg: KnowledgeGraph): string[] {
  const notes: string[] = [];
  const prefix = `[${kg.schema_id}]`;

  if (kg.tables.length === 0) notes.push(`${prefix} KG has 0 tables — schema may be empty`);
  const tablesNoCols = kg.tables.filter((t) => t.all_columns.length === 0);
  if (tablesNoCols.length > 0) {
    notes.push(
      `${prefix} ${tablesNoCols.length} table(s) with 0 columns: ${tablesNoCols.slice(0, 3).map((t) => t.table_name).join(", ")}`,
    );
  }

  const tablesWithEnumsButNoValues = kg.tables.filter((t) =>
    t.enum_columns.some((ec) => ec.values.length === 0),
  );
  if (tablesWithEnumsButNoValues.length > 0) {
    notes.push(
      `${prefix} ${tablesWithEnumsButNoValues.length} table(s) have enum_columns with no values — SQL gen may guess wrong filters`,
    );
  }

  const orphanRelations = kg.relationships.filter((r) => {
    const tableNames = new Set(kg.tables.map((t) => t.table_name));
    return !tableNames.has(r.from_table) || !tableNames.has(r.to_table);
  });
  if (orphanRelations.length > 0) {
    notes.push(
      `${prefix} ${orphanRelations.length} relationship(s) reference tables not in this schema`,
    );
  }

  return notes;
}
