import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import yaml from "js-yaml";

export interface SchemaConfig {
  id: string;
  catalog: string;
  schema: string;
  aliases: string[];
  protos: string | null;
  dbt: string | null;
  phi_skip_patterns: string[];
  distinct_sampling_row_cap: number;
  notes_doc: string | null;
}

export interface SchemaOverride {
  aliases?: string[];
  protos?: string | null;
  dbt?: string | null;
  phi_skip_patterns?: string[];
  phi_skip_patterns_add?: string[];
  /**
   * Tables with more rows than this skip DISTINCT sampling during discovery.
   * Default is 1_000_000 — fine for entity tables, too low for event streams
   * (e.g. mixpanel_*) where you need distinct event names from large tables.
   * Set to a very large number (or Number.MAX_SAFE_INTEGER) to effectively
   * disable the cap.
   */
  distinct_sampling_row_cap?: number;
  /**
   * Path to a markdown "topic doc" with domain-specific context for this
   * schema (funnel definitions, event taxonomy, business rules). Loaded
   * during KG build as additional context.
   */
  notes_doc?: string | null;
}

export interface AutoDiscoverSpec {
  catalogs: string[];
  include: string[];
  exclude: string[];
}

export interface FleetConfig {
  schemas: SchemaConfig[];
  active_ids: string[];
  active_spec: string;
  large_fleet_threshold: number;
  source: "yaml" | "env";
  auto_discover: AutoDiscoverSpec | null;
  baseline_phi: string[];
  baseline_overrides: SchemaOverride;
  baseline_distinct_sampling_row_cap: number;
  overrides: Record<string, SchemaOverride>;
}

interface RawFleetYaml {
  defaults?: {
    active?: string;
    large_fleet_threshold?: number;
    phi_skip_patterns?: string[];
    phi_skip_patterns_add?: string[];
    protos?: string | null;
    dbt?: string | null;
    distinct_sampling_row_cap?: number;
  };
  auto_discover?: {
    catalogs?: string[];
    include?: string[];
    exclude?: string[];
  };
  overrides?: Record<string, SchemaOverride>;
  schemas?: Array<{
    id: string;
    catalog: string;
    schema: string;
    aliases?: string[];
    protos?: string | null;
    dbt?: string | null;
    phi_skip_patterns?: string[];
    phi_skip_patterns_add?: string[];
    distinct_sampling_row_cap?: number;
    notes_doc?: string | null;
  }>;
}

const DEFAULT_DISTINCT_SAMPLING_ROW_CAP = 1_000_000;

const DEFAULT_PHI = [
  "name",
  "first_name",
  "last_name",
  "email",
  "phone",
  "ssn",
  "dob",
  "date_of_birth",
  "address",
  "street",
  "city",
];

function normalizePatterns(list: string[]): string[] {
  return [
    ...new Set(
      list
        .map((s) => (typeof s === "string" ? s.trim().toLowerCase() : ""))
        .filter(Boolean),
    ),
  ];
}

// Convert a glob (supporting * and ?) into an anchored regex.
export function globToRegex(glob: string): RegExp {
  const escaped = glob.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const pattern = escaped.replace(/\*/g, ".*").replace(/\?/g, ".");
  return new RegExp(`^${pattern}$`, "i");
}

export function matchesAny(name: string, patterns: string[]): boolean {
  if (patterns.length === 0) return false;
  return patterns.some((p) => globToRegex(p).test(name));
}

export function loadFleetConfig(): FleetConfig {
  const configPath = resolve("schemas.yaml");
  if (existsSync(configPath)) {
    const text = readFileSync(configPath, "utf-8");
    const raw = yaml.load(text) as RawFleetYaml;
    return buildFromYaml(raw);
  }
  return buildFromEnv();
}

function buildFromYaml(raw: RawFleetYaml): FleetConfig {
  const defaults = raw.defaults ?? {};
  const baselinePhi = normalizePatterns(defaults.phi_skip_patterns ?? DEFAULT_PHI);
  const baselineOverrides: SchemaOverride = {
    phi_skip_patterns_add: defaults.phi_skip_patterns_add,
    protos: defaults.protos ?? null,
    dbt: defaults.dbt ?? null,
  };
  const baselineDistinctCap =
    defaults.distinct_sampling_row_cap ?? DEFAULT_DISTINCT_SAMPLING_ROW_CAP;
  const largeFleetThreshold = defaults.large_fleet_threshold ?? 10;
  const activeSpec = (defaults.active ?? "all").trim();
  const overrides = raw.overrides ?? {};

  let autoDiscover: AutoDiscoverSpec | null = null;
  if (raw.auto_discover) {
    autoDiscover = {
      catalogs: raw.auto_discover.catalogs ?? ["prod"],
      include: raw.auto_discover.include ?? ["*"],
      exclude: raw.auto_discover.exclude ?? [],
    };
  }

  const schemas: SchemaConfig[] = (raw.schemas ?? []).map((s) =>
    materializeFromExplicit(s, baselinePhi, baselineOverrides, baselineDistinctCap),
  );

  // Active IDs can only be resolved for explicit schemas now. Auto-discovered ones
  // are resolved after the expansion step (see expandFleetConfig).
  const active_ids = schemas.length
    ? resolveActiveIds(activeSpec, schemas)
    : [];

  return {
    schemas,
    active_ids,
    active_spec: activeSpec,
    large_fleet_threshold: largeFleetThreshold,
    source: "yaml",
    auto_discover: autoDiscover,
    baseline_phi: baselinePhi,
    baseline_overrides: baselineOverrides,
    baseline_distinct_sampling_row_cap: baselineDistinctCap,
    overrides,
  };
}

function materializeFromExplicit(
  entry: NonNullable<RawFleetYaml["schemas"]>[number],
  baselinePhi: string[],
  baselineOverrides: SchemaOverride,
  baselineDistinctCap: number,
): SchemaConfig {
  if (!entry.id || !entry.catalog || !entry.schema) {
    throw new Error(`Schema entry missing required fields: ${JSON.stringify(entry)}`);
  }
  let phi: string[];
  if (entry.phi_skip_patterns) phi = normalizePatterns(entry.phi_skip_patterns);
  else if (entry.phi_skip_patterns_add)
    phi = normalizePatterns([...baselinePhi, ...entry.phi_skip_patterns_add]);
  else if (baselineOverrides.phi_skip_patterns_add)
    phi = normalizePatterns([...baselinePhi, ...baselineOverrides.phi_skip_patterns_add]);
  else phi = baselinePhi;

  return {
    id: entry.id,
    catalog: entry.catalog,
    schema: entry.schema,
    aliases: entry.aliases ?? [],
    protos: entry.protos ?? baselineOverrides.protos ?? null,
    dbt: entry.dbt ?? baselineOverrides.dbt ?? null,
    phi_skip_patterns: phi,
    distinct_sampling_row_cap: entry.distinct_sampling_row_cap ?? baselineDistinctCap,
    notes_doc: entry.notes_doc ?? null,
  };
}

function materializeFromDiscovery(
  catalog: string,
  schemaName: string,
  cfg: FleetConfig,
): SchemaConfig {
  const id = schemaName;
  const override = cfg.overrides[id] ?? {};

  let phi: string[];
  if (override.phi_skip_patterns) phi = normalizePatterns(override.phi_skip_patterns);
  else if (override.phi_skip_patterns_add)
    phi = normalizePatterns([...cfg.baseline_phi, ...override.phi_skip_patterns_add]);
  else if (cfg.baseline_overrides.phi_skip_patterns_add)
    phi = normalizePatterns([
      ...cfg.baseline_phi,
      ...cfg.baseline_overrides.phi_skip_patterns_add,
    ]);
  else phi = cfg.baseline_phi;

  return {
    id,
    catalog,
    schema: schemaName,
    aliases: override.aliases ?? [],
    protos: override.protos ?? cfg.baseline_overrides.protos ?? null,
    dbt: override.dbt ?? cfg.baseline_overrides.dbt ?? null,
    phi_skip_patterns: phi,
    distinct_sampling_row_cap:
      override.distinct_sampling_row_cap ?? cfg.baseline_distinct_sampling_row_cap,
    notes_doc: override.notes_doc ?? null,
  };
}

function buildFromEnv(): FleetConfig {
  const catalog = process.env.DATABRICKS_CATALOG ?? "prod";
  const schema = process.env.DATABRICKS_SCHEMA ?? "hs_graph";
  const phiEnv = process.env.PHI_SKIP_PATTERNS;
  const phi = phiEnv ? normalizePatterns(phiEnv.split(",")) : [...DEFAULT_PHI];

  const protos = existsSync(resolve("protos")) ? "protos/" : null;

  const one: SchemaConfig = {
    id: schema,
    catalog,
    schema,
    aliases: [],
    protos,
    dbt: null,
    phi_skip_patterns: phi,
    distinct_sampling_row_cap: DEFAULT_DISTINCT_SAMPLING_ROW_CAP,
    notes_doc: null,
  };

  return {
    schemas: [one],
    active_ids: [one.id],
    active_spec: one.id,
    large_fleet_threshold: 10,
    source: "env",
    auto_discover: null,
    baseline_phi: phi,
    baseline_overrides: {},
    baseline_distinct_sampling_row_cap: DEFAULT_DISTINCT_SAMPLING_ROW_CAP,
    overrides: {},
  };
}

export function resolveActiveIds(spec: string, schemas: SchemaConfig[]): string[] {
  const allIds = schemas.map((s) => s.id);
  if (!spec || spec === "all") return allIds;

  // Split by comma first — each part can independently be a glob, a
  // catalog-wildcard ("prod.*"), a schema id, or an alias.
  const parts = spec.split(",").map((s) => s.trim()).filter(Boolean);
  const resolved: string[] = [];

  for (const part of parts) {
    // catalog glob: "prod.*"
    if (part.endsWith(".*") && !part.includes("*", 0)) {
      // only treat as catalog glob if the "*" is the final segment
    }
    if (part.endsWith(".*") && part.indexOf("*") === part.length - 1) {
      const catalog = part.slice(0, -2);
      for (const s of schemas) {
        if (s.catalog === catalog && !resolved.includes(s.id)) resolved.push(s.id);
      }
      continue;
    }

    // schema-id glob (e.g. "hs_*" or "*_events")
    if (part.includes("*") || part.includes("?")) {
      const re = globToRegex(part);
      for (const s of schemas) {
        if (re.test(s.id) && !resolved.includes(s.id)) resolved.push(s.id);
      }
      continue;
    }

    // exact id or alias
    const match = schemas.find((s) => s.id === part || s.aliases.includes(part));
    if (match && !resolved.includes(match.id)) resolved.push(match.id);
  }

  return resolved;
}

export function findSchemaBySpec(
  config: FleetConfig,
  spec: string,
): SchemaConfig | null {
  return (
    config.schemas.find(
      (s) => s.id === spec || s.aliases.includes(spec),
    ) ?? null
  );
}

export function activeSchemas(config: FleetConfig): SchemaConfig[] {
  return config.schemas.filter((s) => config.active_ids.includes(s.id));
}

export function schemaById(config: FleetConfig, id: string): SchemaConfig {
  const s = config.schemas.find((x) => x.id === id);
  if (!s) throw new Error(`Unknown schema id: ${id}`);
  return s;
}

/**
 * Expands auto_discover rules by querying each catalog's information_schema for
 * matching schema names, then materializing a SchemaConfig per match.
 *
 * `execSql` is injected so config.ts stays decoupled from databricks.ts.
 */
export async function expandFleetConfig(
  config: FleetConfig,
  execSql: (sql: string) => Promise<{ data: Record<string, unknown>[] }>,
): Promise<FleetConfig> {
  if (!config.auto_discover) return config;

  const spec = config.auto_discover;
  const discovered: SchemaConfig[] = [];
  const seenIds = new Set(config.schemas.map((s) => s.id));

  for (const catalog of spec.catalogs) {
    let rows: Record<string, unknown>[] = [];
    try {
      const res = await execSql(
        `SELECT schema_name FROM ${catalog}.information_schema.schemata ORDER BY schema_name`,
      );
      rows = res.data;
    } catch (err) {
      throw new Error(
        `Auto-discovery failed for catalog "${catalog}": ${(err as Error).message}`,
      );
    }

    for (const row of rows) {
      const name = String(row.schema_name);
      if (!matchesAny(name, spec.include)) continue;
      if (matchesAny(name, spec.exclude)) continue;
      if (seenIds.has(name)) continue;
      seenIds.add(name);
      discovered.push(materializeFromDiscovery(catalog, name, config));
    }
  }

  const allSchemas = [...config.schemas, ...discovered].sort((a, b) =>
    a.id.localeCompare(b.id),
  );
  const active_ids = resolveActiveIds(config.active_spec, allSchemas);

  return {
    ...config,
    schemas: allSchemas,
    active_ids,
  };
}
