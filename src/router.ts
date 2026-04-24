import Anthropic from "@anthropic-ai/sdk";
import type { SchemaConfig } from "./config.js";
import type { FleetKnowledgeGraph } from "./types.js";

// Use Haiku for pre-filter routing — it's dirt cheap and plenty smart for this task.
const ROUTER_MODEL = "claude-haiku-4-5";
const client = new Anthropic();

const ROUTER_SYSTEM_PROMPT = `You are a schema router. Given a natural language question and
a list of schemas (some already introspected, some only known by name), pick the 1-3 schemas
most likely to contain tables needed to answer the question.

Some schemas show a "Purpose hint:" line — that's the richest signal. Others show only their
name and aliases ("not yet introspected") — the schema NAME itself is usually informative
(e.g. "hs_eligibility" handles eligibility questions, "hs_enrollment" handles enrollment).
Use name + aliases alone when that's all you have.

Respond with ONLY valid JSON: {"schema_ids": ["id1", "id2"]}

Pick the MINIMUM set of schemas that could answer the question. If only one schema is
obviously relevant, return a single-element list. If unsure about a question that might span
domains, include up to 3. If no schema looks relevant, return [] — the caller will fall back.`;

interface RouterResponse {
  schema_ids: string[];
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
    throw new Error(`Could not parse router JSON. First 200 chars: ${cleaned.slice(0, 200)}`);
  }
}

function buildRouterSummary(
  fleet: FleetKnowledgeGraph,
  configSchemas: SchemaConfig[],
  activeSchemaIds: string[],
): string {
  const active = new Set(activeSchemaIds);
  const builtById = new Map(fleet.schemas.map((kg) => [kg.schema_id, kg]));
  const lines: string[] = [];

  for (const cfg of configSchemas) {
    if (!active.has(cfg.id)) continue;
    const kg = builtById.get(cfg.id);

    if (kg && kg.tables.length > 0) {
      const domains = [...new Set(kg.tables.map((t) => t.domain))].slice(0, 3).join(",");
      const topTables = kg.tables
        .slice()
        .sort((a, b) => b.row_count - a.row_count)
        .slice(0, 3)
        .map((t) => t.table_name)
        .join(",");
      const purposeHint = kg.tables[0]!.purpose.slice(0, 140);
      lines.push(
        `- ${cfg.id} (${cfg.catalog}.${cfg.schema}) [domains: ${domains}] top tables: ${topTables}\n  Purpose hint: ${purposeHint}`,
      );
    } else {
      // Unbuilt — supply only name + catalog + aliases. Schema name is usually
      // informative enough for Haiku to route on.
      const aliasHint = cfg.aliases.length ? ` (aliases: ${cfg.aliases.join(", ")})` : "";
      lines.push(
        `- ${cfg.id} (${cfg.catalog}.${cfg.schema})${aliasHint} — not yet introspected`,
      );
    }
  }
  return lines.join("\n");
}

/**
 * Cheap Haiku call to narrow the active set down to 1-3 most-relevant schemas.
 * Considers BOTH built and unbuilt schemas in `configSchemas`. Caller is
 * responsible for building any picked schemas that aren't yet in `fleet`.
 */
export async function routeSchemas(
  question: string,
  fleet: FleetKnowledgeGraph,
  configSchemas: SchemaConfig[],
  activeSchemaIds: string[],
): Promise<string[]> {
  if (activeSchemaIds.length <= 1) return activeSchemaIds;

  const summary = buildRouterSummary(fleet, configSchemas, activeSchemaIds);

  try {
    const response = await client.messages.create({
      model: ROUTER_MODEL,
      max_tokens: 256,
      system: ROUTER_SYSTEM_PROMPT,
      messages: [
        {
          role: "user",
          content: `Question: ${question}\n\nSchemas:\n${summary}`,
        },
      ],
    });

    const textBlock = response.content.find((b) => b.type === "text");
    if (!textBlock || textBlock.type !== "text") return activeSchemaIds;

    const parsed = parseJson<RouterResponse>(textBlock.text);
    const active = new Set(activeSchemaIds);
    const picked = (parsed.schema_ids ?? []).filter((id) => active.has(id));
    if (picked.length === 0) return activeSchemaIds;
    return picked;
  } catch {
    // On failure, fall back to the full active set — correctness over optimization
    return activeSchemaIds;
  }
}
