import Anthropic from "@anthropic-ai/sdk";
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { SchemaConfig } from "./config.js";
import type { FleetKnowledgeGraph } from "./types.js";

// Use Haiku for pre-filter routing — it's dirt cheap and plenty smart for this task.
const ROUTER_MODEL = "claude-haiku-4-5";
const client = new Anthropic();

const ROUTER_SYSTEM_PROMPT = `You are a schema router. Given a natural language question and
a list of schemas (some already introspected, some only known by name + aliases + a purpose
hint), pick the 1-3 schemas most likely to contain tables needed to answer the question.

Use all available signals:
- "Purpose hint:" — richest signal; trust it.
- Aliases — often carry feature names ("care_search", "clicks", "funnel") that map directly
  to question concepts even when the schema NAME doesn't.
- Schema name — "hs_eligibility" handles eligibility questions, "mixpanel_*" handles
  product-analytics / event-stream questions.

Important tie-breakers:
- "How many users ..." or "where do users drop off ..." or "funnel" / "click" / "convert"
  are product-analytics questions → route to mixpanel_* schemas if available.
- "Customer account" or "employer" or "legal entity" are account-management questions
  → route to hs_accounts_* schemas.
- "Eligibility check" / "benefits" → route to hs_eligibility.
- Don't let a single word like "customer" or "user" dominate if the rest of the question
  is about a specific feature (care search, enrollment flow, etc.).

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

function notesDocHint(cfg: SchemaConfig): string | null {
  if (!cfg.notes_doc) return null;
  const path = resolve(cfg.notes_doc);
  if (!existsSync(path)) return null;
  try {
    // Find the first meaningful paragraph after the title. This becomes the
    // router's "purpose hint" for an unbuilt schema — strong signal for Haiku.
    const text = readFileSync(path, "utf-8");
    const lines = text.split("\n");
    const paragraphs: string[] = [];
    let current: string[] = [];
    for (const line of lines) {
      if (line.startsWith("#") || line.startsWith("---")) {
        if (current.length) paragraphs.push(current.join(" "));
        current = [];
        continue;
      }
      if (line.trim() === "") {
        if (current.length) paragraphs.push(current.join(" "));
        current = [];
        continue;
      }
      current.push(line.trim());
    }
    if (current.length) paragraphs.push(current.join(" "));
    const firstMeaningful = paragraphs.find((p) => p.length > 40);
    return firstMeaningful ? firstMeaningful.slice(0, 240) : null;
  } catch {
    return null;
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
    const aliasHint = cfg.aliases.length ? ` (aliases: ${cfg.aliases.join(", ")})` : "";

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
        `- ${cfg.id} (${cfg.catalog}.${cfg.schema})${aliasHint} [domains: ${domains}] top tables: ${topTables}\n  Purpose hint: ${purposeHint}`,
      );
    } else {
      // Unbuilt — use the notes_doc as a purpose hint if available. The topic
      // doc's first paragraph usually describes what the schema is for, which
      // is enough signal for Haiku to route on.
      const docHint = notesDocHint(cfg);
      if (docHint) {
        lines.push(
          `- ${cfg.id} (${cfg.catalog}.${cfg.schema})${aliasHint} — not yet introspected\n  Purpose hint (from curated doc): ${docHint}`,
        );
      } else {
        lines.push(
          `- ${cfg.id} (${cfg.catalog}.${cfg.schema})${aliasHint} — not yet introspected`,
        );
      }
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
