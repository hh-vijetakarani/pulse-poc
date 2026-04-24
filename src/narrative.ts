import Anthropic from "@anthropic-ai/sdk";
import { formatResultsForClaude } from "./engine.js";
import { loadSeedContext, schemaKgById } from "./knowledge.js";
import type {
  ClassificationResult,
  FleetKnowledgeGraph,
  GeneratedSQL,
  QueryResult,
} from "./types.js";

const MODEL = "claude-sonnet-4-6";
const client = new Anthropic();

const NARRATIVE_SYSTEM_PROMPT = `You explain Databricks query results to non-technical PMs
and EMs. Be direct. Use actual numbers from the data.

Format:
- Line 1: **Bold headline** with the key number
- Lines 2-4: Supporting data points with actual numbers
- Line 5 (optional): One suggested next step

Rules:
- Use actual numbers from the results. Never "there appears to be a trend."
- State differences concretely: "X is 23% lower than Y" not "X is lower."
- Keep it under 6 lines. This is read on a screen, not in a slide.
- Don't describe the SQL or how you got the answer. Just answer the question.
- No markdown headers. No code blocks. No bulleted preamble.`;

export async function generateNarrative(
  question: string,
  result: QueryResult,
  sql: GeneratedSQL,
  classification: ClassificationResult,
  fleet: FleetKnowledgeGraph,
): Promise<string> {
  const usedSchemaIds =
    sql.schemas_used.length > 0
      ? sql.schemas_used
      : [...new Set(classification.relevant_tables.map((r) => r.schema_id))];

  // Pull the seed briefings for every schema the query touched.
  const seedBlocks = usedSchemaIds
    .map((id) => {
      const seed = loadSeedContext(id);
      if (!seed) return null;
      const trimmed = seed.split(/\s+/).slice(0, 350).join(" ");
      return `# Seed context for ${id}\n${trimmed}`;
    })
    .filter((s): s is string => s !== null);

  // Funnel context: if any used schema has a funnel, mention its stages.
  const funnelLines: string[] = [];
  for (const id of usedSchemaIds) {
    const kg = schemaKgById(fleet, id);
    if (kg?.funnel) {
      funnelLines.push(
        `Funnel (${id}): ${kg.funnel.stages.map((s) => s.name).join(" → ")}`,
      );
    }
  }

  const systemBlocks: Anthropic.TextBlockParam[] = [
    { type: "text", text: NARRATIVE_SYSTEM_PROMPT },
  ];
  if (seedBlocks.length > 0) {
    systemBlocks.push({
      type: "text",
      text: `Background briefing:\n${seedBlocks.join("\n\n")}${funnelLines.length ? "\n\n" + funnelLines.join("\n") : ""}`,
      cache_control: { type: "ephemeral" },
    });
  } else if (funnelLines.length) {
    systemBlocks.push({ type: "text", text: funnelLines.join("\n") });
  }

  const resultsStr = formatResultsForClaude(result, 25);
  const userContent = [
    `Question: ${question}`,
    `Query explanation: ${sql.explanation}`,
    `Schemas involved: ${usedSchemaIds.join(", ") || "(none)"}`,
    `Confidence: ${Math.round(classification.confidence * 100)}%`,
    "",
    "Results:",
    resultsStr,
  ].join("\n");

  try {
    const response = await client.messages.create({
      model: MODEL,
      max_tokens: 1024,
      system: systemBlocks,
      messages: [{ role: "user", content: userContent }],
    });

    const textBlock = response.content.find((b) => b.type === "text");
    if (!textBlock || textBlock.type !== "text") return fallbackNarrative(result);
    return textBlock.text.trim();
  } catch (err) {
    if (err instanceof Anthropic.APIError) {
      return `${fallbackNarrative(result)}\n(Narrative generation failed: ${err.message.slice(0, 80)})`;
    }
    return fallbackNarrative(result);
  }
}

function fallbackNarrative(result: QueryResult): string {
  return `**Query returned ${result.row_count.toLocaleString()} row${result.row_count === 1 ? "" : "s"}.**\nSee the table below for details.`;
}
