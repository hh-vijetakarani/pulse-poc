import Anthropic from "@anthropic-ai/sdk";
import { buildFocusedContext } from "./sql-generator.js";
import type {
  ClassificationResult,
  FleetKnowledgeGraph,
  GeneratedSQL,
} from "./types.js";

// Haiku is plenty for a critique pass — cheap and fast.
const VERIFIER_MODEL = "claude-haiku-4-5";
const client = new Anthropic();

export interface VerificationResult {
  looks_correct: boolean;
  issues: string[];
  suggested_fix: string | null;
  confidence: number;
}

const VERIFIER_SYSTEM_PROMPT = `You are auditing a Databricks SQL query for correctness.

You will be given:
- The user's natural-language question
- A schema context (tables, columns, data types, enum values, default filters)
- A generated SQL query

Your job: decide whether the SQL correctly answers the question using ONLY the
tables and columns that actually exist in the schema context. Be strict.

Flag any of these as issues:
- A column referenced in the SQL that is NOT listed in the schema context
- A table referenced that is not listed (check the full <catalog>.<schema>.<table> name)
- An enum value in a WHERE clause that isn't in the "valid values" list for that column
- A "default filter" from the context that should have been applied but wasn't
  (e.g., missing \`terminated_at IS NULL\` or similar)
- A semantic mismatch — using a column whose meaning doesn't match what the question asks
  (e.g., using \`created_at\` when the question is about data-source timestamps and the
  schema uses \`__source_ts\`)

If looks_correct is false AND you're confident what the right query should be, provide
\`suggested_fix\` with a revised SQL. If you're unsure, set suggested_fix to null and just
list the issues.

Respond with ONLY valid JSON — no prose, no markdown fences:
{
  "looks_correct": true|false,
  "issues": ["one-line issue 1", "one-line issue 2"],
  "suggested_fix": "revised SQL string" | null,
  "confidence": 0.0-1.0
}

If looks_correct is true, set issues to [].`;

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
    throw new Error(`Could not parse verifier JSON. First 200 chars: ${cleaned.slice(0, 200)}`);
  }
}

export async function verifySql(
  question: string,
  sql: GeneratedSQL,
  classification: ClassificationResult,
  fleet: FleetKnowledgeGraph,
): Promise<VerificationResult> {
  const tableCtx = buildFocusedContext(classification, fleet);

  const userContent = [
    `Question: ${question}`,
    "",
    "Schema context:",
    tableCtx,
    "",
    "Generated SQL:",
    sql.sql,
  ].join("\n");

  try {
    const response = await client.messages.create({
      model: VERIFIER_MODEL,
      max_tokens: 2048,
      system: VERIFIER_SYSTEM_PROMPT,
      messages: [{ role: "user", content: userContent }],
    });

    const textBlock = response.content.find((b) => b.type === "text");
    if (!textBlock || textBlock.type !== "text") {
      return {
        looks_correct: true,
        issues: [],
        suggested_fix: null,
        confidence: 0,
      };
    }

    const parsed = parseJson<VerificationResult>(textBlock.text);
    return {
      looks_correct: parsed.looks_correct ?? true,
      issues: parsed.issues ?? [],
      suggested_fix: parsed.suggested_fix ?? null,
      confidence: parsed.confidence ?? 0.5,
    };
  } catch (err) {
    // On verifier failure, don't block the query — just signal we couldn't verify.
    return {
      looks_correct: true,
      issues: [`verifier call failed: ${(err as Error).message.slice(0, 120)}`],
      suggested_fix: null,
      confidence: 0,
    };
  }
}
