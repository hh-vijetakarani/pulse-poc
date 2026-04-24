import type { QueryResult } from "./types.js";

const POLL_INTERVAL_MS = 2000;
const DEFAULT_MAX_POLLS = 60;

interface StatementResponse {
  statement_id: string;
  status: {
    state: "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELED" | "CLOSED";
    error?: { message?: string; error_code?: string };
  };
  manifest?: {
    schema?: {
      columns?: Array<{ name: string; type_text?: string; type_name?: string }>;
    };
    total_row_count?: number;
  };
  result?: {
    data_array?: unknown[][];
    external_links?: Array<{ external_link: string }>;
    chunk_index?: number;
    next_chunk_index?: number;
  };
}

class DatabricksClient {
  private readonly host: string;
  private readonly token: string;
  private readonly warehouseId: string;

  constructor() {
    const host = process.env.DATABRICKS_HOST;
    const token = process.env.DATABRICKS_TOKEN;
    const warehouseId = process.env.DATABRICKS_WAREHOUSE_ID;

    if (!host) throw new Error("DATABRICKS_HOST is not set in .env");
    if (!token) throw new Error("DATABRICKS_TOKEN is not set in .env");
    if (!warehouseId) throw new Error("DATABRICKS_WAREHOUSE_ID is not set in .env");

    this.host = host.replace(/\/$/, "");
    this.token = token;
    this.warehouseId = warehouseId;
  }

  private headers(): Record<string, string> {
    return {
      Authorization: `Bearer ${this.token}`,
      "Content-Type": "application/json",
    };
  }

  async executeStatement(
    sql: string,
    options: { maxPolls?: number } = {},
  ): Promise<QueryResult> {
    const start = Date.now();
    const maxPolls = options.maxPolls ?? DEFAULT_MAX_POLLS;

    const submitResp = await fetch(`${this.host}/api/2.0/sql/statements/`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({
        warehouse_id: this.warehouseId,
        statement: sql,
        wait_timeout: "30s",
        on_wait_timeout: "CONTINUE",
        format: "JSON_ARRAY",
        disposition: "INLINE",
      }),
    });

    if (!submitResp.ok) {
      const body = await submitResp.text();
      throw new Error(`Databricks submit failed (${submitResp.status}): ${body}`);
    }

    let payload = (await submitResp.json()) as StatementResponse;
    let polls = 0;

    while (
      (payload.status.state === "PENDING" || payload.status.state === "RUNNING") &&
      polls < maxPolls
    ) {
      await sleep(POLL_INTERVAL_MS);
      polls++;

      const pollResp = await fetch(
        `${this.host}/api/2.0/sql/statements/${payload.statement_id}`,
        { method: "GET", headers: this.headers() },
      );

      if (!pollResp.ok) {
        const body = await pollResp.text();
        throw new Error(`Databricks poll failed (${pollResp.status}): ${body}`);
      }

      payload = (await pollResp.json()) as StatementResponse;
    }

    if (payload.status.state !== "SUCCEEDED") {
      // Best-effort cancel so the statement doesn't keep burning warehouse time.
      if (payload.statement_id && payload.status.state === "RUNNING") {
        try {
          await fetch(
            `${this.host}/api/2.0/sql/statements/${payload.statement_id}/cancel`,
            { method: "POST", headers: this.headers() },
          );
        } catch {
          // ignore — best-effort
        }
      }
      const errMsg = payload.status.error?.message ?? `state=${payload.status.state} after ${polls} polls (~${(polls * POLL_INTERVAL_MS) / 1000}s)`;
      throw new Error(`Databricks query failed: ${errMsg}`);
    }

    const columns = (payload.manifest?.schema?.columns ?? []).map((c) => c.name);
    const dataArray = payload.result?.data_array ?? [];

    const data = dataArray.map((row) => {
      const obj: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    return {
      data,
      columns,
      row_count: payload.manifest?.total_row_count ?? data.length,
      sql_executed: sql,
      execution_time_ms: Date.now() - start,
    };
  }

  async testConnection(): Promise<{ success: boolean; message: string }> {
    try {
      const result = await this.executeStatement("SELECT 1 AS connected");
      if (result.data[0]?.connected === 1 || result.data[0]?.connected === "1") {
        return { success: true, message: `Connected to ${this.host}` };
      }
      return { success: false, message: `Unexpected response: ${JSON.stringify(result.data)}` };
    } catch (err) {
      return { success: false, message: (err as Error).message };
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const databricks = new DatabricksClient();
