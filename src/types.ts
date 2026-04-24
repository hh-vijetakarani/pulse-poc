// ==================== SCHEMA TYPES ====================

export interface TableSchema {
  schema_id: string;
  catalog: string;
  schema: string;
  table_name: string;
  columns: ColumnInfo[];
  row_count?: number;
  table_comment?: string;
}

export interface ColumnInfo {
  column_name: string;
  data_type: string;
  comment: string | null;
}

export interface TableSample {
  schema_id: string;
  table_name: string;
  sample_rows: Record<string, unknown>[];
  distinct_values: Record<string, string[]>;
  row_count: number;
  sampled_columns: string[];
  skipped_columns: string[];
}

export interface TagInfo {
  schema_id: string;
  object_type: "table" | "column";
  table_name: string;
  column_name?: string;
  tag_name: string;
  tag_value: string;
}

// ==================== PROTOBUF TYPES ====================

export interface ProtoContext {
  messages: ProtoMessage[];
  enums: ProtoEnum[];
  services: ProtoService[];
  source_files: string[];
}

export interface ProtoMessage {
  name: string;
  full_name: string;
  comment?: string;
  fields: ProtoField[];
  likely_table_match?: string;
  match_score?: number;
}

export interface ProtoField {
  name: string;
  type: string;
  is_repeated: boolean;
  is_optional: boolean;
  comment?: string;
  enum_ref?: string;
}

export interface ProtoEnum {
  name: string;
  full_name: string;
  comment?: string;
  values: { name: string; number: number; comment?: string }[];
}

export interface ProtoService {
  name: string;
  methods: { name: string; input_type: string; output_type: string; comment?: string }[];
}

// ==================== DBT CONTEXT ====================

export interface DbtContext {
  models: DbtModel[];
  source_files: string[];
}

export interface DbtModel {
  name: string;
  description?: string;
  database?: string;
  schema?: string;
  columns: { name: string; description?: string; tests?: string[]; meta?: Record<string, unknown> }[];
  meta?: Record<string, unknown>;
  likely_table_match?: string;
}

// ==================== KNOWLEDGE GRAPH TYPES ====================

// Per-schema knowledge graph (one Claude call produces one of these).
export interface KnowledgeGraph {
  schema_id: string;
  catalog: string;
  schema: string;
  generated_at: string;
  tables: TableKnowledge[];
  relationships: Relationship[];
  funnel: FunnelDefinition | null;
  derivable_metrics: DerivedMetric[];
  sample_questions: string[];
  proto_enriched: boolean;
  dbt_enriched: boolean;
}

// Fleet-level: all configured schemas' KGs combined (this is what the classifier sees).
export interface FleetKnowledgeGraph {
  generated_at: string;
  schemas: KnowledgeGraph[];
}

export interface TableKnowledge {
  schema_id: string;
  catalog: string;
  schema: string;
  table_name: string;
  purpose: string;
  domain: string;
  key_columns: { column: string; meaning: string }[];
  join_keys: { column: string; joins_to: string; foreign_column: string }[];
  default_filters: { column: string; condition: string; reason: string }[];
  business_rules: string[];
  enum_columns: { column: string; enum_name: string; values: string[] }[];
  row_count: number;
  all_columns: ColumnInfo[];
  proto_message?: string;
  dbt_model?: string;
}

export interface Relationship {
  schema_id: string;
  from_table: string;
  to_table: string;
  join_key: string;
  type: "one_to_many" | "many_to_one" | "many_to_many" | "one_to_one";
  source: "inferred" | "proto" | "dbt";
}

export interface FunnelDefinition {
  schema_id: string;
  name: string;
  stages: { name: string; table: string; filter: string; order: number }[];
}

export interface DerivedMetric {
  schema_id: string;
  name: string;
  description: string;
  tables_needed: string[];
  sql_pattern: string;
}

// ==================== CLASSIFICATION TYPES ====================

export interface ClassificationResult {
  relevant_tables: {
    schema_id: string;
    table: string;
    purpose: string;
    relevance_score: number;
  }[];
  suggested_joins: {
    from_schema_id: string;
    from: string;
    to_schema_id: string;
    to: string;
    on: string;
  }[];
  suggested_filters: { schema_id: string; column: string; condition: string; reason: string }[];
  enum_context: { schema_id: string; column: string; valid_values: string[] }[];
  matched_metric: DerivedMetric | null;
  extracted_parameters: Record<string, string>;
  confidence: number;
  method: "claude" | "validated_reuse" | "no_match";
  answerable: boolean;
  validated_query_id?: string;
  routed_schema_ids?: string[];
}

// ==================== QUERY + RESPONSE TYPES ====================

export interface GeneratedSQL {
  sql: string;
  explanation: string;
  tables_used: string[];
  estimated_complexity: "low" | "medium" | "high";
  schemas_used: string[];
}

export interface QueryResult {
  data: Record<string, unknown>[];
  columns: string[];
  row_count: number;
  sql_executed: string;
  execution_time_ms: number;
}

export interface PulseResponse {
  question: string;
  classification: ClassificationResult;
  generated_sql: GeneratedSQL;
  result: QueryResult;
  narrative: string;
  total_time_ms: number;
  timestamp: string;
}

// ==================== LEARNING TYPES ====================

export interface Correction {
  schema_id: string;
  table: string;
  rule: string;
  added_at: string;
}

export interface ValidatedQuery {
  id: string;
  question: string;
  sql: string;
  schemas_used: string[];
  tables_used: string[];
  description: string;
  validated_at: string;
  use_count: number;
}

export interface FeedbackEntry {
  question: string;
  sql: string;
  narrative: string;
  feedback: "good" | "bad" | "corrected" | "unrated";
  correction_note?: string;
  timestamp: string;
  schemas_used?: string[];
}
