// Build an index-definition YAML from the create-form inputs (task-47). Kept pure + tested so
// the form just collects choices. The Control Plane resolves this against the source schema and
// hard-blocks D23-violating cached fields (the create error surfaces inline).
import type { SourceField } from './api';

/** Map a coarse source type to a default GrowlerDB field type, or `null` if it can't be indexed
 *  in the M0 subset (binary/other → hydrate-only). Strings default to TEXT (full-text). */
export function defaultFieldType(sourceType: string): string | null {
  switch (sourceType) {
    case 'string':
      return 'TEXT';
    case 'long':
      return 'LONG';
    case 'double':
      return 'DOUBLE';
    case 'bool':
      return 'BOOL';
    case 'date':
      return 'DATE';
    default:
      return null; // binary / other
  }
}

/** Timestamp source `format` choices (task-112). Declaring one on a field forces it to a DATE
 *  column — even an integer/string epoch — so it surfaces in `time_fields` and enables the
 *  Search time filter (task-132). The `value` is the `TimeFormat` serde alias the backend parses. */
export const TIME_FORMATS: ReadonlyArray<{ value: string; label: string }> = [
  { value: 'epoch_s', label: 'Unix seconds' },
  { value: 'epoch_ms', label: 'Unix milliseconds' },
  { value: 'epoch_us', label: 'Unix microseconds' },
  { value: 'epoch_ns', label: 'Unix nanoseconds' },
  { value: 'rfc3339', label: 'ISO-8601 / RFC-3339 string' },
  { value: 'date', label: 'Date only (YYYY-MM-DD)' },
];

/** A source column declared as a timestamp: its path + the `TimeFormat` to normalize it. */
export interface TimeFieldInput {
  path: string;
  format: string;
}

/** Window granularities the backend accepts (`WindowGranularity`, lowercase serde). */
export const WINDOW_GRANULARITIES: ReadonlyArray<{ value: string; label: string }> = [
  { value: 'hourly', label: 'Hourly' },
  { value: 'daily', label: 'Daily' },
  { value: 'weekly', label: 'Weekly' },
];

/** Time-window routing config (task-81/132). The ingest-time `field` is the declared `timeField`;
 *  `eventTimeField` (optional) keeps a per-window zone-map so event-time queries prune windows;
 *  `hotWindows` (optional) is the cold-tiering policy (most-recent N windows kept hot). */
export interface WindowingInput {
  granularity: string;
  eventTimeField?: TimeFieldInput | null;
  hotWindows?: number | null;
}

export interface DefinitionInput {
  name: string;
  table: string; // "namespace.table"
  selection: 'ALL' | 'EXPLICIT';
  /** For EXPLICIT: the chosen source fields (their default GrowlerDB type is derived). */
  fields: SourceField[];
  /** Optional: a column to map as a DATE timestamp (task-112/132). Emitted as a `format`+`fast`
   *  override so it becomes a time field regardless of its source Arrow type. */
  timeField?: TimeFieldInput | null;
  /** Optional: time-window routing (task-81/132). Ignored unless `timeField` is also set — the
   *  window field is the declared `timeField`. */
  windowing?: WindowingInput | null;
}

/** Compose the index-definition YAML. The `catalog` is the table's namespace (the deployment's
 *  catalog connection is the Control Plane's, not this value). */
export function buildDefinition(input: DefinitionInput): string {
  const catalog = input.table.split('.')[0] || 'growlerdb';
  const tf = input.timeField && input.timeField.path ? input.timeField : null;
  // Windowing needs an ingest field, so it's only honored when a time field is declared.
  const win = input.windowing && tf ? input.windowing : null;
  const eventTf = win?.eventTimeField && win.eventTimeField.path ? win.eventTimeField : null;

  // Every declared time field (ingest + optional event) becomes a `format`+`fast` override (no
  // `type` — the format forces DATE; an explicit non-DATE type alongside it would be rejected).
  // Deduped by path so an event field that reuses the ingest column isn't emitted twice.
  const timeOverrides: TimeFieldInput[] = [];
  for (const t of [tf, eventTf]) {
    if (t && !timeOverrides.some((o) => o.path === t.path)) timeOverrides.push(t);
  }
  const overridePaths = new Set(timeOverrides.map((t) => t.path));
  const overrideEntry = (t: TimeFieldInput) =>
    `{ path: ${t.path}, format: ${t.format}, fast: true }`;

  const lines = [
    `name: ${input.name}`,
    `source: { iceberg: { catalog: ${catalog}, table: ${input.table} } }`,
  ];

  // Windowing is a top-level key referencing the declared time fields by path.
  if (win) {
    const parts = [`field: ${tf!.path}`, `granularity: ${win.granularity}`];
    if (eventTf) parts.push(`event_time_field: ${eventTf.path}`);
    if (win.hotWindows != null) parts.push(`hot_windows: ${win.hotWindows}`);
    lines.push(`windowing: { ${parts.join(', ')} }`);
  }

  if (input.selection === 'ALL') {
    // Under ALL, `fields[]` are per-path overrides — so the only entries we need are time fields.
    if (timeOverrides.length) {
      lines.push(
        `mapping: { selection: ALL, fields: [ ${timeOverrides.map(overrideEntry).join(', ')} ] }`,
      );
    } else {
      lines.push('mapping: { selection: ALL }');
    }
  } else {
    const mapped = input.fields
      .map((f) => ({ path: f.path, type: defaultFieldType(f.type) }))
      .filter((f): f is { path: string; type: string } => f.type !== null)
      // Time fields carry their own overrides below — don't also emit a typed entry for them.
      .filter((f) => !overridePaths.has(f.path));
    const entries = mapped.map((f) => `{ path: ${f.path}, type: ${f.type} }`);
    // Include time fields even if unchecked, so they get indexed under the allowlist.
    entries.push(...timeOverrides.map(overrideEntry));
    lines.push(`mapping: { selection: EXPLICIT, fields: [ ${entries.join(', ')} ] }`);
  }
  return lines.join('\n') + '\n';
}
