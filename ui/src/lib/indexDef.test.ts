import { describe, it, expect } from 'vitest';
import { buildDefinition, defaultFieldType } from './indexDef';

describe('defaultFieldType', () => {
  it('maps coarse source types to GrowlerDB types', () => {
    expect(defaultFieldType('string')).toBe('TEXT');
    expect(defaultFieldType('long')).toBe('LONG');
    expect(defaultFieldType('date')).toBe('DATE');
  });
  it('returns null for un-indexable types', () => {
    expect(defaultFieldType('binary')).toBeNull();
    expect(defaultFieldType('other')).toBeNull();
  });
});

describe('buildDefinition', () => {
  it('builds an ALL definition (catalog from the table namespace)', () => {
    const yaml = buildDefinition({
      name: 'docs',
      table: 'growlerdb.docs',
      selection: 'ALL',
      fields: [],
    });
    expect(yaml).toBe(
      'name: docs\n' +
        'source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n' +
        'mapping: { selection: ALL }\n',
    );
  });

  it('builds an EXPLICIT definition, mapping types and dropping un-indexable fields', () => {
    const yaml = buildDefinition({
      name: 'logs',
      table: 'ns.logs',
      selection: 'EXPLICIT',
      fields: [
        { path: 'id', type: 'string' },
        { path: 'count', type: 'long' },
        { path: 'blob', type: 'binary' }, // dropped
      ],
    });
    expect(yaml).toContain(
      'mapping: { selection: EXPLICIT, fields: [ { path: id, type: TEXT }, { path: count, type: LONG } ] }',
    );
    expect(yaml).not.toContain('blob');
  });

  it('emits a time field as a format+fast override under ALL selection', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'ALL',
      fields: [],
      timeField: { path: 'ingest_ts', format: 'epoch_ms' },
    });
    expect(yaml).toContain(
      'mapping: { selection: ALL, fields: [ { path: ingest_ts, format: epoch_ms, fast: true } ] }',
    );
  });

  it('includes the time field in an EXPLICIT allowlist and overrides its type', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'EXPLICIT',
      fields: [
        { path: 'id', type: 'string' },
        { path: 'ingest_ts', type: 'long' }, // chosen as a normal field too...
      ],
      timeField: { path: 'ingest_ts', format: 'epoch_us' },
    });
    // ...but emitted once, as the format override (no `type: LONG`), and never duplicated.
    expect(yaml).toContain(
      'mapping: { selection: EXPLICIT, fields: [ { path: id, type: TEXT }, ' +
        '{ path: ingest_ts, format: epoch_us, fast: true } ] }',
    );
    expect(yaml).not.toContain('{ path: ingest_ts, type: LONG }');
  });

  it('adds the time field to an EXPLICIT allowlist even when it was not checked', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'EXPLICIT',
      fields: [{ path: 'id', type: 'string' }],
      timeField: { path: 'ts', format: 'rfc3339' },
    });
    expect(yaml).toContain('{ path: ts, format: rfc3339, fast: true }');
  });

  it('omits the time field when none is selected (empty path)', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'ALL',
      fields: [],
      timeField: { path: '', format: 'epoch_ms' },
    });
    expect(yaml).toBe(
      'name: events\n' +
        'source: { iceberg: { catalog: ns, table: ns.events } }\n' +
        'mapping: { selection: ALL }\n',
    );
  });

  it('emits a windowing key over the declared ingest time field (ALL)', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'ALL',
      fields: [],
      timeField: { path: 'ingest', format: 'epoch_ms' },
      windowing: { granularity: 'daily' },
    });
    expect(yaml).toBe(
      'name: events\n' +
        'source: { iceberg: { catalog: ns, table: ns.events } }\n' +
        'windowing: { field: ingest, granularity: daily }\n' +
        'mapping: { selection: ALL, fields: [ { path: ingest, format: epoch_ms, fast: true } ] }\n',
    );
  });

  it('declares the event-time field as its own override and references it in windowing', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'EXPLICIT',
      fields: [{ path: 'id', type: 'string' }],
      timeField: { path: 'ingest', format: 'epoch_us' },
      windowing: {
        granularity: 'hourly',
        eventTimeField: { path: 'event', format: 'rfc3339' },
        hotWindows: 7,
      },
    });
    expect(yaml).toContain(
      'windowing: { field: ingest, granularity: hourly, event_time_field: event, hot_windows: 7 }',
    );
    expect(yaml).toContain(
      'mapping: { selection: EXPLICIT, fields: [ { path: id, type: TEXT }, ' +
        '{ path: ingest, format: epoch_us, fast: true }, { path: event, format: rfc3339, fast: true } ] }',
    );
  });

  it('ignores windowing when no time field is declared', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'ALL',
      fields: [],
      timeField: null,
      windowing: { granularity: 'daily' },
    });
    expect(yaml).not.toContain('windowing:');
    expect(yaml).toContain('mapping: { selection: ALL }');
  });

  it('does not duplicate an override when the event field reuses the ingest column', () => {
    const yaml = buildDefinition({
      name: 'events',
      table: 'ns.events',
      selection: 'ALL',
      fields: [],
      timeField: { path: 'ts', format: 'epoch_ms' },
      windowing: { granularity: 'weekly', eventTimeField: { path: 'ts', format: 'epoch_ms' } },
    });
    expect(yaml).toContain('windowing: { field: ts, granularity: weekly, event_time_field: ts }');
    expect(yaml).toContain(
      'mapping: { selection: ALL, fields: [ { path: ts, format: epoch_ms, fast: true } ] }',
    );
  });
});
