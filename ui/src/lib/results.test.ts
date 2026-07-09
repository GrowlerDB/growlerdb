import { describe, it, expect } from 'vitest';
import { formatEpochMicros, pickTimestamp, pickSnippet, truncate } from './results';

describe('formatEpochMicros', () => {
  it('formats epoch micros as a stable UTC string', () => {
    // 2026-06-30T12:34:56Z in micros.
    const micros = Date.UTC(2026, 5, 30, 12, 34, 56) * 1000;
    expect(formatEpochMicros(micros)).toBe('2026-06-30 12:34:56');
  });
  it('rejects non-numbers and non-finite values', () => {
    expect(formatEpochMicros('2026-06-30')).toBeNull();
    expect(formatEpochMicros(undefined)).toBeNull();
    expect(formatEpochMicros(Number.NaN)).toBeNull();
    expect(formatEpochMicros(Infinity)).toBeNull();
  });
});

describe('pickTimestamp', () => {
  const micros = Date.UTC(2026, 0, 2, 3, 4, 5) * 1000;
  it('returns the first DATE column the hit carries a value for', () => {
    const fields = { id: 'x', ingest_ts: micros, body: 'hi' };
    expect(pickTimestamp(fields, ['event_ts', 'ingest_ts'])).toEqual({
      name: 'ingest_ts',
      display: '2026-01-02 03:04:05',
    });
  });
  it('returns null when no time field is cached or value is not numeric micros', () => {
    expect(pickTimestamp({ id: 'x' }, ['ingest_ts'])).toBeNull();
    expect(pickTimestamp({ ingest_ts: 'nope' }, ['ingest_ts'])).toBeNull();
    expect(pickTimestamp(undefined, ['ingest_ts'])).toBeNull();
  });
});

describe('pickSnippet', () => {
  it('prefers a conventionally-named text field', () => {
    const fields = { id: 'abc', body: 'temperature within range', site: 'plant-1' };
    expect(pickSnippet(fields)).toEqual({ name: 'body', value: 'temperature within range' });
  });
  it('falls back to the longest string value when it is plausibly free text', () => {
    const fields = { a: 'short', b: 'a considerably longer free text value here' };
    expect(pickSnippet(fields)?.name).toBe('b');
  });
  it('returns null when only short unnamed keywords are present (they stay chips)', () => {
    expect(pickSnippet({ device_id: 'sensor-1', status: 'ok' })).toBeNull();
  });
  it('excludes the named fields (e.g. the timestamp source)', () => {
    const fields = {
      body: 'excluded body text that is long',
      note: 'a long free-text note to show',
    };
    expect(pickSnippet(fields, new Set(['body']))).toEqual({
      name: 'note',
      value: 'a long free-text note to show',
    });
  });
  it('returns null when there are no usable strings', () => {
    expect(pickSnippet({ count: 3, ok: true })).toBeNull();
    expect(pickSnippet(undefined)).toBeNull();
  });
});

describe('truncate', () => {
  it('leaves short text intact', () => {
    expect(truncate('hello', 10)).toBe('hello');
  });
  it('cuts on a word boundary and appends an ellipsis', () => {
    expect(truncate('the quick brown fox jumps', 16)).toBe('the quick brown…');
  });
});
