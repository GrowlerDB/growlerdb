import { describe, it, expect } from 'vitest';
import { toJson, toCsv } from './export';

describe('toCsv', () => {
  it('writes a header from the union of keys and quotes special chars', () => {
    const csv = toCsv([
      { id: 'doc-1', score: 0.5 },
      { id: 'doc-2', score: 0.25 },
    ]);
    expect(csv).toBe('id,score\ndoc-1,0.5\ndoc-2,0.25');
  });

  it('quotes values containing comma, quote, or newline (RFC 4180)', () => {
    const csv = toCsv([{ id: 'a,b', note: 'he said "hi"' }]);
    expect(csv).toBe('id,note\n"a,b","he said ""hi"""');
  });

  it('is empty for no rows', () => {
    expect(toCsv([])).toBe('');
  });

  it('neutralizes spreadsheet formula injection (task-153 / I8)', () => {
    // Values that would evaluate as a formula in Excel/Sheets are prefixed with a single quote.
    expect(toCsv([{ v: '=cmd|calc' }])).toBe("v\n'=cmd|calc");
    expect(toCsv([{ v: '@SUM(A1)' }])).toBe("v\n'@SUM(A1)");
    expect(toCsv([{ v: '-2+3' }])).toBe("v\n'-2+3");
    // A dangerous value that ALSO needs RFC-4180 quoting still gets both.
    expect(toCsv([{ v: '=a,b' }])).toBe('v\n"\'=a,b"');
    // A benign value is untouched.
    expect(toCsv([{ v: 'hello' }])).toBe('v\nhello');
  });
});

describe('toJson', () => {
  it('pretty-prints the rows', () => {
    expect(toJson([{ id: 'x' }])).toBe('[\n  {\n    "id": "x"\n  }\n]');
  });
});
