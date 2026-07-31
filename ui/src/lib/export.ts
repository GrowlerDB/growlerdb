// Client-side result export (JSON/CSV): the Engine exposes no REST export endpoint.

export function toJson(rows: unknown[]): string {
  return JSON.stringify(rows, null, 2);
}

/** Flatten `rows` to CSV. Columns are the union of keys (stable order of first appearance);
 *  values with `"`, `,`, or newlines are quoted per RFC 4180, and values that would be interpreted
 *  as a spreadsheet formula are neutralized. */
export function toCsv(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return '';
  const cols: string[] = [];
  for (const r of rows) for (const k of Object.keys(r)) if (!cols.includes(k)) cols.push(k);
  const cell = (v: unknown): string => {
    let s = v === null || v === undefined ? '' : String(v);
    // Formula-injection guard: Excel/Sheets evaluate a cell starting with =, +, -, @, tab or CR
    // as a formula, so prefix a single quote to keep untrusted values from running code.
    if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`;
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const header = cols.map(cell).join(',');
  const lines = rows.map((r) => cols.map((c) => cell(r[c])).join(','));
  return [header, ...lines].join('\n');
}

/** Trigger a browser download of `content`. */
export function download(filename: string, content: string, mime = 'text/plain'): void {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
