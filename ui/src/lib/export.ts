// Result export (task-46). The Engine doesn't expose a REST export endpoint yet, so the UI
// exports the result set it's holding as JSON or CSV — a client-side download.

export function toJson(rows: unknown[]): string {
  return JSON.stringify(rows, null, 2);
}

/** Flatten `rows` to CSV. Columns are the union of keys (stable order of first appearance);
 *  values with `"`, `,`, or newlines are quoted per RFC 4180, and values that would be interpreted
 *  as a spreadsheet formula are neutralized (task-153 / I8). */
export function toCsv(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return '';
  const cols: string[] = [];
  for (const r of rows) for (const k of Object.keys(r)) if (!cols.includes(k)) cols.push(k);
  const cell = (v: unknown): string => {
    let s = v === null || v === undefined ? '' : String(v);
    // Formula-injection guard: a cell beginning with =, +, -, @, tab or CR is evaluated as a
    // formula by Excel/Sheets when the CSV is opened — untrusted document values must not run code
    // on the analyst's machine. Prefix such a value with a single quote, then RFC-4180-quote it.
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
