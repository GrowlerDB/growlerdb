// Sparkline geometry: turn a series into an SVG polyline `points` string, scaled to fit a `w × h`
// box with a small vertical inset. Pure + tested so the Sparkline component is trivial.

/** `points` attribute for an SVG `<polyline>` over `values` in a `w × h` box (inset `pad` px top/
 *  bottom). A flat or single-point series draws a centred horizontal line. */
export function sparklinePath(values: number[], w: number, h: number, pad = 2): string {
  if (values.length === 0) return '';
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min;
  const innerH = Math.max(0, h - 2 * pad);
  const stepX = values.length > 1 ? w / (values.length - 1) : 0;
  return values
    .map((v, i) => {
      const x = values.length > 1 ? i * stepX : w / 2;
      // Higher value → higher on screen (smaller y). Flat series sits in the middle.
      const norm = span === 0 ? 0.5 : (v - min) / span;
      const y = pad + (1 - norm) * innerH;
      return `${round(x)},${round(y)}`;
    })
    .join(' ');
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}
