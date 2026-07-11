// Popover positioning: place a panel under a trigger, clamped to the viewport. Pure + tested; the
// Popover component just reads `getBoundingClientRect()` and applies the result.

export interface Rect {
  top: number;
  bottom: number;
  left: number;
  right: number;
  width: number;
}

export interface Viewport {
  width: number;
  height: number;
}

export interface Placement {
  /** `position: fixed` coordinates (px). */
  top: number;
  left: number;
  /** True when the panel was flipped above the trigger (not enough room below). */
  above: boolean;
}

/** Place a `panelW × panelH` panel just below `anchor`, left-aligned, with a `gap`. Clamp within the
 *  viewport (margin `m`); flip above the trigger when there isn't room below. */
export function popoverPlacement(
  anchor: Rect,
  panelW: number,
  panelH: number,
  viewport: Viewport,
  gap = 6,
  m = 8,
): Placement {
  const roomBelow = viewport.height - anchor.bottom;
  const above = roomBelow < panelH + gap + m && anchor.top > roomBelow;
  const top = above
    ? Math.max(m, anchor.top - gap - panelH)
    : Math.min(anchor.bottom + gap, viewport.height - panelH - m);
  const left = clamp(anchor.left, m, Math.max(m, viewport.width - panelW - m));
  return { top: Math.max(m, top), left, above };
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.min(Math.max(v, lo), hi);
}
