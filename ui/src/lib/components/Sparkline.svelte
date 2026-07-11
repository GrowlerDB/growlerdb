<script lang="ts">
  // Inline sparkline for metric cards. Responsive width (measured, no aspect-ratio distortion) with
  // a hover guide + tooltip so a small card reads its own numbers the way the hero charts do —
  // mouse over to see the value (and time) at that point.
  let {
    points,
    times = [],
    height = 34,
    color = 'var(--accent)',
    format = (n: number) => n.toFixed(2),
  }: {
    points: number[];
    /** Optional epoch-ms per point; when present the tooltip shows the time too. */
    times?: number[];
    height?: number;
    color?: string;
    format?: (n: number) => string;
  } = $props();

  let w = $state(0);
  let hover = $state<number | null>(null);
  const PAD = 3;

  // Point pixel coordinates over the measured width. Empty/flat series draw a mid-line baseline.
  const coords = $derived.by(() => {
    const n = points.length;
    if (!w || n === 0) return [] as { x: number; y: number }[];
    const min = Math.min(...points);
    const max = Math.max(...points);
    const span = max - min || 1;
    const innerW = Math.max(1, w - PAD * 2);
    const innerH = height - PAD * 2;
    return points.map((v, i) => ({
      x: PAD + (n === 1 ? innerW / 2 : (i / (n - 1)) * innerW),
      y: PAD + innerH - ((v - min) / span) * innerH,
    }));
  });
  const path = $derived(coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(' '));

  function onMove(e: PointerEvent) {
    const n = points.length;
    if (!w || n === 0) return;
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const frac = (e.clientX - rect.left - PAD) / Math.max(1, rect.width - PAD * 2);
    hover = Math.max(0, Math.min(n - 1, Math.round(frac * (n - 1))));
  }
  function fmtTime(ms: number): string {
    return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
</script>

<div
  class="dc-spark"
  style="height:{height}px"
  bind:clientWidth={w}
  role="presentation"
  onpointermove={onMove}
  onpointerleave={() => (hover = null)}
>
  <svg width={w} {height} viewBox="0 0 {w} {height}" aria-hidden="true">
    {#if path}
      <polyline
        points={path}
        fill="none"
        stroke={color}
        stroke-width="1.5"
        stroke-linejoin="round"
        stroke-linecap="round"
      />
    {/if}
    {#if hover != null && coords[hover]}
      <line
        x1={coords[hover].x}
        y1={PAD}
        x2={coords[hover].x}
        y2={height - PAD}
        stroke="var(--text-3)"
        stroke-width="1"
        stroke-dasharray="2 2"
        opacity="0.7"
      />
      <circle cx={coords[hover].x} cy={coords[hover].y} r="2.6" fill={color} />
    {/if}
  </svg>
  {#if hover != null && coords[hover]}
    <div class="dc-spark-tip" style="left:{coords[hover].x}px; bottom:{height + 4}px" role="status">
      <span class="dc-spark-val">{format(points[hover])}</span>
      {#if times[hover]}<span class="dc-spark-time">{fmtTime(times[hover])}</span>{/if}
    </div>
  {/if}
</div>

<style>
  .dc-spark {
    display: block;
    position: relative;
    width: 100%;
    touch-action: none;
  }
  .dc-spark svg {
    display: block;
    overflow: visible;
  }
  .dc-spark-tip {
    position: absolute;
    transform: translateX(-50%);
    background: var(--panel);
    border: 1px solid var(--line-strong);
    border-radius: 6px;
    box-shadow: 0 4px 14px var(--shadow);
    padding: 2px 7px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0;
    white-space: nowrap;
    pointer-events: none;
    z-index: 5;
  }
  .dc-spark-val {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--text);
  }
  .dc-spark-time {
    font-size: 0.62rem;
    color: var(--text-3);
  }
</style>
