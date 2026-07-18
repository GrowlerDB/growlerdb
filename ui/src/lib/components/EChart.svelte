<script lang="ts">
  // Themed ECharts wrapper: the "hero" time-series charts (source-vs-index rate, index:source
  // size, latency) the sparkline cards can't express. The parent passes a plain
  // ECharts `option` (series/data only); this component injects theme-token colors (so light/dark
  // + accent keep working), handles resize, and disposes cleanly. Deep charts still live in Grafana.
  import { onMount, onDestroy } from 'svelte';
  import * as echarts from 'echarts';

  let {
    option,
    height = 220,
    ariaLabel = 'chart',
  }: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    option: any;
    height?: number;
    ariaLabel?: string;
  } = $props();

  let el = $state<HTMLDivElement | null>(null);
  let chart: echarts.ECharts | null = null;
  let ro: ResizeObserver | null = null;
  let themeObserver: MutationObserver | null = null;

  /** Read the current theme tokens off the document so the chart matches light/dark + accent. */
  function tokens() {
    const s = getComputedStyle(document.documentElement);
    const v = (name: string, fallback: string) => s.getPropertyValue(name).trim() || fallback;
    return {
      text: v('--text-2', '#666'),
      faint: v('--text-3', '#999'),
      line: v('--line', '#ddd'),
      accent: v('--accent', '#7fa9d4'),
      ok: v('--ok', '#4fb87e'),
      warn: v('--warn', '#d9a04a'),
      panel: v('--panel', '#fff'),
    };
  }

  /** Merge the caller's option with themed axis/grid/tooltip/legend defaults. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function themed(opt: any) {
    const c = tokens();
    return {
      // Theme tokens first; then fixed mid-tone hues (legible on light + dark) so a chart with
      // more than four series (the 8-component index-size stack) doesn't cycle back to a
      // duplicate color.
      color: [c.accent, c.ok, c.warn, c.faint, '#7c6ff0', '#12a5b8', '#d6558f', '#8a8f98'],
      textStyle: {
        color: c.text,
        fontFamily: 'Instrument Sans, system-ui, sans-serif',
        fontSize: 11,
      },
      // containLabel makes the grid reserve exactly enough room for the axis labels, so wide
      // y-labels (e.g. byte values like "221 MB") never clip; left/bottom become small paddings
      // outside the labels rather than a fixed label budget.
      grid: { top: 26, right: 12, bottom: 6, left: 8, containLabel: true, ...(opt.grid ?? {}) },
      legend: {
        type: 'plain',
        top: 0,
        right: 0,
        icon: 'roundRect',
        itemWidth: 10,
        itemHeight: 10,
        textStyle: { color: c.text, fontSize: 11 },
        ...(opt.legend ?? {}),
      },
      tooltip: {
        trigger: 'axis',
        backgroundColor: c.panel,
        borderColor: c.line,
        textStyle: { color: c.text, fontSize: 11 },
        ...(opt.tooltip ?? {}),
      },
      xAxis: applyAxis(opt.xAxis, c),
      yAxis: applyAxis(opt.yAxis, c),
      series: opt.series ?? [],
      ...(opt.animationDuration != null ? { animationDuration: opt.animationDuration } : {}),
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function applyAxis(axis: any, c: ReturnType<typeof tokens>) {
    const base = {
      axisLine: { lineStyle: { color: c.line } },
      axisLabel: { color: c.faint },
      splitLine: { lineStyle: { color: c.line, opacity: 0.5 } },
    };
    if (Array.isArray(axis)) return axis.map((a) => ({ ...base, ...a }));
    return { ...base, ...(axis ?? {}) };
  }

  function render() {
    if (!chart) return;
    chart.setOption(themed(option), true);
  }

  onMount(() => {
    if (!el) return;
    chart = echarts.init(el, undefined, { renderer: 'svg' });
    render();
    ro = new ResizeObserver(() => chart?.resize());
    ro.observe(el);
    // Re-theme when the light/dark/accent attributes flip on <html>.
    themeObserver = new MutationObserver(render);
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme', 'data-accent'],
    });
  });

  // Re-render whenever the caller's option changes (new data on refresh).
  $effect(() => {
    void option;
    render();
  });

  onDestroy(() => {
    ro?.disconnect();
    themeObserver?.disconnect();
    chart?.dispose();
    chart = null;
  });
</script>

<div
  bind:this={el}
  class="dc-chart"
  style="height:{height}px"
  role="img"
  aria-label={ariaLabel}
></div>

<style>
  .dc-chart {
    width: 100%;
  }
</style>
