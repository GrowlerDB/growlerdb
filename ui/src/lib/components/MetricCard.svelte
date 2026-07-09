<script lang="ts">
  // A metric card (task-93) for the Observability SLI grid: label · big value · unit · sub-line ·
  // optional sparkline, tinted by tone.
  import type { Snippet } from 'svelte';
  let {
    label,
    value,
    unit = '',
    sub = '',
    tone = 'default',
    spark,
    info,
  }: {
    label: string;
    value: string;
    unit?: string;
    sub?: string;
    tone?: 'default' | 'ok' | 'warn';
    spark?: Snippet;
    /** Optional trailing content in the label row — the self-serve info (ⓘ) affordance. */
    info?: Snippet;
  } = $props();
</script>

<div class="dc-metric {tone}">
  <div class="dc-metric-head">
    <div class="dc-metric-label">{label}</div>
    {#if info}<div class="dc-metric-info">{@render info()}</div>{/if}
  </div>
  <div class="dc-metric-value mono">
    {value}{#if unit}<span class="dc-metric-unit"> {unit}</span>{/if}
  </div>
  {#if sub}<div class="dc-metric-sub">{sub}</div>{/if}
  {#if spark}<div class="dc-metric-spark">{@render spark()}</div>{/if}
</div>

<style>
  .dc-metric {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 13px 15px;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .dc-metric-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
  }
  .dc-metric-label {
    color: var(--text-2);
    font-weight: 500;
    font-size: 0.92em;
  }
  .dc-metric-info {
    flex: 0 0 auto;
    /* Pull the ⓘ tight to the top-right and out of the text baseline. */
    margin: -2px -4px 0 0;
  }
  .dc-metric-value {
    font-size: 1.7em;
    font-weight: 600;
    letter-spacing: -0.02em;
  }
  .dc-metric.ok .dc-metric-value {
    color: var(--ok);
  }
  .dc-metric.warn .dc-metric-value {
    color: var(--warn);
  }
  .dc-metric-unit {
    font-size: 0.55em;
    color: var(--text-3);
    font-weight: 500;
  }
  .dc-metric-sub {
    color: var(--text-3);
    font-size: 0.9em;
  }
  .dc-metric-spark {
    margin-top: 6px;
  }
</style>
