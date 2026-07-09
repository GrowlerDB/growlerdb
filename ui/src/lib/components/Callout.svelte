<script lang="ts">
  // A tinted callout (task-93) for governance/degraded/info notes (e.g. blocked cached fields).
  import type { Snippet } from 'svelte';
  let { tone = 'info', children }: { tone?: 'info' | 'warn'; children: Snippet } = $props();
</script>

<div class="dc-callout {tone}" role="note">{@render children()}</div>

<style>
  .dc-callout {
    border: 1px solid var(--line);
    border-radius: 7px;
    padding: 9px 11px;
    background: var(--panel2);
    color: var(--text-2);
    font-size: 0.95em;
    line-height: 1.5;
  }
  .dc-callout.warn {
    background: var(--warn-weak);
    border-color: transparent;
    color: var(--warn);
  }
</style>
