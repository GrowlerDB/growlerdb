<script lang="ts">
  // Status/label badge (task-93). `tone='planned'` is the consistent "not built yet" pill used
  // across the revamp; ok/warn/accent carry semantic colour.
  import type { Snippet } from 'svelte';
  let {
    tone = 'default',
    children,
  }: {
    tone?: 'default' | 'ok' | 'warn' | 'accent' | 'planned';
    children: Snippet;
  } = $props();
</script>

<span class="dc-badge {tone}">{@render children()}</span>

<style>
  .dc-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.3em;
    padding: 1px 7px;
    border-radius: 5px;
    font-size: 0.85em;
    font-weight: 500;
    line-height: 1.5;
    border: 1px solid var(--line);
    background: var(--panel2);
    color: var(--text-2);
    white-space: nowrap;
  }
  .dc-badge.ok {
    background: var(--ok-weak);
    border-color: transparent;
    color: var(--ok);
  }
  .dc-badge.warn {
    background: var(--warn-weak);
    border-color: transparent;
    color: var(--warn);
  }
  .dc-badge.accent,
  .dc-badge.planned {
    background: var(--accent-weakest);
    border-color: var(--accent-weak);
    color: var(--accent);
  }
</style>
