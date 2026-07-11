<script lang="ts">
  // A styled data table: consistent header/row chrome at the current density (`--cell`).
  // The caller supplies the column headers + the rows via the `head` and `body` snippets so the
  // table stays type-safe over its own row shape.
  import type { Snippet } from 'svelte';
  let { head, body }: { head: Snippet; body: Snippet } = $props();
</script>

<table class="dc-table">
  <thead>
    <tr>{@render head()}</tr>
  </thead>
  <tbody>{@render body()}</tbody>
</table>

<style>
  .dc-table {
    width: 100%;
    border-collapse: collapse;
    font-variant-numeric: tabular-nums;
  }
  .dc-table :global(th) {
    text-align: left;
    color: var(--text-3);
    font-weight: 500;
    font-size: 0.85em;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    padding: var(--cell);
    border-bottom: 1px solid var(--line);
    white-space: nowrap;
  }
  .dc-table :global(td) {
    padding: var(--cell);
    border-bottom: 1px solid var(--line);
    color: var(--text);
  }
  .dc-table :global(tbody tr:hover) {
    background: var(--accent-weakest);
  }
  .dc-table :global(tbody tr:last-child td) {
    border-bottom: 0;
  }
</style>
