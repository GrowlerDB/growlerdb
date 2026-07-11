<script lang="ts">
  // One node of the BM25 explanation tree, rendered recursively. Each node shows its
  // contributed score + description; children are the sub-clauses (term freq, IDF, field norm, …).
  import Self from './ExplainClauseTree.svelte';
  import type { ExplainClause } from '../lib/api';

  let { clause, depth = 0 }: { clause: ExplainClause; depth?: number } = $props();
</script>

<div class="clause" style="--depth:{depth}">
  <div class="row">
    <span class="score mono">{clause.score.toFixed(4)}</span>
    <span class="desc">{clause.description}</span>
  </div>
  {#if clause.details && clause.details.length > 0}
    <div class="children">
      {#each clause.details as child, i (i)}
        <Self clause={child} depth={depth + 1} />
      {/each}
    </div>
  {/if}
</div>

<style>
  .row {
    display: flex;
    gap: 0.6rem;
    align-items: baseline;
    padding: 2px 0;
  }
  .score {
    color: var(--accent);
    font-size: 0.85em;
    min-width: 4.5em;
    flex-shrink: 0;
    text-align: right;
  }
  .desc {
    color: var(--text-2);
    font-size: 0.9em;
    word-break: break-word;
  }
  .children {
    margin-left: 0.75rem;
    padding-left: 0.6rem;
    border-left: 1px solid var(--line);
  }
</style>
