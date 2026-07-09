<script lang="ts">
  // A key/value grid (task-93): the document drawer's Fields tab, connection info, etc. Values are
  // monospace by default (data/identifiers).
  let {
    entries,
    mono = true,
  }: {
    entries: [string, string][];
    mono?: boolean;
  } = $props();
</script>

<dl class="dc-kv" class:mono>
  {#each entries as [k, v] (k)}
    <div>
      <dt>{k}</dt>
      <dd>{v}</dd>
    </div>
  {/each}
</dl>

<style>
  .dc-kv {
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0;
  }
  .dc-kv > div {
    display: grid;
    grid-template-columns: minmax(0, 9rem) 1fr;
    gap: 1rem;
    padding: var(--cell);
    border-bottom: 1px solid var(--line);
  }
  .dc-kv > div:last-child {
    border-bottom: 0;
  }
  .dc-kv dt {
    color: var(--text-3);
    font-weight: 500;
  }
  .dc-kv dd {
    margin: 0;
    color: var(--text);
    overflow-wrap: anywhere;
  }
  .dc-kv.mono dd {
    font-family: 'IBM Plex Mono', ui-monospace, monospace;
  }
</style>
