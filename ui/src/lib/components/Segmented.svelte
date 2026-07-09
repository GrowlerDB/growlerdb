<script lang="ts">
  // Segmented toggle (task-93): a row of mutually-exclusive options; the active one is raised.
  // `value` is bindable. Used for String/AST, Lucene/KQL, theme, density, etc.
  let {
    options,
    value = $bindable(),
    label = '',
  }: {
    options: { value: string; label: string }[];
    value: string;
    label?: string;
  } = $props();
</script>

<div class="dc-seg" role="group" aria-label={label}>
  {#each options as o (o.value)}
    <button
      type="button"
      class:active={value === o.value}
      aria-pressed={value === o.value}
      onclick={() => (value = o.value)}>{o.label}</button
    >
  {/each}
</div>

<style>
  .dc-seg {
    display: inline-flex;
    padding: 2px;
    gap: 2px;
    background: var(--panel2);
    border: 1px solid var(--line);
    border-radius: 7px;
  }
  .dc-seg button {
    border: 0;
    border-radius: 5px;
    background: transparent;
    color: var(--text-2);
    padding: 4px 10px;
    font: inherit;
    font-weight: 500;
    cursor: pointer;
    line-height: 1.4;
  }
  .dc-seg button:hover {
    color: var(--text);
  }
  .dc-seg button.active {
    background: var(--field);
    color: var(--text);
    box-shadow: 0 1px 2px var(--shadow);
  }
</style>
