<script lang="ts">
  // Underline tabs: a tab bar with an accent underline on the active tab. `active` is
  // bindable; the parent renders the active panel (`{#if active === 'x'}`).
  let {
    tabs,
    active = $bindable(),
  }: {
    tabs: { id: string; label: string }[];
    active: string;
  } = $props();
</script>

<div class="dc-tabs" role="tablist">
  {#each tabs as tab (tab.id)}
    <button
      type="button"
      role="tab"
      aria-selected={active === tab.id}
      class:active={active === tab.id}
      onclick={() => (active = tab.id)}>{tab.label}</button
    >
  {/each}
</div>

<style>
  .dc-tabs {
    display: flex;
    gap: 2px;
    border-bottom: 1px solid var(--line);
  }
  .dc-tabs button {
    border: 0;
    background: transparent;
    color: var(--text-2);
    padding: 8px 12px;
    font: inherit;
    font-weight: 500;
    cursor: pointer;
    box-shadow: inset 0 -2px 0 transparent;
  }
  .dc-tabs button:hover {
    color: var(--text);
    background: var(--accent-weakest);
  }
  .dc-tabs button.active {
    color: var(--accent);
    box-shadow: inset 0 -2px 0 var(--accent);
  }
</style>
