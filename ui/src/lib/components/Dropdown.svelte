<script lang="ts">
  // Styled dropdown button: replaces bare native <select>s on the chrome with a
  // button that matches the field styling (mono uppercase label + selected value + caret) and a
  // Popover listbox. Keyboard-operable: Enter/Space/ArrowDown open; arrows move; Enter/click select;
  // Escape closes (via Popover). `value` is bindable.
  import Popover from './Popover.svelte';

  let {
    value = $bindable(),
    options,
    label = '',
    ariaLabel = '',
    mono = false,
    width = 220,
    onchange,
  }: {
    value: string;
    options: { value: string; label: string }[];
    label?: string;
    ariaLabel?: string;
    mono?: boolean;
    width?: number;
    onchange?: (v: string) => void;
  } = $props();

  let open = $state(false);
  let btn = $state<HTMLElement | null>(null);
  let listEl = $state<HTMLUListElement | null>(null);
  const current = $derived(options.find((o) => o.value === value));

  function select(v: string) {
    open = false;
    btn?.focus();
    if (v !== value) {
      value = v;
      onchange?.(v);
    }
  }

  function onBtnKey(e: KeyboardEvent) {
    if (e.key === 'ArrowDown' || e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      open = true;
    }
  }

  function onListKey(e: KeyboardEvent) {
    if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
    e.preventDefault();
    const items = listEl
      ? (Array.from(listEl.querySelectorAll('button.dc-option')) as HTMLButtonElement[])
      : [];
    const idx = items.indexOf(document.activeElement as HTMLButtonElement);
    const next = e.key === 'ArrowDown' ? idx + 1 : idx - 1;
    (items[(next + items.length) % items.length] ?? items[0])?.focus();
  }

  // On open, move focus to the selected option (or the first) so arrows work immediately.
  $effect(() => {
    if (!open || !listEl) return;
    const active = listEl.querySelector('button.dc-option.active') as HTMLButtonElement | null;
    (active ?? (listEl.querySelector('button.dc-option') as HTMLButtonElement | null))?.focus();
  });
</script>

<button
  bind:this={btn}
  type="button"
  class="dc-select"
  aria-haspopup="listbox"
  aria-expanded={open}
  aria-label={ariaLabel || undefined}
  onclick={() => (open = !open)}
  onkeydown={onBtnKey}
>
  {#if label}<span class="dc-select-label">{label}</span>{/if}
  <span class="dc-select-value" class:mono>{current?.label ?? ''}</span>
  <span class="dc-select-caret" aria-hidden="true">▾</span>
</button>

{#if open && btn}
  <Popover anchor={btn} onClose={() => (open = false)} {width}>
    <ul class="dc-options" role="listbox" aria-label={ariaLabel || undefined} bind:this={listEl}>
      {#each options as o (o.value)}
        <li role="option" aria-selected={o.value === value}>
          <button
            type="button"
            class="dc-option"
            class:active={o.value === value}
            onclick={() => select(o.value)}
            onkeydown={onListKey}
          >
            <span class:mono>{o.label}</span>
            {#if o.value === value}<span class="dc-check" aria-hidden="true">✓</span>{/if}
          </button>
        </li>
      {/each}
    </ul>
  </Popover>
{/if}

<style>
  .dc-select {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    height: 36px;
    padding: 0 11px;
    border: 1px solid var(--line-strong);
    border-radius: 7px;
    background: var(--field);
    color: var(--text);
    cursor: pointer;
    font: inherit;
  }
  .dc-select:hover {
    border-color: var(--accent);
  }
  .dc-select-label {
    font:
      600 9px 'IBM Plex Mono',
      monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-3);
  }
  .dc-select-value {
    font-weight: 500;
    white-space: nowrap;
  }
  .dc-select-value.mono {
    font-family: 'IBM Plex Mono', ui-monospace, monospace;
    font-size: 0.92em;
  }
  .dc-select-caret {
    color: var(--text-3);
    font-size: 10px;
  }
  .dc-options {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .dc-option {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    width: 100%;
    text-align: left;
    border: 0;
    background: transparent;
    color: var(--text);
    font: inherit;
    padding: 6px 8px;
    border-radius: 6px;
    cursor: pointer;
  }
  .dc-option:hover,
  .dc-option:focus-visible {
    background: var(--accent-weakest);
  }
  .dc-option.active {
    color: var(--accent);
    font-weight: 600;
  }
  .dc-option .mono {
    font-family: 'IBM Plex Mono', ui-monospace, monospace;
    font-size: 0.92em;
  }
  .dc-check {
    color: var(--accent);
    flex-shrink: 0;
  }
</style>
