<script lang="ts">
  // A right-side slide-in drawer: the document drawer, etc. Backdrop click + Escape close.
  import type { Snippet } from 'svelte';
  let {
    title,
    onClose,
    eyebrow = '',
    width = '480px',
    children,
  }: {
    title: string;
    onClose: () => void;
    eyebrow?: string;
    width?: string;
    children: Snippet;
  } = $props();

  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape') onClose();
  }
</script>

<svelte:window onkeydown={onKey} />

<!-- Backdrop: a styling/close affordance; Escape + the × button are the keyboard paths. -->
<div class="dc-drawer-backdrop" onclick={onClose} role="presentation"></div>
<div
  class="dc-drawer"
  style="width:min({width},92vw)"
  role="dialog"
  aria-modal="true"
  aria-label={title}
>
  <header class="dc-drawer-head">
    <div>
      {#if eyebrow}<div class="dc-drawer-eyebrow mono">{eyebrow}</div>{/if}
      <h2>{title}</h2>
    </div>
    <button type="button" class="dc-drawer-close" onclick={onClose} aria-label="Close">×</button>
  </header>
  <div class="dc-drawer-body">{@render children()}</div>
</div>

<style>
  .dc-drawer-backdrop {
    position: fixed;
    inset: 0;
    z-index: 30;
    background: rgba(0, 0, 0, 0.18);
  }
  .dc-drawer {
    position: fixed;
    top: 0;
    right: 0;
    height: 100vh;
    z-index: 31;
    display: flex;
    flex-direction: column;
    background: var(--panel);
    border-left: 1px solid var(--line);
    box-shadow: -10px 0 30px var(--shadow);
    animation: gb-slide 0.16s ease-out;
  }
  .dc-drawer-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    padding: 13px 16px;
    border-bottom: 1px solid var(--line);
  }
  .dc-drawer-eyebrow {
    color: var(--text-3);
    font-size: 0.8em;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  .dc-drawer-head h2 {
    margin: 2px 0 0;
    font-size: 1.05em;
    letter-spacing: -0.01em;
  }
  .dc-drawer-close {
    border: 0;
    background: transparent;
    color: var(--text-2);
    font-size: 1.3rem;
    line-height: 1;
    cursor: pointer;
    padding: 0 4px;
  }
  .dc-drawer-close:hover {
    color: var(--text);
  }
  .dc-drawer-body {
    flex: 1;
    overflow: auto;
    padding: 14px 16px;
  }
</style>
