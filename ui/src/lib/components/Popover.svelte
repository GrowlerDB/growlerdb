<script lang="ts">
  // An anchored popover (task-93): the time filter, user menu, etc. Positioned from a trigger's
  // bounding rect (pure math in `lib/popover.ts`), with a full-screen click-away overlay. Escape
  // closes. Pass the trigger element as `anchor`; render content in the default slot.
  import type { Snippet } from 'svelte';
  import { popoverPlacement } from '../popover';

  let {
    anchor,
    onClose,
    width = 280,
    children,
  }: {
    anchor: HTMLElement | null;
    onClose: () => void;
    width?: number;
    children: Snippet;
  } = $props();

  let panel = $state<HTMLDivElement | null>(null);
  let pos = $state<{ top: number; left: number }>({ top: -9999, left: -9999 });

  function place() {
    if (!anchor) return;
    const a = anchor.getBoundingClientRect();
    const h = panel?.offsetHeight ?? 0;
    const p = popoverPlacement(a, width, h, {
      width: window.innerWidth,
      height: window.innerHeight,
    });
    pos = { top: p.top, left: p.left };
  }

  $effect(() => {
    // Re-place when the anchor changes or the panel measures; also on resize/scroll.
    place();
    const on = () => place();
    window.addEventListener('resize', on);
    window.addEventListener('scroll', on, true);
    return () => {
      window.removeEventListener('resize', on);
      window.removeEventListener('scroll', on, true);
    };
  });

  function onKey(e: KeyboardEvent) {
    if (e.key === 'Escape') onClose();
  }
</script>

<svelte:window onkeydown={onKey} />

<div class="dc-pop-overlay" onclick={onClose} role="presentation"></div>
<div
  bind:this={panel}
  class="dc-pop"
  style="top:{pos.top}px;left:{pos.left}px;width:{width}px"
  role="dialog"
>
  {@render children()}
</div>

<style>
  .dc-pop-overlay {
    position: fixed;
    inset: 0;
    z-index: 40;
  }
  .dc-pop {
    position: fixed;
    z-index: 41;
    background: var(--panel);
    border: 1px solid var(--line-strong);
    border-radius: 10px;
    box-shadow: 0 10px 30px var(--shadow);
    padding: 10px;
    animation: gb-slide 0.14s ease-out;
    max-height: 80vh;
    overflow: auto;
  }
</style>
