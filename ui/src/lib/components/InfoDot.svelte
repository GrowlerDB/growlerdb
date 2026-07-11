<script lang="ts">
  // Self-serve help affordance: a small ⓘ button that opens a Popover describing a metric/panel —
  // what it is, and (optionally) what an elevated value means. Keeps the cards themselves clean.
  import Popover from './Popover.svelte';

  let {
    title,
    body,
    hint = '',
  }: {
    /** Short name of the thing being explained (usually the card label). */
    title: string;
    /** Plain-language "what this is". */
    body: string;
    /** Optional "what elevated/low means" diagnostic line. */
    hint?: string;
  } = $props();

  let btn = $state<HTMLElement | null>(null);
  let open = $state(false);
</script>

<button
  bind:this={btn}
  class="dc-info"
  type="button"
  aria-label={`About ${title}`}
  aria-expanded={open}
  onclick={(e) => {
    e.stopPropagation();
    open = !open;
  }}
>
  <svg width="13" height="13" viewBox="0 0 16 16" fill="none" aria-hidden="true">
    <circle cx="8" cy="8" r="6.4" stroke="currentColor" stroke-width="1.3" />
    <circle cx="8" cy="5.1" r="0.95" fill="currentColor" />
    <line
      x1="8"
      y1="7.4"
      x2="8"
      y2="11.4"
      stroke="currentColor"
      stroke-width="1.4"
      stroke-linecap="round"
    />
  </svg>
</button>

{#if open}
  <Popover anchor={btn} onClose={() => (open = false)} width={264}>
    <div class="dc-info-pop">
      <p class="dc-info-title">{title}</p>
      <p class="dc-info-body">{body}</p>
      {#if hint}
        <p class="dc-info-hint">{hint}</p>
      {/if}
    </div>
  </Popover>
{/if}

<style>
  .dc-info {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    padding: 0;
    border: 0;
    border-radius: 50%;
    background: transparent;
    color: var(--text-3);
    cursor: pointer;
  }
  .dc-info:hover {
    color: var(--accent);
    background: var(--accent-weakest);
  }
  .dc-info-pop {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .dc-info-title {
    margin: 0;
    font-weight: 600;
    font-size: 0.82rem;
    color: var(--text);
  }
  .dc-info-body {
    margin: 0;
    font-size: 0.8rem;
    line-height: 1.45;
    color: var(--text-2);
  }
  .dc-info-hint {
    margin: 0;
    font-size: 0.76rem;
    line-height: 1.4;
    color: var(--text-3);
    border-top: 1px solid var(--line);
    padding-top: 6px;
  }
</style>
