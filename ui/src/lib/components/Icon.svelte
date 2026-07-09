<script lang="ts">
  // Small inline stroke icons (console revamp) — replace the legacy emoji (🕑/⚠/⏱) with crisp,
  // currentColor-tinted SVGs. Stroke weight 1.5 matches the Sparkline/nav line weight. Decorative
  // by default (aria-hidden); pair with visible text or an aria-label on the control.
  let { name, size = 14 }: { name: 'clock' | 'warn' | 'stopwatch'; size?: number } = $props();
</script>

<svg
  class="icon"
  width={size}
  height={size}
  viewBox="0 0 16 16"
  fill="none"
  stroke="currentColor"
  stroke-width="1.5"
  stroke-linecap="round"
  stroke-linejoin="round"
  aria-hidden="true"
>
  {#if name === 'clock'}
    <circle cx="8" cy="8" r="6" />
    <path d="M8 4.7V8l2.3 1.4" />
  {:else if name === 'stopwatch'}
    <circle cx="8" cy="9.2" r="5.1" />
    <path d="M8 9.2V6.4M6.4 1.8h3.2M12.5 4.1l1 1" />
  {:else if name === 'warn'}
    <path d="M8 2.5 14.2 13.3a.6.6 0 0 1-.52.9H2.32a.6.6 0 0 1-.52-.9z" />
    <path d="M8 6.7v3.2" />
    <circle cx="8" cy="11.9" r="0.05" />
  {/if}
</svg>

<style>
  .icon {
    flex-shrink: 0;
    vertical-align: -2px;
  }
</style>
