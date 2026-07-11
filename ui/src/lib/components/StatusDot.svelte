<script lang="ts">
  // A small status dot: ok/warn/accent/muted, optionally pulsing (e.g. the header health pill,
  // "rebuilding" shards).
  let {
    tone = 'ok',
    pulse = false,
    title = '',
  }: {
    tone?: 'ok' | 'warn' | 'accent' | 'muted';
    pulse?: boolean;
    title?: string;
  } = $props();
</script>

<span class="dc-dot {tone}" class:pulse {title} aria-hidden={title ? undefined : 'true'}></span>

<style>
  .dc-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--text-3);
    flex: 0 0 auto;
  }
  .dc-dot.ok {
    background: var(--ok);
  }
  .dc-dot.warn {
    background: var(--warn);
  }
  .dc-dot.accent {
    background: var(--accent);
  }
  .dc-dot.pulse {
    animation: gb-pulse 2.6s ease-in-out infinite;
  }
</style>
