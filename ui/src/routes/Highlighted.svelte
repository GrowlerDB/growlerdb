<script lang="ts">
  import { highlightSegments } from '../lib/highlight';

  let { text, terms }: { text: string; terms: string[] } = $props();
  let segments = $derived(highlightSegments(text, terms));
</script>

{#each segments as seg, i (i)}{#if seg.hit}<mark>{seg.text}</mark>{:else}{seg.text}{/if}{/each}
