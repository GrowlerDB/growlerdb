<script lang="ts">
  import { highlightSegments, type Segment } from '../lib/highlight';
  import type { HighlightSegment } from '../lib/api';

  // Prefer server-side highlight `segments` (task-250) when present — they reflect the analyzed
  // match (stemming/positions). Otherwise fall back to client-side marking of the query `terms`
  // in `text` (task-46). Both render the same way: text runs, with matched runs in <mark>.
  let {
    text = '',
    terms = [],
    segments = undefined,
  }: { text?: string; terms?: string[]; segments?: HighlightSegment[] } = $props();

  let rendered = $derived<Segment[]>(
    segments
      ? segments.map((s) => ({ text: s.text, hit: s.marked }))
      : highlightSegments(text, terms),
  );
</script>

{#each rendered as seg, i (i)}{#if seg.hit}<mark>{seg.text}</mark>{:else}{seg.text}{/if}{/each}
