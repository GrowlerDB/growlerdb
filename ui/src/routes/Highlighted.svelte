<script lang="ts">
  import { highlightSegments, type Segment } from '../lib/highlight';
  import type { HighlightSegment } from '../lib/api';

  // Prefer server-side `segments` (they reflect the analyzed match: stemming/positions); else fall
  // back to client-side marking of `terms` in `text`. Both render as text runs, matched in <mark>.
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
