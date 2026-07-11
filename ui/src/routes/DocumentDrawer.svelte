<script lang="ts">
  // Slide-in panel with Fields / Explain / JSON tabs. Fields hydrates the authoritative
  // Iceberg row by key (GetByKey, governed). JSON shows the search hit. Explain calls the
  // engine for the real BM25 score tree + timings, loaded on demand.
  import { onMount } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    getByKey,
    explain,
    hitId,
    type SearchHit,
    type Row,
    type ExplainResult,
    type QuerySyntax,
  } from '../lib/api';
  import Highlighted from './Highlighted.svelte';
  import { fieldTerms, type ScopedTerms } from '../lib/highlight';
  import Drawer from '../lib/components/Drawer.svelte';
  import Tabs from '../lib/components/Tabs.svelte';
  import Icon from '../lib/components/Icon.svelte';
  import ExplainClauseTree from './ExplainClauseTree.svelte';

  let {
    hit,
    scoped,
    query = '',
    syntax = 'lucene',
    index = '',
    onClose,
  }: {
    hit: SearchHit;
    scoped: ScopedTerms;
    query?: string;
    syntax?: QuerySyntax;
    index?: string;
    onClose: () => void;
  } = $props();

  let row = $state<Row | null>(null);
  let loading = $state(true);
  let error = $state('');
  let tab = $state('fields');

  // Explain is computed lazily the first time its tab is opened (opt-in, per-hit).
  let exp = $state<ExplainResult | null>(null);
  let explaining = $state(false);
  let explainErr = $state('');
  let explainRequested = false;

  const tabs = [
    { id: 'fields', label: t('search.fields') },
    { id: 'explain', label: t('search.explain') },
    { id: 'json', label: t('search.json') },
  ];

  const json = $derived(
    JSON.stringify({ _key: hit.coordinates, _score: hit.score, fields: hit.fields ?? {} }, null, 2),
  );

  onMount(async () => {
    try {
      const rows = await getByKey([hit.coordinates ?? {}], [], index);
      row = rows[0] ?? null;
      if (!row) error = t('search.notFound');
    } catch (err) {
      error = String(err);
    } finally {
      loading = false;
    }
  });

  // Fetch the explanation the first time the Explain tab is shown.
  $effect(() => {
    if (tab === 'explain' && !explainRequested && hit.coordinates) {
      explainRequested = true;
      explaining = true;
      explain(query, hit.coordinates, syntax, index)
        .then((d) => (exp = d))
        .catch((e) => (explainErr = String(e)))
        .finally(() => (explaining = false));
    }
  });
</script>

<Drawer title={hitId(hit)} eyebrow={t('search.docEyebrow')} width="392px" {onClose}>
  <Tabs {tabs} bind:active={tab} />

  {#if tab === 'fields'}
    <p class="gov"><span class="gov-dot"></span>{t('search.governed')}</p>
    {#if loading}
      <p class="muted">{t('common.loading')}</p>
    {:else if error}
      <p role="alert" class="error">{error}</p>
    {:else if row}
      <dl class="dfields">
        {#each Object.entries(row.fields) as [name, value] (name)}
          <dt>{name}</dt>
          <dd>
            {#if typeof value === 'string'}
              <Highlighted text={value} terms={fieldTerms(scoped, name)} />
            {:else}
              {JSON.stringify(value)}
            {/if}
          </dd>
        {/each}
      </dl>
    {/if}
  {:else if tab === 'explain'}
    {#if explaining}
      <p class="muted">{t('common.loading')}</p>
    {:else if explainErr}
      <p role="alert" class="error">{explainErr}</p>
    {:else if exp}
      {#if !exp.found}
        <p class="muted">{t('search.explainNotFound')}</p>
      {:else if !exp.matched}
        <p class="muted">{t('search.explainNotMatched')}</p>
      {:else}
        <div class="ex-total">
          <span class="ex-score mono">{exp.score.toFixed(4)}</span>
          <span class="muted">{t('search.explainScore')}</span>
        </div>

        {@const idxMs = exp.timings.index_ms}
        {@const hydMs = exp.timings.hydration_ms}
        {@const sumMs = Math.max(idxMs + hydMs, 0.0001)}
        <section class="perf" aria-label={t('search.perfTitle')}>
          <h3 class="ex-h">{t('search.perfTitle')}</h3>
          <div
            class="perf-bar"
            role="img"
            aria-label={t('search.perfBar', {
              index: idxMs.toFixed(1),
              hydration: hydMs.toFixed(1),
            })}
          >
            <span class="seg idx" style="width:{(idxMs / sumMs) * 100}%"></span>
            <span class="seg hyd" style="width:{(hydMs / sumMs) * 100}%"></span>
          </div>
          <div class="perf-phases">
            <div class="perf-row">
              <span class="swatch idx"></span>
              <span class="perf-key">
                <span class="perf-title"
                  >{t('search.perfIndex')}
                  <span class="muted">· {t('search.perfIndexSub')}</span></span
                >
                <span class="perf-sub"
                  >{t('search.perfIndexNote', {
                    scanned: exp.shards_scanned,
                    total: exp.shards_total,
                  })}</span
                >
              </span>
              <span class="perf-val mono">{idxMs.toFixed(1)} ms</span>
            </div>
            <div class="perf-row">
              <span class="swatch hyd"></span>
              <span class="perf-key">
                <span class="perf-title"
                  >{t('search.perfHydration')}
                  <span class="muted">· {t('search.perfHydrationSub')}</span></span
                >
                <span class="perf-sub">{t('search.perfHydrationNote')}</span>
              </span>
              <span class="perf-val mono">{hydMs.toFixed(1)} ms</span>
            </div>
          </div>
          <div class="perf-total">
            <span class="perf-key"><Icon name="stopwatch" /> {t('search.perfTotal')}</span>
            <span class="perf-val accent mono">{exp.timings.total_ms.toFixed(1)} ms</span>
          </div>
          <p class="perf-note">{t('search.perfNote')}</p>
        </section>

        {#if exp.analyzed.length > 0}
          <h3 class="ex-h">{t('search.explainAnalyzed')}</h3>
          <div class="ex-analyzed">
            {#each exp.analyzed as a (a.field)}
              <div class="ex-af">
                <span class="ex-field">{a.field}</span>
                {#each a.terms as term (term)}<span class="ex-term mono">{term}</span>{/each}
              </div>
            {/each}
          </div>
        {/if}

        {#if exp.detail}
          <h3 class="ex-h">{t('search.explainBreakdown')}</h3>
          <ExplainClauseTree clause={exp.detail} />
        {/if}
      {/if}
    {/if}
  {:else}
    <pre class="json mono">{json}</pre>
  {/if}
</Drawer>

<style>
  .gov {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    color: var(--text-3);
    font-size: 0.9em;
    margin: 0.6rem 0;
  }
  .gov-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--ok);
  }
  .dfields {
    margin: 0;
  }
  .dfields dt {
    color: var(--text-3);
    font-size: 0.8em;
    margin-top: 0.7rem;
  }
  .dfields dd {
    margin: 0.1rem 0 0;
    word-break: break-word;
    font-family: 'IBM Plex Mono', ui-monospace, monospace;
    font-size: 0.95em;
  }
  .ex-total {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    margin: 0.7rem 0 0.3rem;
  }
  .ex-score {
    font-size: 1.6em;
    font-weight: 600;
    color: var(--accent);
  }
  .perf {
    margin: 0.5rem 0 0.7rem;
  }
  .perf-bar {
    display: flex;
    height: 9px;
    border-radius: 5px;
    overflow: hidden;
    background: var(--panel2);
    margin: 0.6rem 0 0.7rem;
  }
  .perf-bar .seg.idx {
    background: var(--accent);
  }
  .perf-bar .seg.hyd {
    background: var(--text-3);
  }
  .perf-phases {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
  }
  .perf-row {
    display: flex;
    align-items: flex-start;
    gap: 0.6rem;
  }
  .swatch {
    width: 8px;
    height: 8px;
    border-radius: 2px;
    margin-top: 4px;
    flex-shrink: 0;
  }
  .swatch.idx {
    background: var(--accent);
  }
  .swatch.hyd {
    background: var(--text-3);
  }
  .perf-key {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .perf-title {
    color: var(--text);
  }
  .perf-sub {
    color: var(--text-3);
    font-size: 0.85em;
  }
  .perf-val {
    font-weight: 600;
    flex-shrink: 0;
  }
  .perf-val.accent {
    color: var(--accent);
  }
  .perf-total {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    margin-top: 0.6rem;
    padding-top: 0.6rem;
    border-top: 1px solid var(--line);
    font-weight: 600;
  }
  .perf-total .perf-key {
    flex-direction: row;
    align-items: center;
    gap: 0.4rem;
  }
  .perf-note {
    color: var(--text-3);
    font-size: 0.8em;
    line-height: 1.45;
    margin: 0.7rem 0 0;
    background: var(--panel2);
    border: 1px solid var(--line);
    border-radius: 7px;
    padding: 0.5rem 0.6rem;
  }
  .ex-h {
    margin: 0.9rem 0 0.4rem;
    font-size: 0.82em;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--text-3);
  }
  .ex-analyzed {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }
  .ex-af {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.3rem;
  }
  .ex-field {
    color: var(--text-3);
    font-size: 0.85em;
    margin-right: 0.2rem;
  }
  .ex-term {
    background: var(--accent-weakest);
    border: 1px solid var(--line);
    border-radius: 5px;
    padding: 1px 6px;
    font-size: 0.85em;
  }
  .json {
    margin: 0.6rem 0 0;
    padding: 0.7rem;
    background: var(--panel2);
    border: 1px solid var(--line);
    border-radius: 7px;
    font-size: 0.85em;
    overflow: auto;
    white-space: pre-wrap;
    word-break: break-word;
  }
</style>
