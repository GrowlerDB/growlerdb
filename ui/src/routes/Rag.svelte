<script lang="ts">
  // Grounded retrieval ("Ask"): a question is hybrid-retrieved (BM25 + KNN, RRF-fused) over a
  // VECTOR field and the matching source passages are shown with their exact Iceberg coordinates as
  // citations. There is intentionally NO answer generation — GrowlerDB never sends text to an
  // external model; the value shipped is grounded retrieval with citations.
  import { onMount } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    listIndexes,
    describeIndex,
    searchHybrid,
    getByKey,
    hitId,
    type SearchHit,
    type VectorFieldInfo,
    type Coordinates,
  } from '../lib/api';
  import { read, persist } from '../lib/prefs';
  import Dropdown from '../lib/components/Dropdown.svelte';
  import Callout from '../lib/components/Callout.svelte';
  import Badge from '../lib/components/Badge.svelte';

  const SCOPE_KEY = 'growlerdb.ragIndex';
  const TOP_K = [4, 6, 8, 12];

  let indexOptions = $state<string[]>([]);
  let scopeIndex = $state('');
  let vectorFields = $state<VectorFieldInfo[]>([]);
  let vectorField = $state(''); // the selected VECTOR field path
  let question = $state('');
  let k = $state(6);

  let hits = $state<SearchHit[]>([]);
  let chunks = $state<string[]>([]); // source-text passage per hit, index-aligned
  let asked = $state(false);
  let loading = $state(false);
  let error = $state('');

  const selectedVec = $derived(vectorFields.find((v) => v.name === vectorField));
  const hasVector = $derived(vectorFields.length > 0);

  const scopeOptions = $derived(indexOptions.map((ix) => ({ value: ix, label: ix })));
  const vectorOptions = $derived(
    vectorFields.map((v) => ({
      value: v.name,
      label: v.dims ? `${v.name} · ${v.dims}d` : v.name,
    })),
  );
  const kOptions = TOP_K.map((n) => ({ value: String(n), label: String(n) }));

  /** Load the scoped index's vector fields from describe; default to the first one. */
  async function loadMeta() {
    try {
      const stats = await describeIndex(scopeIndex);
      vectorFields = stats?.vector_fields ?? [];
    } catch {
      vectorFields = [];
    }
    if (vectorFields.length > 0 && !vectorFields.some((v) => v.name === vectorField)) {
      vectorField = vectorFields[0].name;
    } else if (vectorFields.length === 0) {
      vectorField = '';
    }
  }

  onMount(async () => {
    try {
      indexOptions = (await listIndexes()).map((i) => i.name);
    } catch {
      indexOptions = [];
    }
    const saved = read(SCOPE_KEY);
    if (saved && indexOptions.includes(saved)) scopeIndex = saved;
    else if (indexOptions.length > 0) scopeIndex = indexOptions[0];
    await loadMeta();
  });

  async function onScopeChange() {
    persist(SCOPE_KEY, scopeIndex);
    await loadMeta();
  }

  /** Resolve each hit's source passage: prefer the cached field on the hit, else hydrate the
   *  authoritative row by key (governed) to read it. Index-aligned with `hits`. */
  async function resolveChunks(list: SearchHit[], sourceField: string): Promise<string[]> {
    const out: string[] = new Array(list.length).fill('');
    const missing: number[] = [];
    list.forEach((h, i) => {
      const v = h.fields?.[sourceField];
      if (v != null && v !== '') out[i] = String(v);
      else missing.push(i);
    });
    if (missing.length > 0) {
      try {
        const rows = await getByKey(
          missing.map((i) => list[i].coordinates ?? {}),
          [sourceField],
          scopeIndex || undefined,
        );
        missing.forEach((i, j) => {
          const val = rows[j]?.fields?.[sourceField];
          if (val != null) out[i] = String(val);
        });
      } catch {
        // best-effort — a passage with no resolvable text still shows its coordinates
      }
    }
    return out;
  }

  async function ask(event: Event) {
    event.preventDefault();
    if (!question.trim() || !vectorField) return;
    loading = true;
    error = '';
    asked = true;
    try {
      const res = await searchHybrid(question.trim(), {
        vectorField,
        k,
        index: scopeIndex || undefined,
      });
      hits = res.hits ?? [];
      chunks = selectedVec ? await resolveChunks(hits, selectedVec.source_field) : [];
    } catch (err) {
      error = String(err);
      hits = [];
      chunks = [];
    } finally {
      loading = false;
    }
  }

  /** Coordinate fields (partition + identifier) as `name=value` pairs for the citation. */
  function coordPairs(c: Coordinates | undefined): { name: string; value: string }[] {
    const pairs: { name: string; value: string }[] = [];
    for (const f of c?.partition ?? []) pairs.push({ name: f.name, value: String(f.value) });
    for (const f of c?.identifier ?? []) pairs.push({ name: f.name, value: String(f.value) });
    return pairs;
  }
</script>

<section aria-labelledby="rag-heading">
  <h1 id="rag-heading" class="sr-only">{t('rag.title')}</h1>
  <div class="screen-toolbar">
    <p class="sub">{t('rag.subtitle')}</p>
  </div>

  <form class="ask" onsubmit={ask} aria-label={t('rag.title')}>
    <div class="controls">
      {#if indexOptions.length > 0}
        <Dropdown
          bind:value={scopeIndex}
          options={scopeOptions}
          label={t('rag.scope')}
          ariaLabel={t('rag.scope')}
          mono
          onchange={onScopeChange}
        />
      {/if}
      {#if hasVector}
        <Dropdown
          bind:value={vectorField}
          options={vectorOptions}
          label={t('rag.vectorField')}
          ariaLabel={t('rag.vectorField')}
          mono
        />
        <Dropdown
          value={String(k)}
          options={kOptions}
          label={t('rag.topK')}
          ariaLabel={t('rag.topK')}
          onchange={(v) => (k = Number(v))}
        />
      {/if}
    </div>

    {#if hasVector}
      <div class="qline">
        <label for="rag-q" class="sr-only">{t('rag.question')}</label>
        <input
          id="rag-q"
          bind:value={question}
          placeholder={t('rag.questionPlaceholder')}
          autocomplete="off"
        />
        <button type="submit" class="primary" disabled={loading || !question.trim()}>
          {t('rag.ask')}
        </button>
      </div>
    {/if}
  </form>

  {#if !hasVector}
    <Callout>{t('rag.noVector')}</Callout>
  {:else if loading}
    <p class="muted">{t('common.loading')}</p>
  {:else if error}
    <p role="alert" class="error">{error}</p>
  {:else if !asked}
    <Callout>{t('rag.empty')}</Callout>
  {:else if hits.length === 0}
    <Callout>{t('rag.noResults')}</Callout>
  {:else}
    <p class="count">{t('rag.retrieved', { count: hits.length })}</p>
    <Callout>{t('rag.retrievalOnly')}</Callout>
    <ol class="citations">
      {#each hits as hit, i (hitId(hit) + ':' + i)}
        <li class="citation">
          <div class="cit-head">
            <span class="cit-n mono">{t('rag.citation', { n: i + 1 })}</span>
            <span class="cit-id mono">{hitId(hit)}</span>
            {#if hit.score != null}<Badge tone="default">{hit.score.toFixed(3)}</Badge>{/if}
          </div>
          <div class="coords" aria-label={t('rag.coordinates')}>
            {#each coordPairs(hit.coordinates) as p (p.name + ':' + p.value)}
              <span class="coord mono"><span class="coord-k">{p.name}</span>{p.value}</span>
            {/each}
          </div>
          {#if chunks[i]}
            <p class="chunk">{chunks[i]}</p>
          {/if}
        </li>
      {/each}
    </ol>
  {/if}
</section>

<style>
  .ask {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
    margin-bottom: 1rem;
  }
  .controls {
    display: flex;
    flex-wrap: wrap;
    gap: 0.6rem;
  }
  .qline {
    display: flex;
    gap: 0.6rem;
  }
  .qline input {
    flex: 1;
    min-width: 0;
    height: 36px;
    padding: 0 11px;
    border: 1px solid var(--line-strong);
    border-radius: 7px;
    background: var(--field);
    color: var(--text);
    font: inherit;
  }
  .qline input:focus {
    outline: none;
    border-color: var(--accent);
  }
  .count {
    font-weight: 600;
    margin: 0 0 0.6rem;
  }
  .citations {
    list-style: none;
    margin: 0.8rem 0 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
  }
  .citation {
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--panel);
    padding: 0.8rem 0.9rem;
  }
  .cit-head {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
  }
  .cit-n {
    color: var(--text-3);
    font-size: 0.8em;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .cit-id {
    font-weight: 600;
    color: var(--text);
  }
  .coords {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin: 0.5rem 0;
  }
  .coord {
    display: inline-flex;
    align-items: baseline;
    gap: 0.3rem;
    border: 1px solid var(--line);
    background: var(--panel2);
    border-radius: 5px;
    padding: 1px 7px;
    font-size: 0.82em;
  }
  .coord-k {
    color: var(--text-3);
  }
  .chunk {
    margin: 0.4rem 0 0;
    color: var(--text-2);
    line-height: 1.5;
    white-space: pre-wrap;
    word-break: break-word;
  }
</style>
