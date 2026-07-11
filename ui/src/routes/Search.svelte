<script lang="ts">
  import { onMount } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    search,
    suggest,
    facets,
    listIndexes,
    describeIndex,
    hitId,
    type SearchHit,
    type Suggestion,
    type QuerySyntax,
    type SortKey,
    type FacetGroup,
    type HighlightSegment,
  } from '../lib/api';
  import { currentFieldToken, withCompletion } from '../lib/autocomplete';
  import { queryTerms } from '../lib/highlight';
  import { formatEpochMicros } from '../lib/results';
  import Highlighted from './Highlighted.svelte';
  import { toJson, toCsv, download } from '../lib/export';
  import { read, persist } from '../lib/prefs';
  import {
    loadSavedSearches,
    saveSearch,
    removeSearch,
    type SavedSearch,
  } from '../lib/savedQueries';
  import Segmented from '../lib/components/Segmented.svelte';
  import Callout from '../lib/components/Callout.svelte';
  import Popover from '../lib/components/Popover.svelte';
  import Icon from '../lib/components/Icon.svelte';
  import Dropdown from '../lib/components/Dropdown.svelte';
  import DocumentDrawer from './DocumentDrawer.svelte';

  // Collapsible filters rail (design-QA T9): the collapsed state persists across reloads via prefs.
  const RAIL_KEY = 'growlerdb.searchRailCollapsed';
  let railCollapsed = $state(read(RAIL_KEY) === '1');
  function toggleRail() {
    railCollapsed = !railCollapsed;
    persist(RAIL_KEY, railCollapsed ? '1' : '0');
  }

  const PAGE_SIZE = 10;

  let query = $state('');
  let syntax = $state<QuerySyntax>('lucene');
  let lastQuery = $state('');
  let terms = $state<string[]>([]);
  let hits = $state<SearchHit[]>([]);
  let total = $state(0);
  let partial = $state(false);
  let partialDismissed = $state(false); // task-133: the partial-results banner is dismissible
  let elapsedMs = $state<number | null>(null); // client-measured query round-trip (task-133)
  let shardsScanned = $state(0); // shards the Gateway queried (task-133); 0 = bare Node, no scope
  let shardsTotal = $state(0); // the index's full shard count
  let offset = $state(0);
  let loading = $state(false);
  let error = $state('');
  let searched = $state(false);
  let selected = $state<SearchHit | null>(null);
  let saved = $state<SavedSearch[]>([]);

  // Per-index scoping + sort + keyset paging (task-99).
  let indexOptions = $state<string[]>([]);
  let scopeIndex = $state(''); // '' = the index this endpoint serves
  const SEARCH_INDEX_KEY = 'growlerdb.searchIndex'; // remember the last chosen index (task-131)
  let sortField = $state(''); // '' = relevance (_score)
  let cursor = $state<string | undefined>(undefined); // next_cursor for keyset "Load more"
  // Monotonic search generation (task-153 / I9): overlapping searches (typing + facet/sort changes)
  // race, so a slow earlier response must not clobber a fresh later one. Each run bumps it; a
  // response is applied only if it's still the latest. Not reactive — a plain guard.
  let searchSeq = 0;

  // Sorted by a field ⇒ keyset (search_after) scrolling with "Load more"; relevance ⇒ offset pager.
  const sorted = $derived(sortField !== '');
  const sortKeys = $derived<SortKey[] | undefined>(
    sorted ? [{ field: sortField, desc: true }] : undefined,
  );
  // Field names available to sort by — the cached display fields present on the current hits.
  const sortableFields = $derived.by(() => {
    const names = new Set<string>();
    for (const h of hits) for (const k of Object.keys(h.fields ?? {})) names.add(k);
    return [...names].sort();
  });

  // Option lists for the styled dropdowns (design-QA T5).
  const scopeOptions = $derived([
    { value: '', label: t('search.allIndexes') },
    ...indexOptions.map((ix) => ({ value: ix, label: ix })),
  ]);
  const sortOptions = $derived([
    { value: '', label: t('search.sortScore') },
    ...sortableFields.map((f) => ({ value: f, label: f })),
  ]);
  // The active query syntax, shown as a pill inside the query field (design-QA T10).
  const syntaxLabel = $derived(syntax === 'kql' ? t('search.kql') : t('search.lucene'));

  // Time filter (task-101): the index's DATE columns + the chosen field/range. A resolved range is
  // ANDed into the query as `field:[fromUs TO toUs]` in canonical epoch **micros** (task-112) — the
  // unit DATE columns are indexed/range-queried in — which the gateway also uses to prune windows.
  let timeFields = $state<string[]>([]);
  let timeField = $state('');
  let timePreset = $state(''); // '' = any; else a TIME_PRESETS id
  let timeFrom = $state(''); // datetime-local strings (custom range)
  let timeTo = $state('');
  let timeOpen = $state(false);
  let timeBtn = $state<HTMLElement | null>(null);

  const TIME_PRESETS = [
    { id: '1h', label: t('search.timeHour'), ms: 3600_000 },
    { id: '24h', label: t('search.timeDay'), ms: 86_400_000 },
    { id: '7d', label: t('search.timeWeek'), ms: 7 * 86_400_000 },
    { id: '30d', label: t('search.timeMonth'), ms: 30 * 86_400_000 },
    { id: 'custom', label: t('search.timeCustom'), ms: 0 },
  ];

  /** The active time window in epoch-ms, or null when no time filter is applied. */
  function timeRange(): { from: number; to: number } | null {
    if (!timeField || !timePreset) return null;
    if (timePreset === 'custom') {
      const from = Date.parse(timeFrom);
      const to = Date.parse(timeTo);
      if (Number.isNaN(from) || Number.isNaN(to)) return null;
      return { from, to };
    }
    const p = TIME_PRESETS.find((x) => x.id === timePreset);
    if (!p) return null;
    const now = Date.now();
    return { from: now - p.ms, to: now };
  }
  const timeLabel = $derived.by(() => {
    if (!timeField || !timePreset) return t('search.time');
    const p = TIME_PRESETS.find((x) => x.id === timePreset);
    return `${timeField} · ${p?.label ?? ''}`;
  });
  function clearTime() {
    timePreset = '';
    timeFrom = '';
    timeTo = '';
    if (searched) run(0);
  }
  function applyTime() {
    timeOpen = false;
    run(0);
  }

  /** Load the selected index's DATE columns so the time filter can list them (task-101). The
   *  backend populates `time_fields` on every describe path — including the default-served index
   *  (empty `scopeIndex`) — so an empty list means the index genuinely has no DATE column and the
   *  time filter stays (correctly) disabled, rather than us re-deriving it from the mapping. */
  async function loadTimeFields() {
    try {
      const stats = await describeIndex(scopeIndex);
      timeFields = stats?.time_fields ?? [];
      if (timeFields.length > 0 && !timeFields.includes(timeField)) timeField = timeFields[0];
    } catch {
      timeFields = [];
    }
  }

  onMount(async () => {
    try {
      indexOptions = (await listIndexes()).map((i) => i.name);
    } catch {
      indexOptions = []; // no control plane fronted here → scope selector hidden; serve default
    }
    // Restore the last chosen index (task-131), but only if it still exists. Otherwise default to the
    // first served index (task-248): a multi-index endpoint rejects an index-less search, so the UI
    // must never send one — pick a real index rather than show "index required". (Empty options = a
    // single-index endpoint with no control plane fronted → leave '' to use the served default.)
    const savedIndex = read(SEARCH_INDEX_KEY);
    if (savedIndex && indexOptions.includes(savedIndex)) scopeIndex = savedIndex;
    else if (indexOptions.length > 0) scopeIndex = indexOptions[0];
    await loadTimeFields();
    try {
      saved = await loadSavedSearches();
    } catch {
      saved = []; // saved searches are best-effort; never block the screen
    }
  });

  const syntaxOptions = [
    { value: 'lucene', label: t('search.lucene') },
    { value: 'kql', label: t('search.kql') },
  ];

  // Query autocomplete (task-88): suggest values for the `field:prefix` token being typed.
  let completions = $state<Suggestion[]>([]);
  let acActive = $state(-1); // highlighted completion (-1 = none)
  let acSeq = 0; // request race guard
  let acTimer: ReturnType<typeof setTimeout> | undefined;
  let acOpen = $derived(completions.length > 0);

  function closeAutocomplete() {
    completions = [];
    acActive = -1;
    clearTimeout(acTimer);
  }

  function onInput() {
    clearTimeout(acTimer);
    const token = currentFieldToken(query);
    if (!token) {
      closeAutocomplete();
      return;
    }
    const seq = ++acSeq;
    acTimer = setTimeout(async () => {
      // Scope autocomplete to the selected index on a multi-index endpoint (task-240).
      const results = await suggest(token.field, token.prefix, 8, scopeIndex || undefined);
      if (seq !== acSeq) return; // a newer keystroke superseded this request
      completions = results;
      acActive = -1;
    }, 150);
  }

  function applyCompletion(value: string) {
    const token = currentFieldToken(query);
    if (token) query = withCompletion(query, token, value);
    closeAutocomplete();
  }

  function onQueryKeydown(event: KeyboardEvent) {
    if (!acOpen) return;
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      acActive = (acActive + 1) % completions.length;
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      acActive = (acActive - 1 + completions.length) % completions.length;
    } else if (event.key === 'Enter' && acActive >= 0) {
      event.preventDefault(); // accept the completion instead of submitting the search
      applyCompletion(completions[acActive].text);
    } else if (event.key === 'Escape') {
      event.preventDefault();
      closeAutocomplete();
    }
  }

  /** A facet selection, rendered as a removable chip and ANDed into the query as a filter clause. */
  let filters = $state<{ field: string; value: string }[]>([]);
  let facetGroups = $state<FacetGroup[]>([]);
  let collapsedFacets = $state<Set<string>>(new Set()); // task-133: per-group collapse
  function toggleFacetGroup(field: string) {
    const next = new Set(collapsedFacets);
    if (next.has(field)) next.delete(field);
    else next.add(field);
    collapsedFacets = next;
  }

  /** A single filter as a Lucene phrase clause, escaping `"`/`\` in the value. */
  function clauseOf(f: { field: string; value: string }): string {
    return `${f.field}:"${f.value.replace(/(["\\])/g, '\\$1')}"`;
  }
  /** The query actually sent: the typed query ANDed with each active filter (score-neutral). */
  function effectiveQuery(): string {
    const parts: string[] = [];
    if (query.trim()) parts.push(`(${query.trim()})`);
    const r = timeRange();
    // DATE columns are indexed in epoch micros (task-112); timeRange() is epoch ms → ×1000. Pruned
    // server-side (task-81).
    if (r) parts.push(`${timeField}:[${r.from * 1000} TO ${r.to * 1000}]`);
    for (const f of filters) parts.push(clauseOf(f));
    return parts.join(' AND ');
  }
  function isActive(field: string, value: string): boolean {
    return filters.some((f) => f.field === field && f.value === value);
  }

  /** Run a fresh search (page 1). Resets the keyset cursor; sorted runs scroll via {@link loadMore}. */
  async function run(from = 0) {
    const eq = effectiveQuery();
    if (!eq) return;
    const seq = ++searchSeq; // this run's generation
    loading = true;
    error = '';
    selected = null;
    partialDismissed = false;
    try {
      const t0 = performance.now();
      const res = await search(eq, {
        limit: PAGE_SIZE,
        offset: sorted ? 0 : from, // keyset paging ignores offset; relevance uses it
        syntax,
        index: scopeIndex || undefined,
        sort: sortKeys,
        highlight: true, // server-side highlights (task-250); falls back to client marking when absent
      });
      if (seq !== searchSeq) return; // a newer search started — discard this stale response
      elapsedMs = Math.round(performance.now() - t0);
      hits = res.hits ?? [];
      total = res.total ?? 0;
      partial = res.partial ?? false;
      shardsScanned = res.shards_scanned ?? 0;
      shardsTotal = res.shards_total ?? 0;
      cursor = res.next_cursor;
      offset = sorted ? 0 : from;
      lastQuery = eq; // the effective query (base + filters + time) — used by the drawer's Explain
      terms = queryTerms(eq);
      searched = true;
      void refreshFacets(eq, hits, seq);
    } catch (err) {
      if (seq === searchSeq) error = String(err); // don't surface a stale run's error
    } finally {
      if (seq === searchSeq) loading = false; // only the latest run owns the spinner
    }
  }

  /** Recompute left-rail facets for the current (filtered) query over the result's cached fields. */
  async function refreshFacets(eq: string, sourceHits: SearchHit[], seq: number) {
    const fields = new Set<string>();
    for (const h of sourceHits) for (const k of Object.keys(h.fields ?? {})) fields.add(k);
    for (const f of filters) fields.add(f.field); // keep an active facet visible even if filtered out
    if (fields.size === 0) {
      if (seq === searchSeq) facetGroups = [];
      return;
    }
    const res = await facets(eq, [...fields]);
    if (seq !== searchSeq) return; // discard facets for a superseded search
    // Hide degenerate facets: a group whose every bucket has count 1 groups nothing — clicking a
    // value just narrows to a single hit (e.g. unique numerics like `views`), so it's noise, not a
    // filter. Keep a group only if some value groups ≥2 hits, or the user is already filtering on it.
    const activeFields = new Set(filters.map((f) => f.field));
    facetGroups = res.facets.filter(
      (g) => activeFields.has(g.field) || g.buckets.some((b) => b.count >= 2),
    );
  }

  /** Toggle a facet value: add it as a filter (or remove it if already active), then re-run. */
  function toggleFacet(field: string, value: string) {
    filters = isActive(field, value)
      ? filters.filter((f) => !(f.field === field && f.value === value))
      : [...filters, { field, value }];
    run(0);
  }
  function removeFilter(i: number) {
    filters = filters.filter((_, idx) => idx !== i);
    run(0);
  }

  /** Append the next keyset page (sorted runs only) using the prior response's `next_cursor`. */
  async function loadMore() {
    if (!cursor || loading) return;
    const seq = searchSeq; // continue the current search generation (task-153 / I9)
    loading = true;
    error = '';
    try {
      const res = await search(effectiveQuery(), {
        limit: PAGE_SIZE,
        syntax,
        index: scopeIndex || undefined,
        sort: sortKeys,
        cursor,
        highlight: true, // keep highlights on "Load more" pages consistent (task-250)
      });
      if (seq !== searchSeq) return; // a new search replaced these results — don't append a stale page
      hits = [...hits, ...(res.hits ?? [])];
      partial = partial || (res.partial ?? false);
      cursor = res.next_cursor;
    } catch (err) {
      if (seq === searchSeq) error = String(err);
    } finally {
      if (seq === searchSeq) loading = false;
    }
  }

  function onSubmit(event: Event) {
    event.preventDefault();
    closeAutocomplete();
    run(0);
  }

  /** Re-run from page 1 when the sort field changes. */
  function rerun() {
    if (searched) run(0);
  }

  /** Scope index changed: reload its time fields, then re-run. */
  async function onScopeChange() {
    persist(SEARCH_INDEX_KEY, scopeIndex); // remember the choice across reloads (task-131)
    await loadTimeFields();
    rerun();
  }

  function page(delta: number) {
    const next = Math.max(0, offset + delta * PAGE_SIZE);
    if (next !== offset) run(next);
  }

  function exportRows() {
    return hits.map((h) => ({ id: hitId(h), score: h.score ?? 0 }));
  }
  function exportJson() {
    download('growlerdb-results.json', toJson(exportRows()), 'application/json');
  }
  function exportCsv() {
    download('growlerdb-results.csv', toCsv(exportRows()), 'text/csv');
  }

  /** Capture the full current search state so restoring re-applies index/sort/filters/time (task-106). */
  async function save() {
    const name = query.trim() || filters.map((f) => `${f.field}:${f.value}`).join(' ') || 'query';
    const state = {
      query,
      syntax,
      index: scopeIndex,
      sort: sortField,
      filters: [...filters],
      timeField,
      timePreset,
      timeFrom,
      timeTo,
    };
    try {
      saved = await saveSearch({ name, query, state });
    } catch (err) {
      error = String(err);
    }
  }

  /** Restore a saved search: re-apply every facet of its state, then run it. */
  async function loadQuery(s: SavedSearch) {
    const st = s.state ?? { query: s.query, syntax: 'lucene' };
    query = st.query ?? s.query;
    syntax = (st.syntax as typeof syntax) ?? 'lucene';
    scopeIndex = st.index ?? '';
    sortField = st.sort ?? '';
    filters = st.filters ? [...st.filters] : [];
    timeField = st.timeField ?? timeField;
    timePreset = st.timePreset ?? '';
    timeFrom = st.timeFrom ?? '';
    timeTo = st.timeTo ?? '';
    await loadTimeFields();
    run(0);
  }

  async function remove(s: SavedSearch) {
    try {
      saved = await removeSearch(s);
    } catch (err) {
      error = String(err);
    }
  }

  let pageEnd = $derived(Math.min(offset + hits.length, total));

  /** Column set for the results table (task-86): every cached field name across the page, in
   *  first-seen order — so every row is the same shape (score · id · one cell per field). */
  let columns = $derived.by(() => {
    const order: string[] = [];
    const seen = new Set<string>();
    for (const h of hits) {
      for (const k of Object.keys(h.fields ?? {})) {
        if (!seen.has(k)) {
          seen.add(k);
          order.push(k);
        }
      }
    }
    return order;
  });

  /** Header label for the identifier column — the index's key field name (fallback `id`). */
  let keyLabel = $derived(hits[0]?.coordinates?.identifier?.[0]?.name ?? 'id');

  /** One cell's display value: DATE/time-field columns render as formatted UTC (task-133); every
   *  other field renders its raw cached value, preferring the server highlight fragment when the
   *  gateway returned one for this field (task-250), else client-side term marking. */
  function cellText(hit: SearchHit, col: string): { text: string; segments?: HighlightSegment[] } {
    const raw = hit.fields?.[col];
    if (timeFields.includes(col)) {
      const formatted = formatEpochMicros(raw);
      if (formatted) return { text: formatted };
    }
    return { text: String(raw ?? ''), segments: hit.highlight?.[col]?.[0] };
  }
</script>

<section class="search" aria-labelledby="screen-heading">
  <h1 id="screen-heading" class="sr-only">{t('search.title')}</h1>

  <!-- BAND 1: full-bleed query strip (design-QA T2). -->
  <div class="qstrip">
    <form role="search" class="qbar" onsubmit={onSubmit}>
      <div class="qrow">
        {#if indexOptions.length > 0}
          <Dropdown
            bind:value={scopeIndex}
            options={scopeOptions}
            label={t('search.scope')}
            ariaLabel={t('search.scope')}
            mono
            onchange={onScopeChange}
          />
        {/if}
        <label for="query" class="sr-only">{t('search.title')}</label>
        <div class="qfield">
          <svg
            class="qfield-icon"
            width="15"
            height="15"
            viewBox="0 0 16 16"
            fill="none"
            stroke="var(--text-3)"
            stroke-width="1.5"
            aria-hidden="true"
            ><circle cx="7" cy="7" r="4.3"></circle><line
              x1="10.3"
              y1="10.3"
              x2="14"
              y2="14"
              stroke-linecap="round"
            ></line></svg
          >
          <div class="ac">
            <input
              id="query"
              name="query"
              class="mono"
              bind:value={query}
              oninput={onInput}
              onkeydown={onQueryKeydown}
              onblur={() => setTimeout(closeAutocomplete, 120)}
              placeholder={t('search.placeholder')}
              autocomplete="off"
              role="combobox"
              aria-expanded={acOpen}
              aria-controls="ac-list"
              aria-autocomplete="list"
              aria-activedescendant={acActive >= 0 ? `ac-opt-${acActive}` : undefined}
            />
            {#if acOpen}
              <ul class="ac-list" id="ac-list" role="listbox" aria-label={t('search.suggestions')}>
                {#each completions as c, i (c.text)}
                  <li
                    id="ac-opt-{i}"
                    role="option"
                    aria-selected={i === acActive}
                    class:active={i === acActive}
                  >
                    <button type="button" tabindex="-1" onmousedown={() => applyCompletion(c.text)}>
                      <span class="ac-text mono">{c.text}</span>
                      <span class="muted ac-count">{c.count}</span>
                    </button>
                  </li>
                {/each}
              </ul>
            {/if}
          </div>
          <!-- Active-syntax pill inside the field (design-QA T10). -->
          <span class="syntax-pill mono">{syntaxLabel}</span>
        </div>
        <Segmented options={syntaxOptions} bind:value={syntax} label={t('search.syntax')} />
        <button class="primary" type="submit" disabled={loading}>
          {t('search.run')}
        </button>
      </div>
    </form>
  </div>

  <!-- BAND 2: stats band spanning rail + results (design-QA T2). -->
  <div class="statbar">
    <button
      type="button"
      class="time-btn"
      class:on={timeRange() !== null}
      bind:this={timeBtn}
      onclick={() => (timeOpen = !timeOpen)}
      disabled={timeFields.length === 0}
      title={timeFields.length > 0
        ? t('search.timeDetected', { count: timeFields.length })
        : t('search.timeNone')}
      aria-label={timeFields.length === 0 ? t('search.timeNone') : undefined}
    >
      <Icon name="clock" />
      {timeLabel}
    </button>
    {#if searched && !error}
      <p class="count">
        {t('search.results', { count: total })}
        {#if elapsedMs != null}<span class="muted"> · {elapsedMs}ms</span>{/if}
        {#if shardsTotal > 0}
          <span class="muted" title={t('search.shardsScannedHint')}>
            · {t('search.shardsScanned', { scanned: shardsScanned, total: shardsTotal })}</span
          >
        {/if}
      </p>
    {/if}
    <div class="spacer"></div>
    {#if searched && !error}
      <label class="sort">
        <span class="sr-only">{t('search.sort')}</span>
        <Dropdown
          bind:value={sortField}
          options={sortOptions}
          label={t('search.sort')}
          ariaLabel={t('search.sort')}
          onchange={rerun}
        />
      </label>
      <div class="actions">
        <button type="button" onclick={exportJson} disabled={hits.length === 0}>
          {t('search.exportJson')}
        </button>
        <button type="button" onclick={exportCsv} disabled={hits.length === 0}>
          {t('search.exportCsv')}
        </button>
      </div>
    {/if}
  </div>

  <!-- BAND 3: full-width partial-results banner (design-QA T2). -->
  {#if searched && partial && !partialDismissed}
    <div class="partial-banner" role="status">
      <Icon name="warn" />
      <span><strong>{t('search.partial')}</strong> — {t('search.partialHint')}</span>
      <button
        type="button"
        class="banner-x"
        onclick={() => (partialDismissed = true)}
        aria-label={t('common.close')}>×</button
      >
    </div>
  {/if}

  {#if timeOpen && timeBtn}
    <Popover anchor={timeBtn} onClose={() => (timeOpen = false)} width={300}>
      <div class="time-pop">
        <p class="muted small">{t('search.timeDetected', { count: timeFields.length })}</p>
        <div class="tf-block">
          <span class="tf-label">{t('search.timeField')}</span>
          <div class="tf-chips">
            {#each timeFields as f (f)}
              <button
                type="button"
                class="tf-chip mono"
                class:active={timeField === f}
                aria-pressed={timeField === f}
                onclick={() => (timeField = f)}>{f}</button
              >
            {/each}
          </div>
        </div>
        <label class="tf-row">
          <span>{t('search.timeRange')}</span>
          <select bind:value={timePreset} aria-label={t('search.timeRange')}>
            <option value="">{t('search.timeAny')}</option>
            {#each TIME_PRESETS as p (p.id)}
              <option value={p.id}>{p.label}</option>
            {/each}
          </select>
        </label>
        {#if timePreset === 'custom'}
          <label class="tf-row">
            <span>{t('search.timeFrom')}</span>
            <input type="datetime-local" bind:value={timeFrom} />
          </label>
          <label class="tf-row">
            <span>{t('search.timeTo')}</span>
            <input type="datetime-local" bind:value={timeTo} />
          </label>
        {/if}
        <div class="tf-actions">
          <button type="button" onclick={clearTime}>{t('search.timeClear')}</button>
          <button type="button" class="primary" onclick={applyTime}>{t('search.apply')}</button>
        </div>
      </div>
    </Popover>
  {/if}

  <div class="grid">
    <main class="col-results">
      {#if loading}
        <p class="muted">{t('common.loading')}</p>
      {:else if error}
        <p role="alert" class="error">{error}</p>
      {:else if searched}
        {#if filters.length > 0 || timeRange() !== null}
          <div class="filter-bar" aria-label={t('search.activeFilters')}>
            {#if timeRange() !== null}
              <button class="filter-chip time" onclick={clearTime}>
                <span class="fc-field"><Icon name="clock" /> {timeField}</span>
                <span class="fc-x" aria-hidden="true">×</span>
              </button>
            {/if}
            {#each filters as f, i (f.field + ':' + f.value)}
              <button class="filter-chip" onclick={() => removeFilter(i)}>
                <span class="fc-field">{f.field}</span>
                <span class="fc-val mono">{f.value}</span>
                <span class="fc-x" aria-hidden="true">×</span>
                <span class="sr-only">{t('search.remove', { query: `${f.field}:${f.value}` })}</span
                >
              </button>
            {/each}
          </div>
        {/if}

        {#if hits.length > 0}
          <!-- Results as a fixed-layout datatable (task-86): one row per hit, one cell per cached
               field, so the layout is uniform across rows. Cells clip on overflow — never wrap. -->
          <div class="results">
            <!-- Fixed layout so cells clip uniformly; a min-width floor per field column means many
                 columns scroll horizontally rather than squeezing to nothing (rows never wrap). -->
            <table class="hits-table" style="min-width: {13.5 + columns.length * 7}rem">
              <thead>
                <tr>
                  <th class="c-score">{t('search.score')}</th>
                  <th class="c-id">{keyLabel}</th>
                  {#each columns as col (col)}
                    <th>{col}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each hits as hit, i (hitId(hit) + ':' + i)}
                  <tr
                    class="hit"
                    role="button"
                    tabindex="0"
                    onclick={() => (selected = hit)}
                    onkeydown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        selected = hit;
                      }
                    }}
                  >
                    <td class="score mono">{hit.score?.toFixed(3) ?? '—'}</td>
                    <td class="id mono">{hitId(hit)}</td>
                    {#each columns as col (col)}
                      {@const cell = cellText(hit, col)}
                      <td title={cell.text}
                        ><Highlighted text={cell.text} {terms} segments={cell.segments} /></td
                      >
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}

        {#if sorted}
          <!-- Keyset (search_after) scroll: forward-only, no offset (stable across shards, task-99). -->
          {#if cursor}
            <nav class="pager" aria-label={t('search.pager')}>
              <button type="button" class="load-more" onclick={loadMore} disabled={loading}>
                {t('search.loadMore')}
              </button>
              <span class="muted">{t('search.shownOf', { shown: hits.length, total })}</span>
            </nav>
          {/if}
        {:else if total > PAGE_SIZE}
          <nav class="pager" aria-label={t('search.pager')}>
            <button type="button" onclick={() => page(-1)} disabled={offset === 0}>
              {t('search.prev')}
            </button>
            <span class="muted">{t('search.range', { from: offset + 1, to: pageEnd, total })}</span>
            <button type="button" onclick={() => page(1)} disabled={pageEnd >= total}>
              {t('search.next')}
            </button>
          </nav>
        {/if}
      {:else}
        <Callout>{t('search.empty')}</Callout>
      {/if}
    </main>

    <!-- Filters rail: a single flush panel with a right border (design-QA T2), collapsible to a
         thin strip whose state persists (design-QA T9). -->
    <aside class="col-rail" class:collapsed={railCollapsed} aria-label={t('search.filters')}>
      {#if railCollapsed}
        <button
          class="rail-strip"
          type="button"
          onclick={toggleRail}
          aria-expanded="false"
          aria-label={t('search.expandFilters')}
        >
          <span class="rail-chevron" aria-hidden="true">›</span>
          <span class="rail-vlabel">{t('search.filters')}</span>
        </button>
      {:else}
        <div class="rail-inner">
          <div class="rail-section rail-head">
            <h2>{t('search.savedQueries')}</h2>
            <button
              class="rail-toggle"
              type="button"
              onclick={toggleRail}
              aria-expanded="true"
              aria-label={t('search.collapseFilters')}>‹</button
            >
          </div>
          <div class="rail-section rail-saved">
            {#if saved.length === 0}
              <p class="muted small">{t('search.noSaved')}</p>
            {:else}
              <ul class="saved-list">
                {#each saved as s, i (s.id ?? s.name + ':' + i)}
                  <li>
                    <button class="saved-q" onclick={() => loadQuery(s)}>
                      <span class="sq-row">
                        <span class="sq-name">{s.name}</span>
                        {#if s.shared}<span class="sq-shared" title={t('search.savedShared')}
                            >↗</span
                          >{/if}
                      </span>
                      <span class="sq-query mono">{s.query}</span>
                    </button>
                    <button
                      class="chip-x"
                      onclick={() => remove(s)}
                      aria-label={t('search.remove', { query: s.name })}>×</button
                    >
                  </li>
                {/each}
              </ul>
            {/if}
            <button class="save-btn" type="button" onclick={save} disabled={!query.trim()}>
              + {t('search.save')}
            </button>
          </div>

          {#if searched}
            <div class="rail-divider"></div>
            <div class="rail-section">
              <h2>{t('search.facets')}</h2>
              {#if facetGroups.length === 0}
                <p class="muted small">{t('search.noFacets')}</p>
              {:else}
                {#each facetGroups as g (g.field)}
                  {@const collapsed = collapsedFacets.has(g.field)}
                  <div class="facet-group">
                    <button
                      class="facet-field"
                      onclick={() => toggleFacetGroup(g.field)}
                      aria-expanded={!collapsed}
                    >
                      <span class="facet-caret" aria-hidden="true">{collapsed ? '▸' : '▾'}</span>
                      {g.field}
                    </button>
                    {#if !collapsed}
                      <ul class="facet-list">
                        {#each g.buckets as b (b.value)}
                          <li>
                            <button
                              class="facet-val"
                              class:active={isActive(g.field, b.value)}
                              onclick={() => toggleFacet(g.field, b.value)}
                            >
                              <span class="fv-text mono" title={b.value}>{b.value}</span>
                              <span class="fv-count">{b.count}</span>
                            </button>
                          </li>
                        {/each}
                      </ul>
                    {/if}
                  </div>
                {/each}
              {/if}
            </div>
          {/if}
        </div>
      {/if}
    </aside>
  </div>
</section>

{#if selected}
  {#key selected}
    <DocumentDrawer
      hit={selected}
      {terms}
      query={lastQuery}
      {syntax}
      onClose={() => (selected = null)}
    />
  {/key}
{/if}

<style>
  /* The Search screen is a full-viewport app shell (design-QA T2): full-bleed horizontal bands over
     a flex body whose panes scroll independently. The section itself does not scroll and carries no
     padding — each band and the body own their spacing (overrides the generic `#main > section`). */
  .search {
    padding: 0;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  /* BAND 1 — full-bleed query strip. */
  .qstrip {
    flex: 0 0 auto;
    background: var(--panel);
    border-bottom: 1px solid var(--line);
    padding: 12px 16px;
  }
  .qbar {
    margin: 0;
  }
  .qrow {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
  }
  /* The query field: a bordered wrapper holding a search glyph, the input, and the syntax pill. */
  .qfield {
    flex: 1;
    min-width: 220px;
    display: flex;
    align-items: center;
    gap: 9px;
    background: var(--field);
    border: 1px solid var(--line-strong);
    border-radius: 7px;
    padding: 0 11px;
  }
  .qfield:focus-within {
    border-color: var(--accent);
  }
  .qfield-icon {
    flex: 0 0 auto;
  }
  .ac {
    position: relative;
    flex: 1;
    min-width: 0;
  }
  .ac input {
    width: 100%;
    border: 0;
    background: transparent;
    border-radius: 0;
    padding: 0;
    height: 34px;
  }
  .ac input:focus {
    outline: none;
  }
  .syntax-pill {
    flex: 0 0 auto;
    font-size: 9.5px;
    color: var(--text-3);
    border: 1px solid var(--line);
    border-radius: 4px;
    padding: 2px 5px;
    white-space: nowrap;
  }

  /* BAND 2 — stats band spanning the rail + results. */
  .statbar {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 8px 16px;
    border-bottom: 1px solid var(--line);
    font-size: 0.9em;
    flex-wrap: wrap;
  }
  .statbar .count {
    margin: 0;
    font-weight: 600;
  }
  .statbar .spacer {
    flex: 1;
  }
  .sort {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }
  .actions {
    display: flex;
    gap: 0.4rem;
  }
  .time-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    white-space: nowrap;
  }
  .time-btn.on {
    border-color: var(--accent);
    color: var(--accent);
  }
  .time-btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  /* BAND 3 — full-width dismissible partial-results banner (task-133). */
  .partial-banner {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 16px;
    border-bottom: 1px solid var(--warn);
    background: var(--warn-weak, rgba(180, 83, 9, 0.08));
    font-size: 0.9rem;
  }
  .partial-banner .banner-x {
    margin-left: auto;
    background: none;
    border: none;
    color: var(--muted);
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }

  /* BODY — rail | results. */
  .grid {
    flex: 1;
    min-height: 0;
    display: flex;
    gap: 0;
    align-items: stretch;
  }
  .col-results {
    flex: 1;
    min-width: 0;
    overflow-y: auto;
    padding: 1rem 1.25rem;
  }
  /* The filters rail: one flush panel on the LEFT with a right border (design-QA T2). order:-1 keeps
     results first in the DOM (a11y) while painting the rail on the left. */
  .col-rail {
    order: -1;
    flex: 0 0 236px;
    background: var(--panel);
    border-right: 1px solid var(--line);
    overflow-y: auto;
  }
  .col-rail.collapsed {
    flex: 0 0 40px;
  }
  .rail-inner {
    display: flex;
    flex-direction: column;
  }
  .rail-section {
    padding: 0.75rem 0.85rem;
  }
  .rail-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-bottom: 0;
  }
  .rail-saved {
    padding-top: 0.5rem;
  }
  .rail-divider {
    height: 1px;
    background: var(--line);
  }
  .rail-toggle {
    border: 0;
    background: transparent;
    color: var(--text-3);
    cursor: pointer;
    padding: 0 4px;
    line-height: 1;
    font-size: 1.1rem;
  }
  .rail-toggle:hover {
    color: var(--text);
  }
  /* Collapsed rail: a ~40px strip with a vertical "Filters" label (design-QA T9). */
  .rail-strip {
    width: 100%;
    height: 100%;
    min-height: 120px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    padding: 10px 0;
    border: 0;
    border-radius: 0;
    background: transparent;
    color: var(--text-2);
    cursor: pointer;
  }
  .rail-strip:hover {
    color: var(--text);
    background: var(--accent-weakest);
  }
  .rail-chevron {
    font-size: 1rem;
  }
  .rail-vlabel {
    writing-mode: vertical-rl;
    font:
      600 9.5px 'IBM Plex Mono',
      monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-3);
  }

  @media (max-width: 860px) {
    /* Stack and let the whole grid scroll as one column; rail first (saved/facets above results). */
    .grid {
      flex-direction: column;
      overflow-y: auto;
    }
    .col-results,
    .col-rail {
      flex: none;
      overflow: visible;
    }
    .col-rail {
      border-right: 0;
      border-bottom: 1px solid var(--line);
    }
  }
  .time-pop {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
    padding: 0.3rem 0.1rem;
  }
  .tf-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    font-size: 0.9em;
  }
  .tf-row select,
  .tf-row input {
    flex: 1;
    min-width: 0;
  }
  /* Time-field chips (design-QA T5) — replaces the bare <select> with the mockup's chip picker. */
  .tf-block {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .tf-label {
    font:
      600 9px 'IBM Plex Mono',
      monospace;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-3);
  }
  .tf-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    max-height: 68px;
    overflow-y: auto;
  }
  .tf-chip {
    border: 1px solid var(--line-strong);
    background: var(--field);
    color: var(--text-2);
    border-radius: 6px;
    padding: 4px 9px;
    font-size: 0.85em;
    cursor: pointer;
  }
  .tf-chip:hover {
    color: var(--text);
  }
  .tf-chip.active {
    border-color: var(--accent);
    background: var(--accent-weak);
    color: var(--text);
    font-weight: 600;
  }
  .tf-actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.4rem;
    margin-top: 0.2rem;
  }

  .filter-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-bottom: 0.6rem;
  }
  .filter-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    border: 1px solid var(--accent);
    background: var(--accent-weak);
    color: var(--text);
    border-radius: 999px;
    padding: 2px 8px;
    font-size: 0.85em;
    cursor: pointer;
  }
  .filter-chip:hover {
    background: var(--accent-weakest);
  }
  .filter-chip .fc-field {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    color: var(--text-3);
  }
  .filter-chip .fc-x {
    color: var(--text-2);
    font-size: 1.1em;
    line-height: 1;
  }

  .facet-group {
    margin-bottom: 0.7rem;
  }
  /* The facet-group header is now a collapse toggle button (task-133) — reset button chrome. */
  .facet-field {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    width: 100%;
    margin: 0.3rem 0 0.25rem;
    padding: 0;
    background: none;
    border: none;
    cursor: pointer;
    font-size: 0.82em;
    color: var(--text-3);
    text-transform: uppercase;
    letter-spacing: 0.03em;
    text-align: left;
  }
  .facet-field .facet-caret {
    font-size: 0.85em;
    color: var(--muted);
  }
  .facet-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .facet-val {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    width: 100%;
    border: 0;
    background: transparent;
    color: var(--text-2);
    cursor: pointer;
    padding: 2px 6px;
    border-radius: 5px;
    font-size: 0.85em;
  }
  .facet-val:hover {
    background: var(--accent-weakest);
    color: var(--text);
  }
  .facet-val.active {
    background: var(--accent-weak);
    color: var(--text);
    font-weight: 600;
  }
  .facet-val .fv-text {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .facet-val .fv-count {
    color: var(--text-3);
    flex-shrink: 0;
  }

  .results {
    overflow-x: auto;
    overflow-y: hidden;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: var(--panel);
  }
  .hits-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: 0.9em;
  }
  .hits-table th,
  .hits-table td {
    text-align: left;
    padding: 8px 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    border-bottom: 1px solid var(--line);
  }
  .hits-table tbody tr:last-child td {
    border-bottom: none;
  }
  .hits-table thead th {
    color: var(--text-3);
    font-weight: 500;
    font-size: 0.85em;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    border-bottom-color: var(--line-strong);
  }
  /* Score + id are fixed-width; the remaining field columns share the rest evenly. */
  .hits-table .c-score {
    width: 4.5rem;
  }
  .hits-table .c-id {
    width: 9rem;
  }
  .hit {
    display: table-row;
    cursor: pointer;
  }
  .hit:hover td {
    background: var(--accent-weakest);
  }
  .hit:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }
  .hit td.score {
    color: var(--accent);
  }
  .hit td.id {
    font-weight: 600;
    color: var(--text);
  }
  .hits-table :global(mark) {
    background: var(--accent-weak, rgba(255, 214, 0, 0.28));
    color: inherit;
    border-radius: 2px;
    padding: 0 1px;
  }

  .pager {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-top: 0.9rem;
  }

  .rail-section h2 {
    margin: 0 0 0.5rem;
    font-size: 0.95em;
  }
  .rail-head h2 {
    margin: 0;
  }
  .small {
    font-size: 0.85em;
  }
  .saved-list {
    list-style: none;
    margin: 0 0 0.6rem;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .saved-list li {
    display: flex;
    align-items: center;
    gap: 0.25rem;
  }
  .saved-q {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
    text-align: left;
    border: 0;
    background: transparent;
    color: var(--accent);
    cursor: pointer;
    padding: 3px 5px;
    border-radius: 5px;
    font-size: 0.85em;
  }
  .saved-q .sq-row {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    min-width: 0;
  }
  .saved-q .sq-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  /* The saved query's text on a second line (task-133), muted + truncated. */
  .saved-q .sq-query {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 0.9em;
    color: var(--muted);
  }
  .saved-q .sq-shared {
    color: var(--text-3);
    flex-shrink: 0;
  }
  .saved-q:hover {
    background: var(--accent-weakest);
  }
  .chip-x {
    border: 0;
    background: transparent;
    color: var(--text-3);
    cursor: pointer;
    line-height: 1;
    padding: 0 4px;
  }
  .chip-x:hover {
    color: var(--text);
  }
  .save-btn {
    width: 100%;
    font-size: 0.85em;
  }
</style>
