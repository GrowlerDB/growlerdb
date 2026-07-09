<script lang="ts">
  import { onMount } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    listIndexes,
    getIndex,
    describeIndex,
    describeSource,
    createIndex,
    listAliases,
    getIngestion,
    backupStatus,
    type IndexSummary,
    type IndexInfo,
    type IndexStats,
    type BackupStatus,
    type SourceSchema,
    type Alias,
  } from '../lib/api';
  import { formatAge } from '../lib/ingestion';
  import {
    buildDefinition,
    defaultFieldType,
    TIME_FORMATS,
    WINDOW_GRANULARITIES,
  } from '../lib/indexDef';
  import Badge from '../lib/components/Badge.svelte';
  import StatusDot from '../lib/components/StatusDot.svelte';
  import Drawer from '../lib/components/Drawer.svelte';
  import IndexDetail from './IndexDetail.svelte';

  type Enriched = { info?: IndexInfo; stats?: IndexStats | null };

  let indexes = $state<IndexSummary[]>([]);
  let enriched = $state<Record<string, Enriched>>({});
  let lagMap = $state<Record<string, number | null>>({}); // index → snapshots behind source (task-109)
  let backupMap = $state<Record<string, BackupStatus>>({}); // index → last-backup status (task-140)
  let now = $state(Date.now()); // reference time for relative backup ages, stamped at load
  let aliases = $state<Alias[]>([]);
  let listError = $state('');
  let loadingList = $state(true);

  let selected = $state<string | null>(null);

  // Create-from-introspection modal (task-47/89).
  let showCreate = $state(false);
  let cName = $state('');
  let cTable = $state('');
  let cSelection = $state<'ALL' | 'EXPLICIT'>('ALL');
  let schema = $state<SourceSchema | null>(null);
  let chosen = $state(new Set<string>());
  let cTimeField = $state(''); // a source column to map as a DATE timestamp (task-132); '' = none
  let cTimeFormat = $state(TIME_FORMATS[1].value); // default: epoch_ms
  // Time windowing (task-81/132) — only valid once a time field is declared (the ingest field).
  let cWindowing = $state(false);
  let cGranularity = $state('daily');
  let cEventField = $state(''); // optional event-time column for zone-map pruning; '' = none
  let cEventFormat = $state(TIME_FORMATS[1].value);
  let cHotWindows = $state<number | null>(null); // optional: keep N most-recent windows hot; null = all hot
  let introspecting = $state(false);
  let createError = $state('');
  let creating = $state(false);

  onMount(load);

  async function load() {
    loadingList = true;
    listError = '';
    try {
      indexes = await listIndexes();
      aliases = await listAliases();
      const entries = await Promise.all(
        indexes.map(async (ix): Promise<[string, Enriched]> => {
          try {
            const [info, stats] = await Promise.all([getIndex(ix.name), describeIndex(ix.name)]);
            return [ix.name, { info, stats }];
          } catch {
            return [ix.name, {}];
          }
        }),
      );
      enriched = Object.fromEntries(entries);
      now = Date.now();
      // Backup column (task-140): last-backup status per index (best-effort — a node without a
      // backup target reports `configured: false`, rendered as "Off").
      backupMap = Object.fromEntries(
        await Promise.all(
          indexes.map(async (ix): Promise<[string, BackupStatus]> => {
            try {
              return [ix.name, await backupStatus(ix.name)];
            } catch {
              return [ix.name, { configured: false, present: false }];
            }
          }),
        ),
      );
      // Lag column (task-109): snapshots the worst-behind shard trails the source head, from ingestion.
      try {
        const lag: Record<string, number | null> = {};
        for (const it of await getIngestion()) {
          const head = it.source_snapshot_id;
          const committed = it.shards.map((s) => s.committed_snapshot_id).filter((n) => n > 0);
          lag[it.name] =
            head != null && committed.length > 0
              ? Math.max(0, head - Math.min(...committed))
              : null;
        }
        lagMap = lag;
      } catch {
        lagMap = {};
      }
    } catch (err) {
      listError = String(err);
    } finally {
      loadingList = false;
    }
  }

  function tone(status: string): 'ok' | 'warn' | 'muted' {
    if (status === 'ready') return 'ok';
    if (status === 'building' || status === 'reindexing') return 'warn';
    return 'muted';
  }
  function badgeTone(status: string): 'ok' | 'warn' | 'default' {
    const tn = tone(status);
    return tn === 'muted' ? 'default' : tn;
  }
  function aliasFor(name: string): string {
    return (
      aliases
        .filter((a) => a.targets.includes(name))
        .map((a) => a.alias)
        .join(', ') || '—'
    );
  }

  /** Shard count, formatted "N × R" when replicated (R = copies incl. primary), else just "N". */
  function shardsLabel(info?: IndexInfo): string {
    if (!info) return '—';
    const maxReplicas = Math.max(0, ...(info.shards ?? []).map((s) => s.replicas?.length ?? 0));
    const copies = maxReplicas + 1;
    return copies > 1 ? `${info.shard_count} × ${copies}` : `${info.shard_count}`;
  }

  /** A short summary under the title: "{n} indexes · {m} rebuilding" (only when any are rebuilding). */
  function summaryLine(): string {
    const rebuilding = indexes.filter(
      (ix) => ix.status === 'building' || ix.status === 'reindexing',
    ).length;
    const base = t('indexes.summaryCount', { count: indexes.length });
    return rebuilding > 0
      ? `${base} · ${t('indexes.summaryRebuilding', { count: rebuilding })}`
      : base;
  }

  async function introspect() {
    schema = null;
    createError = '';
    introspecting = true;
    try {
      schema = await describeSource(cTable);
      chosen = new Set(
        schema.fields.filter((f) => defaultFieldType(f.type) !== null).map((f) => f.path),
      );
      // Pre-pick an obvious timestamp column so the time filter works out of the box: prefer a
      // source `date`, else a field named like a time/timestamp/ingest column.
      const dateField = schema.fields.find((f) => f.type === 'date');
      const named = schema.fields.find((f) => /time|ts|date|ingest|event/i.test(f.path));
      cTimeField = (dateField ?? named)?.path ?? '';
    } catch (err) {
      createError = String(err);
    } finally {
      introspecting = false;
    }
  }

  function toggleField(path: string) {
    const next = new Set(chosen);
    if (next.has(path)) next.delete(path);
    else next.add(path);
    chosen = next;
  }

  async function submitCreate(event: Event) {
    event.preventDefault();
    createError = '';
    creating = true;
    try {
      const fields = (schema?.fields ?? []).filter((f) => chosen.has(f.path));
      const timeField = cTimeField ? { path: cTimeField, format: cTimeFormat } : null;
      const windowing =
        cWindowing && cTimeField
          ? {
              granularity: cGranularity,
              eventTimeField: cEventField ? { path: cEventField, format: cEventFormat } : null,
              hotWindows: cHotWindows && cHotWindows > 0 ? cHotWindows : null,
            }
          : null;
      const definition = buildDefinition({
        name: cName,
        table: cTable,
        selection: cSelection,
        fields,
        timeField,
        windowing,
      });
      await createIndex(definition);
      showCreate = false;
      cName = '';
      cTable = '';
      schema = null;
      cTimeField = '';
      cWindowing = false;
      cEventField = '';
      cHotWindows = null;
      await load();
    } catch (err) {
      createError = String(err);
    } finally {
      creating = false;
    }
  }
</script>

{#if selected}
  {#key selected}
    <IndexDetail name={selected} {aliases} onBack={() => (selected = null)} onChanged={load} />
  {/key}
{:else}
  <section aria-labelledby="screen-heading">
    <h1 id="screen-heading" class="sr-only">{t('indexes.title')}</h1>
    <div class="screen-toolbar">
      <p class="sub">
        {#if !loadingList && !listError && indexes.length > 0}{summaryLine()}{/if}
      </p>
      <div class="actions">
        <button type="button" onclick={load}>{t('indexes.refresh')}</button>
        <button type="button" class="primary" onclick={() => (showCreate = true)}>
          {t('indexes.create')}
        </button>
      </div>
    </div>

    {#if loadingList}
      <p>{t('common.loading')}</p>
    {:else if listError}
      <p role="alert" class="error">{listError}</p>
    {:else if indexes.length === 0}
      <p class="muted">{t('indexes.empty')}</p>
    {:else}
      <div class="gb-panel">
        <table class="ix-table">
          <thead>
            <tr>
              <th>{t('indexes.name')}</th>
              <th>{t('indexes.status')}</th>
              <th class="num">{t('indexes.colDocs')}</th>
              <th class="num">{t('indexes.colShards')}</th>
              <th>{t('indexes.colLag')}</th>
              <th>{t('indexes.colBackup')}</th>
              <th>{t('indexes.colAlias')}</th>
            </tr>
          </thead>
          <tbody>
            {#each indexes as ix (ix.name)}
              {@const e = enriched[ix.name] ?? {}}
              <tr>
                <td class="name">
                  <div class="name-inner">
                    <StatusDot tone={tone(ix.status)} pulse={tone(ix.status) === 'warn'} />
                    <button class="link id" onclick={() => (selected = ix.name)}>{ix.name}</button>
                  </div>
                </td>
                <td><Badge tone={badgeTone(ix.status)}>{ix.status}</Badge></td>
                <td class="num mono">{e.stats ? e.stats.num_docs : '—'}</td>
                <td class="num mono">{shardsLabel(e.info)}</td>
                <td class="num">
                  {#if lagMap[ix.name] == null}
                    <span class="muted">—</span>
                  {:else if lagMap[ix.name] === 0}
                    <span class="lag-ok">{t('indexes.lagSync')}</span>
                  {:else}
                    <span class="lag-behind mono">{lagMap[ix.name]}</span>
                  {/if}
                </td>
                <td>
                  {#if !backupMap[ix.name]?.configured}
                    <span class="muted">{t('indexes.backupOff')}</span>
                  {:else if !backupMap[ix.name]?.present}
                    <span class="muted">{t('indexes.backupNever')}</span>
                  {:else if backupMap[ix.name]?.created_ms}
                    <span
                      class="mono"
                      title={t('indexes.backupSnap', {
                        snapshot: backupMap[ix.name]?.snapshot ?? 0,
                      })}
                    >
                      {formatAge(backupMap[ix.name]?.created_ms ?? null, now)}
                    </span>
                  {:else}
                    <span class="mono"
                      >{t('indexes.backupSnap', {
                        snapshot: backupMap[ix.name]?.snapshot ?? 0,
                      })}</span
                    >
                  {/if}
                </td>
                <td class="mono">{aliasFor(ix.name)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
      <p class="legend muted">
        <StatusDot tone="ok" />
        {t('cluster.health.ok')} ·
        <StatusDot tone="warn" />
        {t('cluster.health.warn')}
      </p>
    {/if}
  </section>
{/if}

{#if showCreate}
  <Drawer title={t('indexes.create')} onClose={() => (showCreate = false)}>
    <form class="create" onsubmit={submitCreate} aria-label={t('indexes.create')}>
      <div class="row">
        <label for="c-name">{t('indexes.name')}</label>
        <input id="c-name" bind:value={cName} autocomplete="off" required />
      </div>
      <div class="row">
        <label for="c-table">{t('indexes.table')}</label>
        <input
          id="c-table"
          bind:value={cTable}
          placeholder="namespace.table"
          autocomplete="off"
          required
        />
        <button type="button" onclick={introspect} disabled={!cTable.trim() || introspecting}>
          {t('indexes.introspect')}
        </button>
      </div>

      {#if schema}
        <fieldset>
          <legend>{t('indexes.selection')}</legend>
          <label
            ><input type="radio" name="sel" value="ALL" bind:group={cSelection} />
            {t('indexes.all')}</label
          >
          <label
            ><input type="radio" name="sel" value="EXPLICIT" bind:group={cSelection} />
            {t('indexes.explicit')}</label
          >
        </fieldset>

        {#if cSelection === 'EXPLICIT'}
          <fieldset>
            <legend>{t('indexes.fields')}</legend>
            {#each schema.fields as f (f.path)}
              {@const indexable = defaultFieldType(f.type) !== null}
              <label class:disabled={!indexable}>
                <input
                  type="checkbox"
                  checked={chosen.has(f.path)}
                  disabled={!indexable}
                  onchange={() => toggleField(f.path)}
                />
                <code>{f.path}</code>
                <span class="muted"
                  >{f.type}{indexable ? ` → ${defaultFieldType(f.type)}` : ''}</span
                >
              </label>
            {/each}
          </fieldset>
        {/if}

        <fieldset>
          <legend>{t('indexes.timeField')}</legend>
          <p class="hint muted">{t('indexes.timeFieldHint')}</p>
          <div class="row">
            <label for="c-time-field">{t('indexes.timeFieldLabel')}</label>
            <select id="c-time-field" bind:value={cTimeField}>
              <option value="">{t('indexes.timeFieldNone')}</option>
              {#each schema.fields as f (f.path)}
                <option value={f.path}>{f.path} ({f.type})</option>
              {/each}
            </select>
          </div>
          {#if cTimeField}
            <div class="row">
              <label for="c-time-format">{t('indexes.timeFieldFormat')}</label>
              <select id="c-time-format" bind:value={cTimeFormat}>
                {#each TIME_FORMATS as fmt (fmt.value)}
                  <option value={fmt.value}>{fmt.label}</option>
                {/each}
              </select>
            </div>
          {/if}
        </fieldset>

        <fieldset>
          <legend>{t('indexes.windowing')}</legend>
          <p class="hint muted">{t('indexes.windowingHint')}</p>
          {#if !cTimeField}
            <p class="hint muted">{t('indexes.windowingNeedsTimeField')}</p>
          {:else}
            <label>
              <input type="checkbox" bind:checked={cWindowing} />
              {t('indexes.windowingEnable')}
            </label>
            {#if cWindowing}
              <div class="row">
                <span class="label">{t('indexes.windowingField')}</span>
                <code>{cTimeField}</code>
              </div>
              <div class="row">
                <label for="c-granularity">{t('indexes.windowingGranularity')}</label>
                <select id="c-granularity" bind:value={cGranularity}>
                  {#each WINDOW_GRANULARITIES as g (g.value)}
                    <option value={g.value}>{g.label}</option>
                  {/each}
                </select>
              </div>
              <div class="row">
                <label for="c-event-field">{t('indexes.windowingEventField')}</label>
                <select id="c-event-field" bind:value={cEventField}>
                  <option value="">{t('indexes.windowingEventNone')}</option>
                  {#each schema.fields.filter((f) => f.path !== cTimeField) as f (f.path)}
                    <option value={f.path}>{f.path} ({f.type})</option>
                  {/each}
                </select>
              </div>
              {#if cEventField}
                <div class="row">
                  <label for="c-event-format">{t('indexes.timeFieldFormat')}</label>
                  <select id="c-event-format" bind:value={cEventFormat}>
                    {#each TIME_FORMATS as fmt (fmt.value)}
                      <option value={fmt.value}>{fmt.label}</option>
                    {/each}
                  </select>
                </div>
              {/if}
              <div class="row">
                <label for="c-hot-windows">{t('indexes.windowingHotWindows')}</label>
                <input
                  id="c-hot-windows"
                  type="number"
                  min="1"
                  bind:value={cHotWindows}
                  placeholder={t('indexes.windowingHotAll')}
                />
              </div>
            {/if}
          {/if}
        </fieldset>
      {/if}

      {#if createError}
        <p role="alert" class="error">{createError}</p>
      {/if}
      <div class="actions">
        <button
          type="submit"
          class="primary"
          disabled={creating || !cName.trim() || !cTable.trim()}
        >
          {t('indexes.submit')}
        </button>
        <button type="button" onclick={() => (showCreate = false)}>{t('indexes.cancel')}</button>
      </div>
    </form>
  </Drawer>
{/if}

<style>
  .ix-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.92em;
  }
  .ix-table th {
    text-align: left;
    color: var(--text-3);
    font:
      600 9.5px 'IBM Plex Mono',
      monospace;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 10px 15px;
    border-bottom: 1px solid var(--line);
    background: var(--panel);
  }
  .ix-table td {
    padding: var(--cell, 8px 12px);
    border-bottom: 1px solid var(--line);
    vertical-align: middle;
  }
  /* Panel border already draws the bottom edge (design-QA T3) — drop the last row's rule. */
  .ix-table tbody tr:last-child td {
    border-bottom: 0;
  }
  .ix-table tr:hover td {
    background: var(--accent-weakest);
  }
  .ix-table th.num,
  .ix-table td.num {
    text-align: right;
  }
  /* Keep td.name a real table-cell so its border-bottom aligns with the other cells (task-134);
     lay out the dot + name in an inner flex wrapper instead. */
  td.name .name-inner {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  td.name .id {
    font-weight: 600;
  }
  .lag-ok {
    color: var(--ok);
    font-size: 0.85em;
  }
  .lag-behind {
    color: var(--warn);
  }
  .legend {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.85em;
    margin-top: 0.7rem;
    flex-wrap: wrap;
  }
  .create {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
</style>
