<script lang="ts">
  // Tabbed view of one index. Stats strip is always visible; tabs split
  // policy/mapping/shards/maintenance/activity. Maintenance wires reindex/drop/alias against
  // current APIs; Mapping, Shards and Activity are scaffolds, and Compact/Backup render as PLANNED.
  import { onMount } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    getIndex,
    describeIndex,
    reindexIndex,
    dropIndex,
    setAlias,
    dropAlias,
    compactIndex,
    backupIndex,
    backupStatus,
    listActivity,
    type ActivityEvent,
    type IndexInfo,
    type IndexStats,
    type Alias,
    type ReindexResult,
  } from '../lib/api';
  import Tabs from '../lib/components/Tabs.svelte';
  import Badge from '../lib/components/Badge.svelte';
  import KeyValue from '../lib/components/KeyValue.svelte';
  import StatusDot from '../lib/components/StatusDot.svelte';
  import Callout from '../lib/components/Callout.svelte';

  let {
    name,
    aliases,
    onBack,
    onChanged,
  }: {
    name: string;
    aliases: Alias[];
    onBack: () => void;
    onChanged: () => void;
  } = $props();

  let info = $state<IndexInfo | null>(null);
  let stats = $state<IndexStats | null>(null);
  let loadError = $state('');
  let tab = $state('mapping'); // land on the leftmost/primary tab

  let reindexing = $state(false);
  let reindexResult = $state<ReindexResult | null>(null);
  let reindexError = $state('');

  let aliasName = $state('');
  let aliasError = $state('');
  let aliasBusy = $state(false);

  // Compact + backup.
  let compacting = $state(false);
  let compactMsg = $state('');
  let backingUp = $state(false);
  let backupMsg = $state('');
  let bstatus = $state<import('../lib/api').BackupStatus | null>(null);

  // Activity log.
  let activity = $state<ActivityEvent[]>([]);
  function evIcon(kind: string): string {
    if (kind.startsWith('index.created')) return '＋';
    if (kind.startsWith('alias')) return '↗';
    if (kind.startsWith('reshard') || kind.startsWith('bucket')) return '⇄';
    if (kind.startsWith('compact') || kind.startsWith('merge')) return '⚙';
    if (kind.startsWith('backup')) return '⤓';
    return '•';
  }
  function relTime(ms: number): string {
    const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 60) return `${s}s ago`;
    const m = Math.round(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.round(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.round(h / 24)}d ago`;
  }

  const tabs = [
    { id: 'mapping', label: t('indexes.tabMapping') },
    { id: 'policies', label: t('indexes.tabPolicies') },
    { id: 'shards', label: t('indexes.tabShards') },
    { id: 'maintenance', label: t('indexes.tabMaintenance') },
    { id: 'activity', label: t('indexes.tabActivity') },
  ];

  function tone(status: string): 'ok' | 'warn' | 'accent' | 'muted' {
    if (status === 'ready') return 'ok';
    if (status === 'building' || status === 'reindexing') return 'warn';
    return 'muted';
  }

  const ownAliases = $derived(aliases.filter((a) => a.targets.includes(name)));

  // Cached-fields policy summary derived from the loaded mapping: how many fields are
  // cached for display, and how many are excluded by policy (blocked / not cached).
  const mappingFields = $derived(info?.fields ?? []);
  const cachedCount = $derived(mappingFields.filter((f) => f.cached).length);
  const excludedCount = $derived(mappingFields.filter((f) => f.blocked).length);

  // Shard map.
  const shardCells = $derived(info?.shards ?? []);
  const primaryCount = $derived(shardCells.filter((s) => s.primary).length);
  const replicaCount = $derived(shardCells.reduce((n, s) => n + (s.replicas?.length ?? 0), 0));
  function shardTone(s: { state: string }): 'ok' | 'warn' | 'muted' {
    return s.state === 'active' ? 'ok' : 'warn';
  }
  function shardTitle(s: {
    ordinal: number;
    window?: number;
    primary?: string;
    replicas?: string[];
  }): string {
    const who = s.window ? `window ${s.window}` : `shard ${s.ordinal}`;
    const primary = s.primary || '—';
    const reps = s.replicas?.length ? ` · replicas: ${s.replicas.join(', ')}` : '';
    return `${who} · primary: ${primary}${reps}`;
  }

  async function load() {
    loadError = '';
    try {
      info = await getIndex(name);
      stats = await describeIndex(name);
    } catch (err) {
      loadError = String(err);
    }
  }

  onMount(async () => {
    await load();
    try {
      bstatus = await backupStatus(name);
    } catch {
      bstatus = { configured: false, present: false };
    }
    activity = await listActivity(name);
  });

  async function compact() {
    if (!confirm(t('indexes.confirmCompact', { name }))) return;
    compacting = true;
    compactMsg = '';
    try {
      const r = await compactIndex(name);
      compactMsg = t('indexes.compactDone', { before: r.segments_before, after: r.segments_after });
      stats = await describeIndex(name);
    } catch (err) {
      compactMsg = String(err);
    } finally {
      compacting = false;
    }
  }

  async function backup() {
    if (!confirm(t('indexes.confirmBackup', { name }))) return;
    backingUp = true;
    backupMsg = '';
    try {
      const r = await backupIndex(name);
      backupMsg = t('indexes.backupDone', { files: r.file_count, snapshot: r.snapshot });
      bstatus = await backupStatus(name);
    } catch (err) {
      backupMsg = String(err);
    } finally {
      backingUp = false;
    }
  }

  async function reindex() {
    if (!confirm(t('indexes.confirmReindex', { name }))) return;
    reindexing = true;
    reindexResult = null;
    reindexError = '';
    try {
      reindexResult = await reindexIndex(name);
      stats = await describeIndex(name);
    } catch (err) {
      reindexError = String(err);
    } finally {
      reindexing = false;
    }
  }

  async function drop() {
    if (!confirm(t('indexes.confirmDrop', { name }))) return;
    try {
      await dropIndex(name);
      onChanged();
      onBack();
    } catch (err) {
      loadError = String(err);
    }
  }

  async function pointAlias(event: Event) {
    event.preventDefault();
    if (!aliasName.trim()) return;
    aliasBusy = true;
    aliasError = '';
    try {
      await setAlias(aliasName.trim(), [name]);
      aliasName = '';
      onChanged();
    } catch (err) {
      aliasError = String(err);
    } finally {
      aliasBusy = false;
    }
  }

  async function removeAlias(alias: string) {
    if (!confirm(t('indexes.confirmDropAlias', { alias }))) return;
    aliasError = '';
    try {
      await dropAlias(alias);
      onChanged();
    } catch (err) {
      aliasError = String(err);
    }
  }
</script>

<section class="detail" aria-labelledby="screen-heading">
  <button class="back link" type="button" onclick={onBack}>{t('indexes.back')}</button>

  <header class="detail-head">
    <h1 id="screen-heading" class="mono ix-title">{name}</h1>
    {#if info}
      <span class="status">
        <StatusDot tone={tone(info.status)} pulse={tone(info.status) === 'warn'} />
        {info.status}
      </span>
    {/if}
  </header>

  {#if loadError}
    <p role="alert" class="error">{loadError}</p>
  {/if}

  <div class="strip">
    <div class="stat">
      <span class="k">{t('indexes.docs')}</span><span class="v mono"
        >{stats ? stats.num_docs : '—'}</span
      >
    </div>
    <div class="stat">
      <span class="k">{t('indexes.shards')}</span><span class="v mono"
        >{info ? info.shard_count : '—'}</span
      >
    </div>
    <div class="stat">
      <span class="k">{t('indexes.routing')}</span><span class="v mono"
        >{info ? info.routing : '—'}</span
      >
    </div>
    <div class="stat">
      <span class="k">{t('indexes.snapshot')}</span><span class="v mono"
        >{stats ? stats.snapshot : '—'}</span
      >
    </div>
    <div class="stat">
      <span class="k">{t('indexes.checkpoint')}</span><span class="v mono"
        >{stats ? stats.checkpoint : '—'}</span
      >
    </div>
  </div>

  <Tabs {tabs} bind:active={tab} />

  <div class="panel">
    {#if tab === 'mapping'}
      {#if !info || !info.fields || info.fields.length === 0}
        <p class="muted">{t('indexes.mappingNone')}</p>
      {:else}
        {#if info.fields.some((f) => f.blocked)}
          <Callout tone="warn">{t('indexes.mappingBlocked')}</Callout>
        {/if}
        <div class="gb-panel">
          <table class="map-table">
            <thead>
              <tr>
                <th>{t('indexes.mapField')}</th>
                <th>{t('indexes.mapType')}</th>
                <th>{t('indexes.mapAnalyzer')}</th>
                <th class="flag">{t('indexes.mapFast')}</th>
                <th class="flag">{t('indexes.mapCached')}</th>
              </tr>
            </thead>
            <tbody>
              {#each info.fields as f (f.path)}
                <tr class:blocked={!!f.blocked}>
                  <td class="mono field">
                    {f.path}
                    {#if f.pk}<span class="pk">PK</span>{/if}
                  </td>
                  <td class="mono">{f.type}</td>
                  <td class="muted">{f.analyzer || '—'}</td>
                  <td class="flag">{f.fast ? '✓' : '—'}</td>
                  <td class="flag">
                    {#if f.blocked}
                      <span class="blocked-tag" title={f.blocked}>⊘ {t('indexes.mapBlocked')}</span>
                    {:else}
                      {f.cached ? '✓' : '—'}
                    {/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    {:else if tab === 'policies'}
      <div class="cards">
        <div class="pcard">
          <h3>{t('indexes.polSharding')}</h3>
          {#if info}
            <KeyValue
              entries={[
                [t('indexes.shards'), String(info.shard_count)],
                [t('indexes.routing'), info.routing],
              ]}
            />
          {/if}
        </div>
        <div class="pcard">
          <h3>{t('indexes.polSync')}</h3>
          {#if stats}
            <KeyValue
              entries={[
                [t('indexes.snapshot'), String(stats.snapshot)],
                [t('indexes.checkpoint'), String(stats.checkpoint)],
                [t('indexes.generations'), String(stats.generation_count ?? '—')],
              ]}
            />
          {:else}
            <p class="muted small">{t('indexes.noStats')}</p>
          {/if}
        </div>
        <div class="pcard">
          <h3>{t('indexes.polCached')}</h3>
          {#if mappingFields.length > 0}
            <p class="cached-summary">
              {t('indexes.polCachedSummary', { cached: cachedCount, total: mappingFields.length })}
            </p>
            <p class="muted small cached-note">
              {excludedCount > 0
                ? t('indexes.polCachedExcluded', { count: excludedCount })
                : t('indexes.polCachedNoExcluded')}
            </p>
          {:else}
            <p class="muted small">{t('indexes.mappingNone')}</p>
          {/if}
        </div>
      </div>
    {:else if tab === 'shards'}
      {#if shardCells.length === 0}
        <p class="muted">{t('indexes.shardsNone')}</p>
      {:else}
        <div class="shard-head">
          <span
            >{t('indexes.shardCounts', { primaries: primaryCount, replicas: replicaCount })}</span
          >
          <span class="legend muted">
            <StatusDot tone="ok" />
            {t('indexes.shardActive')} ·
            <StatusDot tone="warn" />
            {t('indexes.shardBuilding')}
          </span>
        </div>
        <div class="shard-grid">
          {#each shardCells as s (s.window ? `w${s.window}` : `o${s.ordinal}`)}
            <div class="shard-cell {shardTone(s)}" title={shardTitle(s)}></div>
          {/each}
        </div>
      {/if}
    {:else if tab === 'maintenance'}
      <div class="maint">
        <div class="mrow">
          <button type="button" onclick={reindex} disabled={reindexing}>
            {reindexing ? t('indexes.reindexing') : t('indexes.reindex')}
          </button>
          <span class="muted reindex-hint">{t('indexes.reindexHint')}</span>
        </div>
        {#if reindexResult}
          <p class="muted" role="status">
            {t('indexes.reindexed', {
              docs: reindexResult.doc_count,
              snapshot: reindexResult.snapshot,
            })}
          </p>
        {/if}
        {#if reindexError}
          <p class="error" role="alert">{reindexError}</p>
        {/if}

        <div class="mrow ops">
          <button type="button" onclick={compact} disabled={compacting}>
            {compacting ? t('indexes.compacting') : t('indexes.maintCompact')}
          </button>
          {#if bstatus?.configured}
            <button type="button" onclick={backup} disabled={backingUp}>
              {backingUp ? t('indexes.backingUp') : t('indexes.maintBackup')}
            </button>
          {:else}
            <button type="button" disabled>{t('indexes.maintBackup')}</button>
            <Badge tone="planned">{t('indexes.backupNotConfigured')}</Badge>
          {/if}
        </div>
        {#if compactMsg}<p class="muted small" role="status">{compactMsg}</p>{/if}
        {#if backupMsg}<p class="muted small" role="status">{backupMsg}</p>{/if}
        {#if bstatus?.configured}
          <p class="muted small">
            {bstatus.present
              ? t('indexes.backupLast', { snapshot: bstatus.snapshot ?? 0 })
              : t('indexes.backupNone')}
          </p>
        {/if}

        <div class="alias-block">
          <h3>{t('indexes.aliases')}</h3>
          <p class="muted small">{t('indexes.aliasesHelp')}</p>
          {#if ownAliases.length > 0}
            <ul class="alias-list">
              {#each ownAliases as a (a.alias)}
                <li>
                  <span class="mono">{a.alias}</span>
                  <span class="muted">→ {a.targets.join(', ')}</span>
                  <button class="link drop" onclick={() => removeAlias(a.alias)}
                    >{t('indexes.drop')}</button
                  >
                </li>
              {/each}
            </ul>
          {/if}
          <form class="alias-form" onsubmit={pointAlias} aria-label={t('indexes.setAlias')}>
            <input
              id="alias-name"
              bind:value={aliasName}
              placeholder="events"
              autocomplete="off"
              aria-label={t('indexes.aliasName')}
            />
            <button type="submit" disabled={aliasBusy || !aliasName.trim()}
              >{t('indexes.setAlias')}</button
            >
          </form>
          {#if aliasError}
            <p role="alert" class="error">{aliasError}</p>
          {/if}
        </div>

        <div class="danger">
          <h3>{t('indexes.maintDanger')}</h3>
          <button type="button" class="drop-btn" onclick={drop}>{t('indexes.drop')}</button>
        </div>
      </div>
    {:else}
      <p class="muted small activity-scope">{t('indexes.activityScope')}</p>
      {#if activity.length === 0}
        <p class="muted">{t('indexes.activityNone')}</p>
      {:else}
        <div class="gb-panel">
          <ul class="activity">
            {#each activity as ev, i (ev.ts_ms + ':' + i)}
              <li>
                <span class="ev-icon">{evIcon(ev.kind)}</span>
                <span class="ev-msg">{ev.message}</span>
                <span class="ev-time muted" title={new Date(ev.ts_ms).toLocaleString()}>
                  {relTime(ev.ts_ms)}
                </span>
              </li>
            {/each}
          </ul>
        </div>
      {/if}
    {/if}
  </div>
</section>

<style>
  .back {
    border: 0;
    background: transparent;
    color: var(--text-2);
    cursor: pointer;
    padding: 0;
    font: inherit;
    margin-bottom: 0.4rem;
  }
  .back:hover {
    color: var(--accent);
  }
  .detail-head {
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }
  /* The index name is an identifier → mono. */
  .detail-head h1.ix-title {
    margin: 0;
    font:
      600 17px 'Geist Mono',
      ui-monospace,
      monospace;
    letter-spacing: -0.01em;
  }
  .status {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    color: var(--text-2);
    font-size: 0.9em;
  }
  .strip {
    display: flex;
    flex-wrap: wrap;
    gap: 1.5rem;
    margin: 0.9rem 0 1rem;
    padding: 0.7rem 0.9rem;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
  }
  .stat {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .stat .k {
    color: var(--text-3);
    font-size: 0.78em;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .stat .v {
    font-weight: 600;
  }
  .panel {
    margin-top: 1rem;
  }
  .activity {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
  }
  .activity li {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.5rem 15px;
    border-bottom: 1px solid var(--line);
    font-size: 0.9em;
  }
  .activity li:last-child {
    border-bottom: 0;
  }
  .activity .ev-icon {
    color: var(--accent);
    flex-shrink: 0;
    width: 1.2em;
    text-align: center;
  }
  .activity .ev-msg {
    flex: 1;
    word-break: break-word;
  }
  .activity .ev-time {
    flex-shrink: 0;
    font-size: 0.85em;
  }
  .shard-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin: 0.6rem 0;
    font-size: 0.9em;
  }
  .shard-head .legend {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.85em;
  }
  /* Dense 16-column heatmap of small squares: ok = --ok @ .85, degraded = --warn.
     Detail stays in the per-cell title tooltip. */
  .shard-grid {
    display: grid;
    grid-template-columns: repeat(16, 1fr);
    gap: 5px;
    max-width: 560px;
    padding: 4px 0 8px;
  }
  .shard-cell {
    width: 100%;
    aspect-ratio: 1;
    border-radius: 2px;
    cursor: default;
  }
  .shard-cell.ok {
    background: var(--ok);
    opacity: 0.85;
  }
  .shard-cell.warn {
    background: var(--warn);
  }
  .map-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9em;
  }
  .map-table th {
    text-align: left;
    color: var(--text-3);
    font:
      600 9.5px 'Geist Mono',
      monospace;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 10px 15px;
    border-bottom: 1px solid var(--line);
  }
  .map-table td {
    padding: 0.45rem 15px;
    border-bottom: 1px solid var(--line);
  }
  .map-table tbody tr:last-child td {
    border-bottom: 0;
  }
  .map-table th.flag,
  .map-table td.flag {
    text-align: center;
    width: 5rem;
  }
  .map-table tr.blocked td {
    background: var(--warn-weak, var(--panel2));
  }
  .map-table .field .pk {
    margin-left: 0.4rem;
    font-size: 0.72em;
    font-weight: 700;
    color: var(--accent);
    border: 1px solid var(--accent);
    border-radius: 4px;
    padding: 0 3px;
  }
  .map-table .blocked-tag {
    color: var(--warn);
    font-size: 0.85em;
    white-space: nowrap;
  }
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0.75rem;
  }
  .pcard {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 0.75rem 0.85rem;
  }
  .pcard h3 {
    margin: 0 0 0.5rem;
    font-size: 0.92em;
  }
  .cached-summary {
    margin: 0;
    font-size: 0.9em;
  }
  .cached-note {
    margin: 0.35rem 0 0;
  }
  .pcard-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0.4rem;
  }
  .small {
    font-size: 0.85em;
  }
  .maint {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .mrow {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
  }
  .alias-block,
  .danger {
    border-top: 1px solid var(--line);
    padding-top: 0.9rem;
  }
  .alias-block h3,
  .danger h3 {
    margin: 0 0 0.4rem;
    font-size: 0.92em;
  }
  .alias-list {
    list-style: none;
    margin: 0 0 0.6rem;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .alias-list li {
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }
  .alias-form {
    display: flex;
    gap: 0.4rem;
  }
  .drop,
  .drop-btn {
    color: var(--warn);
  }
  .drop {
    border: 0;
    background: transparent;
    cursor: pointer;
  }
</style>
