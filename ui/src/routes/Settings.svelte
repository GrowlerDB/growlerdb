<script lang="ts">
  // Appearance · Connection · Users & roles · API tokens · About.
  // Appearance + Connection + About are live; Users & roles is real for admins; API
  // tokens is a PLANNED placeholder.
  import { t } from '../lib/i18n';
  import {
    theme,
    accent,
    density,
    setTheme,
    setAccent,
    setDensity,
    type Theme,
    type Accent,
    type Density,
  } from '../lib/prefs';
  import { loadConfig } from '../lib/config';
  import { identity, initialsOf } from '../lib/identity';
  import {
    listUsers,
    listRoles,
    setUserRoles,
    listTokens,
    createToken,
    revokeToken,
    getLicense,
    type RoleBinding,
    type ApiTokenMeta,
    type LicenseInfo,
  } from '../lib/api';
  import { build } from '../lib/build';
  import Segmented from '../lib/components/Segmented.svelte';
  import Badge from '../lib/components/Badge.svelte';
  import KeyValue from '../lib/components/KeyValue.svelte';

  const config = loadConfig();
  const user = $derived($identity);
  const isAdmin = $derived(user?.roles?.includes('admin') ?? false);

  // User & role management — loaded + editable only for admins.
  let users = $state<RoleBinding[]>([]);
  let roleCatalog = $state<string[]>([]);
  let newSubject = $state('');
  let newRoles = $state<Set<string>>(new Set(['reader'])); // roles to grant a new user
  let usersErr = $state('');
  let usersLoaded = false;

  // Scale-limit license status — visible to any signed-in user.
  let license = $state<LicenseInfo | null>(null);
  let licenseLoaded = false;
  $effect(() => {
    if (!licenseLoaded) {
      licenseLoaded = true;
      getLicense()
        .then((l) => (license = l))
        .catch(() => {});
    }
  });
  let licenseEntries = $derived(
    license
      ? ([
          ...(license.licensee ? [[t('settings.licensee'), license.licensee]] : []),
          [t('settings.nodes'), `${license.current_nodes} / ${license.max_nodes}`],
        ] as [string, string][])
      : ([] as [string, string][]),
  );

  /** Toggle a role in a selection set (returns a new Set so Svelte tracks the change). */
  function toggleInSet(set: Set<string>, role: string): Set<string> {
    const next = new Set(set);
    if (next.has(role)) next.delete(role);
    else next.add(role);
    return next;
  }

  $effect(() => {
    if (isAdmin && !usersLoaded) {
      usersLoaded = true;
      void loadUsers();
    }
  });

  async function loadUsers() {
    try {
      [users, roleCatalog] = await Promise.all([listUsers(), listRoles()]);
    } catch (err) {
      usersErr = String(err);
    }
  }
  async function toggleRole(subject: string, role: string, on: boolean) {
    const current = users.find((u) => u.subject === subject)?.roles ?? [];
    const next = on ? [...new Set([...current, role])] : current.filter((r) => r !== role);
    try {
      await setUserRoles(subject, next);
      await loadUsers();
    } catch (err) {
      usersErr = String(err);
    }
  }
  async function addUser() {
    const s = newSubject.trim();
    if (!s) return;
    const roles = newRoles.size ? [...newRoles] : ['reader']; // default to least-privilege
    try {
      await setUserRoles(s, roles);
      newSubject = '';
      newRoles = new Set(['reader']);
      await loadUsers();
    } catch (err) {
      usersErr = String(err);
    }
  }

  // API tokens — admin-only.
  let tokens = $state<ApiTokenMeta[]>([]);
  let newTokenLabel = $state('');
  let newTokenRoles = $state<Set<string>>(new Set(['reader'])); // roles for a new token
  let newSecret = $state(''); // the just-issued secret, shown once
  let tokensErr = $state('');
  let tokensLoaded = false;

  $effect(() => {
    if (isAdmin && !tokensLoaded) {
      tokensLoaded = true;
      void loadTokens();
    }
  });

  async function loadTokens() {
    try {
      tokens = await listTokens();
    } catch (err) {
      tokensErr = String(err);
    }
  }
  async function createTok() {
    const label = newTokenLabel.trim();
    if (!label) return;
    const roles = newTokenRoles.size ? [...newTokenRoles] : ['reader'];
    try {
      const created = await createToken(label, roles);
      newSecret = created.secret; // shown once
      newTokenLabel = '';
      newTokenRoles = new Set(['reader']);
      await loadTokens();
    } catch (err) {
      tokensErr = String(err);
    }
  }
  async function revokeTok(id: string) {
    try {
      await revokeToken(id);
      await loadTokens();
    } catch (err) {
      tokensErr = String(err);
    }
  }
  function copySecret() {
    void navigator.clipboard?.writeText(newSecret);
  }

  // Segmented binds to a string; round-trip through the typed setters.
  let themeV = $state<string>($theme);
  let accentV = $state<string>($accent);
  let densityV = $state<string>($density);
  $effect(() => setTheme(themeV as Theme));
  $effect(() => setAccent(accentV as Accent));
  $effect(() => setDensity(densityV as Density));

  const engine = (import.meta.env.VITE_ENGINE_API as string) || window.location.origin;
  const connection: [string, string][] = [
    [t('settings.engine'), engine],
    [t('settings.transport'), 'REST + Arrow'],
    [t('settings.oidc'), config.oidc?.issuer ?? t('settings.oidcOpen')],
  ];
  const about: [string, string][] = [
    [t('settings.version'), build.version],
    [t('settings.mode'), build.mode],
    [t('settings.license'), build.license],
  ];
</script>

<section aria-labelledby="screen-heading" class="settings">
  <h1 id="screen-heading" class="sr-only">{t('settings.title')}</h1>
  <div class="screen-toolbar">
    <p class="sub">{t('settings.sub')}</p>
  </div>

  <div class="cards">
    <div class="card">
      <h2>{t('settings.appearance')}</h2>
      <div class="row">
        <span class="label">{t('prefs.theme')}</span>
        <Segmented
          bind:value={themeV}
          options={[
            { value: 'light', label: t('prefs.light') },
            { value: 'dark', label: t('prefs.dark') },
          ]}
        />
      </div>
      <div class="row">
        <span class="label">{t('settings.accent')}</span>
        <Segmented
          bind:value={accentV}
          options={[
            { value: 'blue', label: t('settings.accentBlue') },
            { value: 'orange', label: t('settings.accentOrange') },
            { value: 'green', label: t('settings.accentGreen') },
          ]}
        />
      </div>
      <div class="row">
        <span class="label">{t('settings.density')}</span>
        <Segmented
          bind:value={densityV}
          options={[
            { value: 'compact', label: t('settings.compact') },
            { value: 'comfortable', label: t('settings.comfortable') },
          ]}
        />
      </div>
    </div>

    <div class="card">
      <h2>{t('settings.connection')}</h2>
      <KeyValue entries={connection} />
    </div>

    <div class="card">
      <div class="card-head">
        <h2>{t('settings.users')}</h2>
        {#if isAdmin}<Badge tone="accent">{t('settings.admin')}</Badge>{/if}
      </div>
      {#if user?.authenticated}
        <div class="identity">
          <span class="avatar">{initialsOf(user)}</span>
          <div>
            <div class="who">{user.display_name || user.subject}</div>
            {#if user.email}<div class="muted small">{user.email}</div>{/if}
            <div class="muted">
              {user.roles.length ? user.roles.join(', ') : t('settings.noRoles')}
            </div>
          </div>
        </div>
      {:else}
        <p class="muted">{t('settings.signedOut')}</p>
      {/if}

      {#if isAdmin}
        {#if usersErr}<p role="alert" class="error small">{usersErr}</p>{/if}
        <table class="users-table">
          <thead>
            <tr>
              <th>{t('settings.subject')}</th>
              {#each roleCatalog as role (role)}<th class="role-col">{role}</th>{/each}
            </tr>
          </thead>
          <tbody>
            {#each users as u (u.subject)}
              <tr>
                <td class="mono">{u.subject}</td>
                {#each roleCatalog as role (role)}
                  <td class="role-col">
                    <input
                      type="checkbox"
                      checked={u.roles.includes(role)}
                      aria-label={`${u.subject}: ${role}`}
                      onchange={(e) => toggleRole(u.subject, role, e.currentTarget.checked)}
                    />
                  </td>
                {/each}
              </tr>
            {/each}
            {#if users.length === 0}
              <tr
                ><td colspan={roleCatalog.length + 1} class="muted small"
                  >{t('settings.noUsers')}</td
                ></tr
              >
            {/if}
          </tbody>
        </table>
        <form class="add-user" onsubmit={(e) => (e.preventDefault(), addUser())}>
          <input
            bind:value={newSubject}
            placeholder={t('settings.subject')}
            aria-label={t('settings.addUser')}
            autocomplete="off"
          />
          <div class="role-pick" role="group" aria-label={t('settings.roles')}>
            {#each roleCatalog as role (role)}
              <label class="role-chk">
                <input
                  type="checkbox"
                  checked={newRoles.has(role)}
                  onchange={() => (newRoles = toggleInSet(newRoles, role))}
                />
                {role}
              </label>
            {/each}
          </div>
          <button type="submit" disabled={!newSubject.trim()}>{t('settings.addUser')}</button>
        </form>
      {:else}
        <p class="muted small">{t('settings.usersNote')}</p>
      {/if}
    </div>

    <div class="card">
      <div class="card-head">
        <h2>{t('settings.tokens')}</h2>
        {#if isAdmin}<Badge tone="accent">{t('settings.admin')}</Badge>{/if}
      </div>
      {#if isAdmin}
        {#if tokensErr}<p role="alert" class="error small">{tokensErr}</p>{/if}
        {#if newSecret}
          <div class="new-secret">
            <p class="small">{t('settings.tokenOnce')}</p>
            <code class="mono secret">{newSecret}</code>
            <div class="secret-actions">
              <button type="button" onclick={copySecret}>{t('settings.copy')}</button>
              <button type="button" class="primary" onclick={() => (newSecret = '')}>
                {t('settings.done')}
              </button>
            </div>
          </div>
        {/if}
        {#if tokens.length === 0}
          <p class="muted small">{t('settings.noTokens')}</p>
        {:else}
          <ul class="token-list">
            {#each tokens as tk (tk.id)}
              <li>
                <span class="mono tk-prefix">{tk.prefix}…</span>
                <span class="tk-label">{tk.label}</span>
                <span class="muted small">{tk.roles.join(', ')}</span>
                <button class="link revoke" onclick={() => revokeTok(tk.id)}>
                  {t('settings.revoke')}
                </button>
              </li>
            {/each}
          </ul>
        {/if}
        <form class="add-user" onsubmit={(e) => (e.preventDefault(), createTok())}>
          <input
            bind:value={newTokenLabel}
            placeholder={t('settings.tokenLabel')}
            aria-label={t('settings.tokenLabel')}
            autocomplete="off"
          />
          <div class="role-pick" role="group" aria-label={t('settings.roles')}>
            {#each roleCatalog as role (role)}
              <label class="role-chk">
                <input
                  type="checkbox"
                  checked={newTokenRoles.has(role)}
                  onchange={() => (newTokenRoles = toggleInSet(newTokenRoles, role))}
                />
                {role}
              </label>
            {/each}
          </div>
          <button type="submit" class="primary" disabled={!newTokenLabel.trim()}>
            {t('settings.newToken')}
          </button>
        </form>
      {:else}
        <p class="muted small">{t('settings.tokensNote')}</p>
      {/if}
    </div>

    <div class="card">
      <h2>{t('settings.entLicense')}</h2>
      {#if license}
        <p class="lic-badge">
          <Badge tone={license.licensed ? 'accent' : 'default'}>
            {license.licensed ? t('settings.licenseEnterprise') : t('settings.licenseFree')}
          </Badge>
        </p>
        <KeyValue entries={licenseEntries} />
      {:else}
        <p class="muted">—</p>
      {/if}
    </div>

    <div class="card">
      <h2>{t('settings.about')}</h2>
      <KeyValue entries={about} />
    </div>
  </div>
</section>

<style>
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 1rem;
    align-items: start;
  }
  .card {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 14px 16px;
  }
  .card h2 {
    margin: 0 0 0.75rem;
    font-size: 0.95rem;
  }
  .card-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0.75rem;
  }
  .card-head h2 {
    margin: 0;
  }
  .row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 6px 0;
  }
  .label {
    color: var(--text-2);
    font-weight: 500;
  }
  .identity {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-bottom: 0.6rem;
  }
  .avatar {
    width: 30px;
    height: 30px;
    border-radius: 50%;
    background: var(--accent-weak);
    color: var(--accent);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-weight: 600;
    font-size: 0.8rem;
  }
  .who {
    font-weight: 600;
  }
  .small {
    font-size: 0.9em;
  }
  .users-table {
    width: 100%;
    border-collapse: collapse;
    margin: 0.6rem 0;
    font-size: 0.9em;
  }
  .users-table th,
  .users-table td {
    text-align: left;
    padding: 0.35rem 0.6rem;
    border-bottom: 1px solid var(--line);
  }
  .users-table th {
    color: var(--text-3);
    font-weight: 600;
    font-size: 0.85em;
  }
  .users-table .role-col {
    text-align: center;
    width: 4.5rem;
  }
  .add-user {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    margin-top: 0.4rem;
  }
  .add-user input {
    flex: 1;
    min-width: 0;
  }
  /* Role picker on add-user / new-token — checkboxes from the assignable-role catalog. */
  .role-pick {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }
  .role-chk {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    font-size: 0.85em;
    color: var(--muted);
  }
  .new-secret {
    border: 1px solid var(--accent);
    background: var(--accent-weakest);
    border-radius: 8px;
    padding: 0.6rem 0.7rem;
    margin: 0.5rem 0;
  }
  .new-secret .secret {
    display: block;
    word-break: break-all;
    margin: 0.4rem 0;
    font-size: 0.85em;
  }
  .secret-actions {
    display: flex;
    gap: 0.4rem;
  }
  .token-list {
    list-style: none;
    margin: 0.5rem 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }
  .token-list li {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    font-size: 0.9em;
  }
  .token-list .tk-label {
    font-weight: 600;
  }
  .token-list .revoke {
    margin-left: auto;
    color: var(--warn);
    border: 0;
    background: transparent;
    cursor: pointer;
  }
</style>
