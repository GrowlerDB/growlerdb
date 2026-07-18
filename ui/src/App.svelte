<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { t, locales, locale, setLocale, localeStore } from './lib/i18n';
  import { theme, toggleTheme, applyPrefs } from './lib/prefs';
  import { createShortcutHandler } from './lib/shortcuts';
  import { path, navigate, type Route } from './lib/router';
  import { loadConfig } from './lib/config';
  import { isAuthenticated, login, logout, handleRedirectCallback } from './lib/auth';
  import { serverConfig, UNAUTHORIZED_EVENT } from './lib/api';
  import { identity, refreshIdentity, initialsOf } from './lib/identity';
  import LoginGate from './lib/components/LoginGate.svelte';
  import { clusterHealth, startHealthPolling } from './lib/health';
  import type { Health } from './lib/cluster';
  import Search from './routes/Search.svelte';
  import Indexes from './routes/Indexes.svelte';
  import Settings from './routes/Settings.svelte';
  import Popover from './lib/components/Popover.svelte';
  import StatusDot from './lib/components/StatusDot.svelte';
  // Observability pulls in ECharts; lazy-load it so the heavy chart lib is only fetched when that
  // screen is opened (keeps the initial bundle small).

  const config = loadConfig();
  let authed = $state(isAuthenticated());
  // Closed vs open mode: the server advertises whether auth is required via /v1/config. When
  // required and the caller isn't signed in, the app is replaced by the login gate.
  let authRequired = $state(false);
  // Built-in username/password login available → the gate shows a credential form.
  let passwordLogin = $state(false);
  let showHelp = $state(false);
  let menuOpen = $state(false);
  let menuBtn = $state<HTMLElement | null>(null);
  const localeOptions = locales();
  // The verified identity from GET /v1/me — server truth, loaded on mount.
  const user = $derived($identity);
  // Closed mode + not signed in → gate the app behind login.
  const gated = $derived(authRequired && !user?.authenticated);

  // Mirror the persisted design-system prefs (theme + accent + density) onto <html>.
  applyPrefs();

  const navItems: { route: Route; key: string }[] = [
    { route: '/', key: 'nav.search' },
    { route: '/indexes', key: 'nav.indexes' },
    { route: '/observability', key: 'nav.observability' },
    { route: '/settings', key: 'nav.settings' },
  ];

  // Header health pill: one reactive Health → dot tone + label, pulsing when healthy.
  const HEALTH: Record<Health, { tone: 'ok' | 'warn' | 'muted'; key: string; pulse: boolean }> = {
    ok: { tone: 'ok', key: 'cluster.health.ok', pulse: true },
    warn: { tone: 'warn', key: 'cluster.health.warn', pulse: false },
    down: { tone: 'warn', key: 'cluster.health.down', pulse: false },
    unknown: { tone: 'muted', key: 'cluster.health.unknown', pulse: false },
  };
  const health = $derived(HEALTH[$clusterHealth]);

  // Global keyboard shortcuts: the handler is a pure mapper; we perform its action.
  const shortcut = createShortcutHandler();
  function onKeydown(event: KeyboardEvent) {
    const action = shortcut(event);
    if (!action) return;
    switch (action.kind) {
      case 'focus-search':
        event.preventDefault();
        navigate('/');
        setTimeout(() => document.getElementById('query')?.focus(), 0);
        break;
      case 'navigate':
        navigate(action.route);
        break;
      case 'toggle-theme':
        toggleTheme();
        break;
      case 'toggle-help':
        showHelp = !showHelp;
        break;
      case 'close-overlays':
        showHelp = false;
        menuOpen = false;
        break;
    }
  }

  const helpItems = [
    { keys: ['/'], desc: 'help.focusSearch' },
    { keys: ['g', 's'], desc: 'help.nav' },
    { keys: ['t'], desc: 'help.theme' },
    { keys: ['?'], desc: 'help.help' },
    { keys: ['Esc'], desc: 'help.close' },
  ];

  let stopHealth: (() => void) | undefined;
  // An expired/revoked token surfaces as a 401 from apiFetch: drop the session and re-check
  // identity → in closed mode the gate reappears and the user re-authenticates.
  async function onUnauthorized() {
    authed = false;
    await refreshIdentity();
  }
  onMount(async () => {
    stopHealth = startHealthPolling();
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    // Learn the auth mode before anything else so the gate decision is correct on first paint.
    const cfg = await serverConfig();
    authRequired = cfg.auth_required;
    passwordLogin = cfg.password_login ?? false;
    if (config.oidc) {
      try {
        if (await handleRedirectCallback(config.oidc)) authed = true;
      } catch (err) {
        console.error(err);
      }
    }
    // Load the verified identity — server truth for the header + Settings.
    await refreshIdentity();
  });
  onDestroy(() => {
    stopHealth?.();
    window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  });

  async function signIn() {
    if (config.oidc) await login(config.oidc);
  }
  function signOut() {
    logout();
    authed = false;
    identity.set(null);
    menuOpen = false;
  }
  function go(event: MouseEvent, route: Route) {
    event.preventDefault();
    navigate(route);
  }
  function menuNav(route: Route) {
    menuOpen = false;
    navigate(route);
  }

  // The username shown beside the avatar in the top bar. Mirrors the menu's identity line: the
  // verified name/subject when signed in, else the anon/open-mode label.
  function userLabel(u: typeof user): string {
    if (u?.authenticated) return u.display_name || u.subject;
    return authRequired ? t('menu.anon') : t('menu.openMode');
  }
</script>

<svelte:window onkeydown={onKeydown} />

<a href="#main" class="skip-link">{t('app.skipToContent')}</a>

<!-- Per-item nav glyphs (search / list / down-arrow / bars / sliders). Decorative; the adjacent
     label carries the accessible name. -->
{#snippet navGlyph(route: Route)}
  {#if route === '/'}
    <svg
      width="15"
      height="15"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      stroke-width="1.55"
      aria-hidden="true"
      ><circle cx="7" cy="7" r="4.3"></circle><line
        x1="10.3"
        y1="10.3"
        x2="14"
        y2="14"
        stroke-linecap="round"
      ></line></svg
    >
  {:else if route === '/indexes'}
    <svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"
      ><rect x="2" y="3" width="12" height="2.4" rx="1"></rect><rect
        x="2"
        y="6.8"
        width="12"
        height="2.4"
        rx="1"
      ></rect><rect x="2" y="10.6" width="12" height="2.4" rx="1"></rect></svg
    >
  {:else if route === '/observability'}
    <svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"
      ><rect x="2.5" y="8" width="2.6" height="5.5" rx="0.6"></rect><rect
        x="6.7"
        y="5"
        width="2.6"
        height="8.5"
        rx="0.6"
      ></rect><rect x="10.9" y="2.5" width="2.6" height="11" rx="0.6"></rect></svg
    >
  {:else if route === '/settings'}
    <svg
      width="15"
      height="15"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      stroke-width="1.55"
      aria-hidden="true"
      ><line x1="2" y1="5.5" x2="14" y2="5.5"></line><line x1="2" y1="10.5" x2="14" y2="10.5"
      ></line><circle cx="6" cy="5.5" r="2" fill="var(--panel)"></circle><circle
        cx="10.5"
        cy="10.5"
        r="2"
        fill="var(--panel)"
      ></circle></svg
    >
  {/if}
{/snippet}

{#key $localeStore}
  {#if gated}
    <LoginGate {passwordLogin} hasProvider={!!config.oidc} onSignIn={signIn} />
  {:else}
    <header class="topbar">
      <a class="brand" href="/" onclick={(e) => go(e, '/')} aria-label={t('app.home')}>
        <!-- Waterline mark (the brand berg crossing the waterline) — see brand/favicon.svg. -->
        <svg class="brand-mark" width="26" height="26" viewBox="0 0 32 32" aria-hidden="true">
          <rect width="32" height="32" rx="7.3" fill="#46b8c8" />
          <clipPath id="brand-berg"><circle cx="16" cy="16" r="10" /></clipPath>
          <g clip-path="url(#brand-berg)">
            <rect x="6" y="6" width="20" height="8.8" fill="#fcfcfc" />
            <rect x="6" y="14.8" width="20" height="11.2" fill="#fcfcfc" opacity="0.42" />
          </g>
        </svg>
        <span class="wordmark" aria-hidden="true">growler<span class="db">db</span></span>
      </a>

      <button
        class="health"
        onclick={() => navigate('/observability')}
        title={t('health.tooltip')}
        aria-label={t(health.key)}
      >
        <StatusDot tone={health.tone} pulse={health.pulse} />
        <span>{t(health.key)}</span>
      </button>

      <nav aria-label={t('nav.label')}>
        <ul>
          {#each navItems as item (item.route)}
            <li>
              <a
                href={item.route}
                aria-current={$path === item.route ? 'page' : undefined}
                onclick={(e) => go(e, item.route)}
              >
                {@render navGlyph(item.route)}
                <span>{t(item.key)}</span>
              </a>
            </li>
          {/each}
        </ul>
      </nav>

      <div class="user">
        <button
          bind:this={menuBtn}
          class="avatar-btn"
          onclick={() => (menuOpen = !menuOpen)}
          aria-haspopup="menu"
          aria-expanded={menuOpen}
          aria-label={t('menu.label')}
        >
          <span class="avatar">{initialsOf(user)}</span>
          <span class="user-name">{userLabel(user)}</span>
          <span class="caret" aria-hidden="true">▾</span>
        </button>
      </div>
    </header>

    {#if menuOpen}
      <Popover anchor={menuBtn} onClose={() => (menuOpen = false)} width={250}>
        <div class="menu">
          <div class="menu-id">
            <span class="avatar">{initialsOf(user)}</span>
            <div>
              <div class="menu-name">
                {user?.authenticated
                  ? user.display_name || user.subject
                  : authRequired
                    ? t('menu.anon')
                    : t('menu.openMode')}
              </div>
              {#if user?.authenticated && user.roles.length}
                <div class="menu-role">{user.roles.join(', ')}</div>
              {/if}
            </div>
          </div>
          <hr />
          <button class="menu-item" onclick={() => menuNav('/settings')}>{t('nav.settings')}</button
          >
          <button class="menu-item" onclick={toggleTheme}
            >{$theme === 'dark' ? t('prefs.themeToLight') : t('prefs.themeToDark')}</button
          >
          <button class="menu-item" onclick={() => ((showHelp = true), (menuOpen = false))}
            >{t('help.button')}</button
          >
          {#if localeOptions.length > 1}
            <div class="menu-locale">
              <label class="sr-only" for="locale">{t('prefs.locale')}</label>
              <select
                id="locale"
                value={locale()}
                onchange={(e) => setLocale(e.currentTarget.value)}
              >
                {#each localeOptions as code (code)}
                  <option value={code}>{t('locale.' + code)}</option>
                {/each}
              </select>
            </div>
          {/if}
          {#if config.oidc}
            <hr />
            {#if authed}
              <button class="menu-item" onclick={signOut}>{t('auth.signOut')}</button>
            {:else}
              <button class="menu-item" onclick={signIn}>{t('auth.signIn')}</button>
            {/if}
          {/if}
        </div>
      </Popover>
    {/if}

    <main id="main" tabindex="-1">
      {#if $path === '/'}
        <Search />
      {:else if $path === '/indexes'}
        <Indexes />
      {:else if $path === '/observability'}
        {#await import('./routes/Observability.svelte')}
          <p>{t('common.loading')}</p>
        {:then m}
          {@const Observability = m.default}
          <Observability />
        {/await}
      {:else if $path === '/settings'}
        <Settings />
      {/if}
    </main>

    {#if showHelp}
      <div class="modal-backdrop">
        <div class="modal" role="dialog" aria-modal="true" aria-labelledby="help-title">
          <div class="drawer-head">
            <h2 id="help-title">{t('help.title')}</h2>
            <button class="close" onclick={() => (showHelp = false)} aria-label={t('common.close')}>
              ×
            </button>
          </div>
          <dl class="shortcuts">
            {#each helpItems as item (item.desc)}
              <div>
                <dt>
                  {#each item.keys as k (k)}<kbd>{k}</kbd>{/each}
                </dt>
                <dd>{t(item.desc)}</dd>
              </div>
            {/each}
          </dl>
        </div>
      </div>
    {/if}
  {/if}
{/key}

<style>
  .topbar {
    display: flex;
    align-items: center;
    gap: 1rem;
    height: 54px;
    padding: 0 1.1rem;
    border-bottom: 1px solid var(--line);
    background: var(--panel);
  }
  .brand {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    /* A link to home — keep the header's text styling, add a pointer + focus. */
    color: inherit;
    text-decoration: none;
    cursor: pointer;
  }
  .brand:hover {
    opacity: 0.85;
  }
  .brand-mark {
    flex: 0 0 auto;
    display: block;
  }
  /* Wordmark lockup: Archivo 800, lowercase, tight; the "db" carries the melt identity colour. */
  .wordmark {
    font-family: 'Archivo', 'Instrument Sans', system-ui, sans-serif;
    font-weight: 800;
    font-size: 15.5px;
    letter-spacing: -0.03em;
    color: var(--text);
  }
  .wordmark .db {
    color: var(--melt);
  }
  .health {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 4px 10px;
    border: 1px solid var(--line);
    border-radius: 999px;
    background: var(--panel2);
    color: var(--text-2);
    font: inherit;
    font-weight: 500;
    cursor: pointer;
  }
  .health:hover {
    border-color: var(--line-strong);
    color: var(--text);
  }
  .user {
    margin-left: auto;
  }
  .avatar-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    border: 1px solid var(--line);
    border-radius: 999px;
    padding: 3px 7px 3px 3px;
    background: var(--panel2);
    color: var(--text-2);
    cursor: pointer;
  }
  .avatar-btn:hover {
    border-color: var(--line-strong);
  }
  .user-name {
    font:
      500 12px 'Instrument Sans',
      system-ui,
      sans-serif;
    color: var(--text);
    white-space: nowrap;
    max-width: 12ch;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  @media (max-width: 720px) {
    /* Reclaim top-bar width on narrow viewports — the avatar + caret still identify the menu. */
    .user-name {
      display: none;
    }
  }
  .caret {
    color: var(--text-3);
    font-size: 0.7rem;
  }
  /* User chip avatar carries the melt identity tint (not the interactive glacier accent). */
  .avatar {
    width: 26px;
    height: 26px;
    border-radius: 50%;
    background: var(--melt-weak);
    color: var(--melt);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-weight: 600;
    font-size: 0.72rem;
  }
  .menu {
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .menu hr {
    border: 0;
    border-top: 1px solid var(--line);
    margin: 5px 0;
  }
  .menu-id {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 4px 6px 8px;
  }
  .menu-name {
    font-weight: 600;
  }
  .menu-role {
    color: var(--text-3);
    font-size: 0.85em;
    margin-top: 2px;
  }
  .menu-item {
    text-align: left;
    border: 0;
    background: transparent;
    color: var(--text);
    font: inherit;
    padding: 7px 8px;
    border-radius: 6px;
    cursor: pointer;
  }
  .menu-item:hover {
    background: var(--accent-weakest);
    color: var(--accent);
  }
  .menu-locale {
    padding: 5px 6px;
  }
  .menu-locale select {
    width: 100%;
  }
</style>
