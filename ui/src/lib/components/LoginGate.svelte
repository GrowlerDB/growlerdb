<script lang="ts">
  // Closed-mode login gate (task-127/128): shown full-screen instead of the app when the gateway
  // requires authentication and the caller isn't signed in. The app body isn't mounted behind it,
  // so anonymous users can't see data or trigger 401-ing API calls. Renders a built-in
  // username/password form (task-128) when the server advertises it, else an OIDC sign-in button.
  import { t } from '../i18n';
  import { passwordLogin } from '../api';
  import { setToken } from '../auth';
  import { refreshIdentity } from '../identity';

  let {
    passwordLogin: usePassword = false,
    hasProvider = false,
    onSignIn,
  }: { passwordLogin?: boolean; hasProvider?: boolean; onSignIn: () => void } = $props();

  let username = $state('');
  let password = $state('');
  let error = $state('');
  let busy = $state(false);

  async function submit(event: Event) {
    event.preventDefault();
    if (!username.trim() || busy) return;
    busy = true;
    error = '';
    try {
      const result = await passwordLogin(username.trim(), password);
      setToken(result.token);
      // Re-check identity → the app's reactive gate drops and the console renders.
      await refreshIdentity();
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
    }
  }
</script>

<div class="gate">
  <div class="card">
    <div class="brand">{t('app.title')}</div>
    <h1>{t('auth.gateTitle')}</h1>
    <p class="muted">{t('auth.gateSubtitle')}</p>

    {#if usePassword}
      <form onsubmit={submit}>
        <label class="field">
          <span>{t('auth.username')}</span>
          <input bind:value={username} autocomplete="username" />
        </label>
        <label class="field">
          <span>{t('auth.password')}</span>
          <input type="password" bind:value={password} autocomplete="current-password" />
        </label>
        {#if error}<p class="warn" role="alert">{error}</p>{/if}
        <button class="primary" type="submit" disabled={busy || !username.trim()}>
          {busy ? t('common.loading') : t('auth.gateButton')}
        </button>
      </form>
    {:else if hasProvider}
      <button class="primary" onclick={onSignIn}>{t('auth.gateButton')}</button>
    {:else}
      <p class="warn">{t('auth.gateNoProvider')}</p>
    {/if}
  </div>
</div>

<style>
  .gate {
    min-height: 100vh;
    display: grid;
    place-items: center;
    padding: 2rem;
  }
  .card {
    width: min(420px, 100%);
    text-align: center;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 2.5rem 2rem;
    background: var(--bg-elevated, var(--bg));
  }
  .brand {
    font-weight: 700;
    letter-spacing: 0.02em;
    color: var(--accent);
    margin-bottom: 1.5rem;
  }
  h1 {
    font-size: 1.25rem;
    margin: 0 0 0.5rem;
  }
  .muted {
    color: var(--muted);
    margin: 0 0 1.5rem;
  }
  form {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    text-align: left;
  }
  .field {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    font-size: 0.85rem;
    color: var(--muted);
  }
  .field input {
    padding: 0.5rem 0.6rem;
    border: 1px solid var(--line);
    border-radius: 6px;
    background: var(--bg);
    color: var(--text);
    font-size: 0.95rem;
  }
  .primary {
    background: var(--accent);
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 0.6rem 1.4rem;
    font-size: 0.95rem;
    cursor: pointer;
    margin-top: 0.25rem;
  }
  .primary:hover {
    filter: brightness(1.05);
  }
  .primary:disabled {
    opacity: 0.6;
    cursor: default;
  }
  .warn {
    color: var(--warn, #b45309);
    font-size: 0.9rem;
    margin: 0;
  }
</style>
