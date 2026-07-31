// The signed-in identity: the verified `GET /v1/me` result (server truth — the gateway validated
// the token), fetched once at app start and shared via a store for the header + Settings.
import { writable, derived } from 'svelte/store';
import { me as fetchMe, type Me } from './api';

/** The current identity, or `null` until loaded / when not signed in. */
export const identity = writable<Me | null>(null);

/** Roles that grant the server's `admin` scope (mirrors rbac.rs: admin + the index-admin/service
 * aliases). */
const ADMIN_ROLES = new Set(['admin', 'index-admin', 'service']);

/** Whether the signed-in user holds a role granting the Admin scope. */
export const isAdmin = derived(identity, ($id) => !!$id?.roles?.some((r) => ADMIN_ROLES.has(r)));

/** Whether the UI should surface Admin-scoped reads/actions (backup, compact, …). True in open mode
 * (unauthenticated ⇒ the gateway isn't enforcing RBAC) or for an admin-granting role — so a
 * closed-mode reader/operator session doesn't fire calls the gateway answers 403. */
export const canAdminister = derived(
  identity,
  ($id) => !$id?.authenticated || !!$id?.roles?.some((r) => ADMIN_ROLES.has(r)),
);

/** (Re)load `/v1/me` into the store. Never throws — a failure resolves to `null` (anonymous). */
export async function refreshIdentity(): Promise<void> {
  identity.set(await fetchMe());
}

/** A short avatar label from a display name or subject (initials, max two). */
export function initialsOf(me: Me | null): string {
  const label = me?.display_name || me?.subject || '';
  const parts = label.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return '·';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}
