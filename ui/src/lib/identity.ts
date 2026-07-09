// The signed-in identity (task-103): the verified `GET /v1/me` result, fetched once at app start
// and shared via a store. This is server truth (the gateway validated the token), replacing the
// earlier client-side JWT decode for the header + Settings.
import { writable } from 'svelte/store';
import { me as fetchMe, type Me } from './api';

/** The current identity, or `null` until loaded / when not signed in. */
export const identity = writable<Me | null>(null);

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
