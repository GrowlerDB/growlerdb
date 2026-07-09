// Minimal i18n (task-45): a message catalog per locale + `t(key, params)`. The baseline
// ships `en`; adding a locale is dropping in a catalog. Interpolation uses `{name}` tokens.
// Runtime switching (task-91): `setLocale` updates `localeStore`; App re-keys on it so every
// `t(...)` re-evaluates. The choice is persisted in localStorage.
import { writable } from 'svelte/store';
import en from './locales/en';

type Catalog = Record<string, string>;

const catalogs: Record<string, Catalog> = { en };
const KEY = 'growlerdb.locale';

function read(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null;
  }
}

const saved = read();
let current = saved && catalogs[saved] ? saved : 'en';

/** Reactive active-locale store; subscribe (or re-key) to re-render on a locale change. */
export const localeStore = writable(current);

/** Locale codes with a registered catalog (drives the switcher). */
export function locales(): string[] {
  return Object.keys(catalogs);
}

/** Switch the active locale if a catalog for it is registered, and remember the choice. */
export function setLocale(locale: string): void {
  if (catalogs[locale]) {
    current = locale;
    try {
      localStorage.setItem(KEY, locale);
    } catch {
      /* no storage — choice just isn't remembered */
    }
    localeStore.set(locale);
  }
}

/** The active locale code. */
export function locale(): string {
  return current;
}

/** Translate `key`, interpolating `{name}` tokens from `params`. Falls back to `en`, then
 *  to the key itself, so a missing translation degrades visibly rather than crashing. */
export function t(key: string, params?: Record<string, string | number>): string {
  let s = catalogs[current]?.[key] ?? catalogs.en[key] ?? key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      s = s.split(`{${k}}`).join(String(v));
    }
  }
  return s;
}
