// UI preferences (theme, accent, density): the design-system knobs, persisted and mirrored onto
// <html> as `data-theme` / `data-accent` / `data-density`. The stylesheet is entirely
// variable-driven, so a preference is just a data-attribute flip. Theme defaults to dark (Brand v1.0
// is dark-first, D40); accent + density have fixed defaults. An explicit choice is remembered in
// localStorage and wins afterwards.
import { writable, type Writable } from 'svelte/store';

export type Theme = 'light' | 'dark';
export type Accent = 'blue' | 'orange' | 'green';
export type Density = 'compact' | 'comfortable';

/** Read a localStorage value, null-safe in private mode / no-storage environments. */
export function read(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

/** Write a localStorage value, no-op if storage is unavailable. */
export function persist(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* private mode / no storage — preference just isn't remembered */
  }
}

/** The OS preference, or `dark` when it can't be determined (jsdom has no matchMedia). */
export function systemTheme(): Theme {
  return typeof matchMedia === 'function' && matchMedia('(prefers-color-scheme: light)').matches
    ? 'light'
    : 'dark';
}

const THEME_KEY = 'growlerdb.theme';
const ACCENT_KEY = 'growlerdb.accent';
const DENSITY_KEY = 'growlerdb.density';

function initialTheme(): Theme {
  const saved = read(THEME_KEY);
  // Brand v1.0 is dark-first (D40): default to dark unless the user has explicitly chosen a theme.
  // `systemTheme()` remains available for a "match system" option, but is no longer the default.
  return saved === 'light' || saved === 'dark' ? saved : 'dark';
}

function initial<T extends string>(key: string, allowed: readonly T[], fallback: T): T {
  const saved = read(key);
  return allowed.includes(saved as T) ? (saved as T) : fallback;
}

const ACCENTS = ['blue', 'orange', 'green'] as const;
const DENSITIES = ['compact', 'comfortable'] as const;

/** Reactive preference stores; subscribe (or re-key) to react to changes. */
export const theme = writable<Theme>(initialTheme());
export const accent = writable<Accent>(initial(ACCENT_KEY, ACCENTS, 'blue'));
export const density = writable<Density>(initial(DENSITY_KEY, DENSITIES, 'compact'));

export function setTheme(value: Theme): void {
  persist(THEME_KEY, value);
  theme.set(value);
}

export function toggleTheme(): void {
  theme.update((current) => {
    const next: Theme = current === 'dark' ? 'light' : 'dark';
    persist(THEME_KEY, next);
    return next;
  });
}

export function setAccent(value: Accent): void {
  persist(ACCENT_KEY, value);
  accent.set(value);
}

export function setDensity(value: Density): void {
  persist(DENSITY_KEY, value);
  density.set(value);
}

/** Mirror a store onto an <html> data attribute for the whole session. */
function mirror<T extends string>(store: Writable<T>, attr: string): void {
  store.subscribe((value) => {
    if (typeof document === 'undefined') return;
    document.documentElement.dataset[attr] = value;
  });
}

/** Mirror the active theme onto <html> (data-theme + color-scheme). */
export function applyTheme(): void {
  theme.subscribe((value) => {
    if (typeof document === 'undefined') return;
    document.documentElement.dataset.theme = value;
    document.documentElement.style.colorScheme = value;
  });
}

/** Mirror all design-system prefs (theme, accent, density) onto <html> for the session. */
export function applyPrefs(): void {
  applyTheme();
  mirror(accent, 'accent');
  mirror(density, 'density');
}
