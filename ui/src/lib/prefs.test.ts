import { describe, it, expect, beforeEach, vi } from 'vitest';

// prefs reads localStorage at import time, so reset modules per test to re-evaluate `initial()`.
beforeEach(() => {
  localStorage.clear();
  vi.resetModules();
});

describe('prefs theme', () => {
  it('defaults to dark when nothing is stored and no light OS preference', async () => {
    const { theme } = await import('./prefs');
    expect(get(theme)).toBe('dark');
  });

  it('honors a stored preference over the OS default', async () => {
    localStorage.setItem('growlerdb.theme', 'light');
    const { theme } = await import('./prefs');
    expect(get(theme)).toBe('light');
  });

  it('toggles between light and dark and persists the choice', async () => {
    const { theme, toggleTheme } = await import('./prefs');
    expect(get(theme)).toBe('dark');
    toggleTheme();
    expect(get(theme)).toBe('light');
    expect(localStorage.getItem('growlerdb.theme')).toBe('light');
    toggleTheme();
    expect(get(theme)).toBe('dark');
    expect(localStorage.getItem('growlerdb.theme')).toBe('dark');
  });

  it('setTheme stores and updates the store', async () => {
    const { theme, setTheme } = await import('./prefs');
    setTheme('light');
    expect(get(theme)).toBe('light');
    expect(localStorage.getItem('growlerdb.theme')).toBe('light');
  });

  it('applyTheme mirrors the active theme onto <html>', async () => {
    const { setTheme, applyTheme } = await import('./prefs');
    applyTheme();
    setTheme('light');
    expect(document.documentElement.dataset.theme).toBe('light');
    setTheme('dark');
    expect(document.documentElement.dataset.theme).toBe('dark');
  });
});

describe('prefs accent + density', () => {
  it('default to blue / compact, honoring a stored choice', async () => {
    const a = await import('./prefs');
    expect(get(a.accent)).toBe('blue');
    expect(get(a.density)).toBe('compact');
    localStorage.setItem('growlerdb.accent', 'green');
    localStorage.setItem('growlerdb.density', 'comfortable');
    vi.resetModules();
    const b = await import('./prefs');
    expect(get(b.accent)).toBe('green');
    expect(get(b.density)).toBe('comfortable');
  });

  it('ignore an invalid stored value, falling back to the default', async () => {
    localStorage.setItem('growlerdb.accent', 'magenta');
    const { accent } = await import('./prefs');
    expect(get(accent)).toBe('blue');
  });

  it('setAccent / setDensity persist and update the stores', async () => {
    const { accent, density, setAccent, setDensity } = await import('./prefs');
    setAccent('orange');
    setDensity('comfortable');
    expect(get(accent)).toBe('orange');
    expect(get(density)).toBe('comfortable');
    expect(localStorage.getItem('growlerdb.accent')).toBe('orange');
    expect(localStorage.getItem('growlerdb.density')).toBe('comfortable');
  });

  it('applyPrefs mirrors theme, accent and density onto <html>', async () => {
    const { applyPrefs, setAccent, setDensity, setTheme } = await import('./prefs');
    applyPrefs();
    setTheme('light');
    setAccent('green');
    setDensity('comfortable');
    expect(document.documentElement.dataset.theme).toBe('light');
    expect(document.documentElement.dataset.accent).toBe('green');
    expect(document.documentElement.dataset.density).toBe('comfortable');
  });
});

/** Read a Svelte store's current value synchronously. */
function get<T>(store: { subscribe: (run: (v: T) => void) => () => void }): T {
  let value!: T;
  store.subscribe((v) => (value = v))();
  return value;
}
