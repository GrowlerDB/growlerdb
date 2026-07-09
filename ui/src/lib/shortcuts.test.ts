import { describe, it, expect } from 'vitest';
import { createShortcutHandler, isEditable, NAV_KEYS } from './shortcuts';

describe('shortcuts', () => {
  it('maps the single-key bindings', () => {
    const h = createShortcutHandler();
    expect(h({ key: '/' })).toEqual({ kind: 'focus-search' });
    expect(h({ key: 't' })).toEqual({ kind: 'toggle-theme' });
    expect(h({ key: '?' })).toEqual({ kind: 'toggle-help' });
    expect(h({ key: 'Escape' })).toEqual({ kind: 'close-overlays' });
    expect(h({ key: 'x' })).toBeNull();
  });

  it('resolves the g-prefix to a navigation on the second key', () => {
    const h = createShortcutHandler();
    expect(h({ key: 'g' })).toBeNull(); // prefix armed, no action yet
    expect(h({ key: 'i' })).toEqual({ kind: 'navigate', route: NAV_KEYS.i });
    // prefix is consumed — a lone nav key after does nothing
    expect(h({ key: 's' })).toBeNull();
  });

  it('drops the g-prefix when the second key is unmapped', () => {
    const h = createShortcutHandler();
    h({ key: 'g' });
    expect(h({ key: 'z' })).toBeNull();
    // and the prefix is cleared, so the next key is interpreted fresh
    expect(h({ key: '/' })).toEqual({ kind: 'focus-search' });
  });

  it('ignores keystrokes inside text fields, but Escape still closes', () => {
    const h = createShortcutHandler();
    const input = { tagName: 'INPUT' } as unknown as EventTarget;
    expect(h({ key: '/', target: input })).toBeNull();
    expect(h({ key: 'Escape', target: input })).toEqual({ kind: 'close-overlays' });
  });

  it('ignores modifier combos (browser/OS shortcuts win)', () => {
    const h = createShortcutHandler();
    expect(h({ key: '/', metaKey: true })).toBeNull();
    expect(h({ key: 't', ctrlKey: true })).toBeNull();
  });

  it('clears a pending g-prefix when an in-field or modifier key intervenes', () => {
    const h = createShortcutHandler();
    h({ key: 'g' });
    expect(h({ key: 'i', metaKey: true })).toBeNull(); // modifier resets the prefix
    expect(h({ key: 's' })).toBeNull(); // so this is a bare, unmapped key
  });
});

describe('isEditable', () => {
  it('detects text-entry controls', () => {
    expect(isEditable({ tagName: 'INPUT' } as unknown as EventTarget)).toBe(true);
    expect(isEditable({ tagName: 'textarea' } as unknown as EventTarget)).toBe(true);
    expect(isEditable({ tagName: 'SELECT' } as unknown as EventTarget)).toBe(true);
    expect(isEditable({ tagName: 'DIV', isContentEditable: true } as unknown as EventTarget)).toBe(
      true,
    );
    expect(isEditable({ tagName: 'DIV' } as unknown as EventTarget)).toBe(false);
    expect(isEditable(null)).toBe(false);
  });
});
