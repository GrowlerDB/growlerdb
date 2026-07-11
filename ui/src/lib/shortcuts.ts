// Keyboard shortcuts: a pure event→action mapper so the bindings are unit-testable independent of
// the DOM. App.svelte feeds it `keydown`s and performs the returned action.
//
// Bindings: `/` focus search · `g` then s/i/o/e jump to a screen (Gmail-style prefix) ·
// `t` toggle theme · `?` toggle the help overlay · Escape close overlays. Modifier combos and
// keystrokes inside text fields are ignored (Escape still closes, so a field can be escaped).
import type { Route } from './router';

export type ShortcutAction =
  | { kind: 'focus-search' }
  | { kind: 'navigate'; route: Route }
  | { kind: 'toggle-theme' }
  | { kind: 'toggle-help' }
  | { kind: 'close-overlays' }
  | null;

/** Second key of the `g`-prefix → route. */
export const NAV_KEYS: Record<string, Route> = {
  s: '/',
  i: '/indexes',
  o: '/observability',
  e: '/settings',
};

/** True when focus is in a text-entry control, so typing isn't hijacked as a shortcut. */
export function isEditable(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || !el.tagName) return false;
  const tag = el.tagName.toUpperCase();
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable === true;
}

type Keyish = {
  key: string;
  target?: EventTarget | null;
  ctrlKey?: boolean;
  metaKey?: boolean;
  altKey?: boolean;
};

/** Build a stateful handler (the `g` prefix needs to remember the previous keystroke). */
export function createShortcutHandler(): (e: Keyish) => ShortcutAction {
  let pendingG = false;

  return function handle(e: Keyish): ShortcutAction {
    // Escape clears any pending prefix and closes overlays — even from inside a field.
    if (e.key === 'Escape') {
      pendingG = false;
      return { kind: 'close-overlays' };
    }
    // Leave real key-combos and in-field typing alone.
    if (e.ctrlKey || e.metaKey || e.altKey || isEditable(e.target ?? null)) {
      pendingG = false;
      return null;
    }

    if (pendingG) {
      pendingG = false;
      const route = NAV_KEYS[e.key];
      return route ? { kind: 'navigate', route } : null;
    }

    switch (e.key) {
      case '/':
        return { kind: 'focus-search' };
      case 'g':
        pendingG = true;
        return null;
      case 't':
        return { kind: 'toggle-theme' };
      case '?':
        return { kind: 'toggle-help' };
      default:
        return null;
    }
  };
}
