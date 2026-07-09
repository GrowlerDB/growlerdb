import { describe, it, expect } from 'vitest';
import { t, setLocale, locales, locale } from './i18n';

describe('i18n', () => {
  it('translates a known key', () => {
    expect(t('nav.search')).toBe('Search');
  });

  it('interpolates {param} tokens', () => {
    expect(t('search.results', { count: 3 })).toBe('3 result(s)');
    expect(t('auth.signedInAs', { user: 'alice' })).toBe('Signed in as alice');
  });

  it('falls back to the key for an unknown string', () => {
    expect(t('does.not.exist')).toBe('does.not.exist');
  });

  it('ignores an unregistered locale (stays on en)', () => {
    setLocale('zz');
    expect(t('nav.search')).toBe('Search');
    expect(locale()).toBe('en');
  });

  it('exposes the registered locales for the switcher', () => {
    expect(locales()).toContain('en');
  });
});
