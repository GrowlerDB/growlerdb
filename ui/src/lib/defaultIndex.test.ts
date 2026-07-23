import { describe, it, expect } from 'vitest';
import { pickDefaultIndex } from './defaultIndex';

describe('pickDefaultIndex', () => {
  const available = ['docs', 'catalog', 'movies'];

  it('honours the saved index when it still exists', () => {
    expect(pickDefaultIndex(available, 'catalog', 'movies')).toBe('catalog');
  });

  it('ignores a saved index that no longer exists, falling to the configured default', () => {
    expect(pickDefaultIndex(available, 'gone', 'movies')).toBe('movies');
  });

  it('uses the configured default when there is no saved index', () => {
    expect(pickDefaultIndex(available, null, 'movies')).toBe('movies');
  });

  it('ignores a configured default that is not available', () => {
    expect(pickDefaultIndex(available, null, 'nope')).toBe('docs');
  });

  it('falls back to the first index when nothing is saved or configured', () => {
    expect(pickDefaultIndex(available, null, undefined)).toBe('docs');
  });

  it('returns empty string when there are no indexes (served-default endpoint)', () => {
    expect(pickDefaultIndex([], null, 'movies')).toBe('');
  });
});
