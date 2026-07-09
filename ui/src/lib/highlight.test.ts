import { describe, it, expect } from 'vitest';
import { queryTerms, highlightSegments } from './highlight';

describe('queryTerms', () => {
  it('strips field prefixes, operators, and quotes', () => {
    expect(queryTerms('body:iceberg AND title:search')).toEqual(['iceberg', 'search']);
    expect(queryTerms('"fast search" OR hello')).toEqual(['fast', 'search', 'hello']);
  });

  it('strips wildcards/fuzzy/boost and leading minus', () => {
    expect(queryTerms('web-* foo~2 bar^3 -baz')).toEqual(['web-', 'foo', 'bar', 'baz']);
  });

  it('de-duplicates case-insensitively', () => {
    expect(queryTerms('Iceberg iceberg ICEBERG')).toEqual(['iceberg']);
  });
});

describe('highlightSegments', () => {
  it('marks case-insensitive matches', () => {
    const segs = highlightSegments('Fast search over Iceberg', ['search', 'iceberg']);
    expect(segs.filter((s) => s.hit).map((s) => s.text)).toEqual(['search', 'Iceberg']);
    expect(segs.map((s) => s.text).join('')).toBe('Fast search over Iceberg');
  });

  it('returns one non-hit segment when there are no terms', () => {
    expect(highlightSegments('hello', [])).toEqual([{ text: 'hello', hit: false }]);
  });

  it('treats regex metacharacters in terms literally', () => {
    const segs = highlightSegments('a.b a x b', ['a.b']);
    expect(segs.filter((s) => s.hit).map((s) => s.text)).toEqual(['a.b']);
  });
});
