import { describe, it, expect } from 'vitest';
import { queryTerms, queryTermsByField, fieldTerms, highlightSegments } from './highlight';

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

describe('queryTermsByField', () => {
  it('scopes a field term to its field, not others', () => {
    expect(queryTermsByField('category:guide')).toEqual({
      fields: { category: ['guide'] },
      bare: [],
    });
  });

  it('distributes a grouped set across the field', () => {
    expect(queryTermsByField('category:(guide OR reference)')).toEqual({
      fields: { category: ['guide', 'reference'] },
      bare: [],
    });
  });

  it('keeps bare (default-field) terms separate from qualified ones', () => {
    expect(queryTermsByField('hydrate title:iceberg')).toEqual({
      fields: { title: ['iceberg'] },
      bare: ['hydrate'],
    });
  });

  it('ignores negated clauses and ranges', () => {
    expect(queryTermsByField('-archived:true')).toEqual({ fields: {}, bare: [] });
    expect(queryTermsByField('published:[2024-01-01 TO *]')).toEqual({ fields: {}, bare: [] });
  });

  it('fieldTerms returns a field its own terms plus bare terms, and unnamed fields get only bare', () => {
    const scoped = queryTermsByField('category:(guide OR reference)');
    expect(fieldTerms(scoped, 'category')).toEqual(['guide', 'reference']);
    expect(fieldTerms(scoped, 'title')).toEqual([]); // title was never queried ⇒ nothing to mark
    const withBare = queryTermsByField('hydrate');
    expect(fieldTerms(withBare, 'body')).toEqual(['hydrate']);
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
