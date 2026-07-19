import { describe, it, expect } from 'vitest';
import { semanticBody, hybridBody, RRF_PRESETS, DEFAULT_RRF_K } from './vectorSearch';

describe('semanticBody', () => {
  it('always carries vector_field + query_text, omitting unset options', () => {
    expect(semanticBody('cats', { vectorField: 'body_vec' })).toEqual({
      vector_field: 'body_vec',
      query_text: 'cats',
    });
  });

  it('includes k/filter/syntax/index when set', () => {
    expect(
      semanticBody('cats', {
        vectorField: 'body_vec',
        k: 5,
        filter: 'lang:en',
        syntax: 'kql',
        index: 'docs',
      }),
    ).toEqual({
      vector_field: 'body_vec',
      query_text: 'cats',
      k: 5,
      filter: 'lang:en',
      syntax: 'kql',
      index: 'docs',
    });
  });

  it('omits a zero/negative k and an empty filter/index (engine defaults apply)', () => {
    expect(semanticBody('cats', { vectorField: 'v', k: 0, filter: '', index: '' })).toEqual({
      vector_field: 'v',
      query_text: 'cats',
    });
  });
});

describe('hybridBody', () => {
  it('adds rrf_k on top of the semantic body when set', () => {
    expect(hybridBody('cats', { vectorField: 'body_vec', k: 10, rrfK: 30 })).toEqual({
      vector_field: 'body_vec',
      query_text: 'cats',
      k: 10,
      rrf_k: 30,
    });
  });

  it('omits rrf_k when unset (engine default 60)', () => {
    expect(hybridBody('cats', { vectorField: 'body_vec' })).toEqual({
      vector_field: 'body_vec',
      query_text: 'cats',
    });
  });
});

describe('RRF presets', () => {
  it('offers the documented presets including the default', () => {
    expect(RRF_PRESETS).toContain(DEFAULT_RRF_K);
    expect([...RRF_PRESETS]).toEqual([10, 30, 60, 100]);
  });
});
