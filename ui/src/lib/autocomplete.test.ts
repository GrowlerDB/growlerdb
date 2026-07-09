import { describe, it, expect } from 'vitest';
import { currentFieldToken, withCompletion } from './autocomplete';

describe('currentFieldToken', () => {
  it('extracts the field:prefix being typed at the end', () => {
    expect(currentFieldToken('body:err')).toEqual({ field: 'body', prefix: 'err', start: 0 });
    expect(currentFieldToken('env:prod AND body:err')).toEqual({
      field: 'body',
      prefix: 'err',
      start: 13,
    });
    expect(currentFieldToken('nested.path:va')).toEqual({
      field: 'nested.path',
      prefix: 'va',
      start: 0,
    });
  });

  it('returns null when there is nothing to complete', () => {
    expect(currentFieldToken('body:')).toBeNull(); // empty prefix (suggest needs non-empty text)
    expect(currentFieldToken('hello world')).toBeNull(); // no field context
    expect(currentFieldToken('body:err env:')).toBeNull(); // cursor token is the empty `env:`
  });

  it('does not fire inside phrases or ranges', () => {
    expect(currentFieldToken('body:"quoted')).toBeNull();
    expect(currentFieldToken('rank:[0 TO')).toBeNull();
  });
});

describe('withCompletion', () => {
  it('splices the chosen value in place of the prefix', () => {
    const q = 'env:prod AND body:err';
    const token = currentFieldToken(q)!;
    expect(withCompletion(q, token, 'error')).toBe('env:prod AND body:error');
  });
});
