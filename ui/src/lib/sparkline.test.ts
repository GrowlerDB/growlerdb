import { describe, it, expect } from 'vitest';
import { sparklinePath } from './sparkline';

describe('sparklinePath', () => {
  it('returns empty for no points', () => {
    expect(sparklinePath([], 100, 30)).toBe('');
  });

  it('draws a rising series with the max at the top (smallest y)', () => {
    const pts = sparklinePath([0, 10], 100, 30, 2).split(' ');
    expect(pts).toHaveLength(2);
    const [x0, y0] = pts[0].split(',').map(Number);
    const [x1, y1] = pts[1].split(',').map(Number);
    expect(x0).toBe(0);
    expect(x1).toBe(100); // spans the full width
    expect(y0).toBeGreaterThan(y1); // value 0 sits lower (bigger y) than value 10
    expect(y1).toBe(2); // the max hugs the top inset
  });

  it('centres a flat series and a single point', () => {
    expect(sparklinePath([5, 5, 5], 100, 30)).toBe('0,15 50,15 100,15');
    expect(sparklinePath([7], 100, 30)).toBe('50,15');
  });
});
