import { describe, it, expect } from 'vitest';
import { popoverPlacement, type Rect } from './popover';

const vp = { width: 1000, height: 800 };
const anchor = (over: Partial<Rect> = {}): Rect => ({
  top: 100,
  bottom: 130,
  left: 200,
  right: 300,
  width: 100,
  ...over,
});

describe('popoverPlacement', () => {
  it('drops below the anchor, left-aligned', () => {
    const p = popoverPlacement(anchor(), 280, 200, vp);
    expect(p.above).toBe(false);
    expect(p.top).toBe(136); // bottom (130) + gap (6)
    expect(p.left).toBe(200); // anchor left
  });

  it('flips above when there is no room below', () => {
    // Anchor near the bottom; a tall panel doesn't fit below.
    const p = popoverPlacement(anchor({ top: 700, bottom: 730 }), 280, 200, vp);
    expect(p.above).toBe(true);
    expect(p.top).toBe(494); // 700 - 6 - 200
  });

  it('clamps the left edge within the viewport margin', () => {
    const p = popoverPlacement(anchor({ left: 960, right: 990 }), 280, 200, vp);
    expect(p.left).toBe(1000 - 280 - 8); // 712, clamped to keep the panel on-screen
  });
});
