import { test, expect } from '@playwright/test';
import { installMocks } from './mocks';

test.describe('Search & Explore', () => {
  test('runs a query, renders ranked hits, and hydrates a row', async ({ page }) => {
    await installMocks(page);
    await page.goto('/');

    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');

    await expect(page.getByText('2 result(s)')).toBeVisible();
    await expect(page.locator('.results .id').first()).toContainText('evt-1');
    // Cached display fields render as cells in the results table row.
    await expect(page.locator('.results .hit').first()).toContainText('sensor-1');

    // Open the document drawer for the first hit → authoritative Iceberg row (Fields tab).
    await page.locator('.results .hit').first().click();
    const drawer = page.getByRole('dialog');
    await expect(drawer).toBeVisible();
    await expect(drawer.getByRole('heading', { name: 'evt-1' })).toBeVisible();
    await expect(drawer).toContainText('temperature within range');
    // Explain tab shows the real BM25 tree, analyzed terms, timings, and shard counts.
    await drawer.getByRole('tab', { name: 'Explain' }).click();
    await expect(drawer.getByText('BM25 score')).toBeVisible();
    await expect(drawer.getByText('TermQuery(status:ok)')).toBeVisible();
    await expect(drawer.getByText('1 of 3 shards scanned')).toBeVisible();

    // JSON tab shows the raw search hit (key + cached fields).
    await drawer.getByRole('tab', { name: 'JSON' }).click();
    await expect(drawer.locator('pre')).toContainText('_score');

    await drawer.getByRole('button', { name: 'Close' }).click();
    await expect(drawer).toHaveCount(0);
  });

  test('shows the empty state for a query with no matches', async ({ page }) => {
    await installMocks(page, { search: { json: { total: 0, partial: false, hits: [] } } });
    await page.goto('/');
    await page.fill('#query', 'status:offline');
    await page.press('#query', 'Enter');

    await expect(page.getByText('0 result(s)')).toBeVisible();
    await expect(page.locator('.results li')).toHaveCount(0);
  });

  test('surfaces a server error inline', async ({ page }) => {
    await installMocks(page, { search: { status: 500, json: {} } });
    await page.goto('/');
    await page.fill('#query', 'boom');
    await page.press('#query', 'Enter');

    const alert = page.getByRole('alert');
    await expect(alert).toBeVisible();
    await expect(alert).toContainText('500');
  });

  test('flags partial results when a shard is down', async ({ page }) => {
    await installMocks(page, {
      search: {
        json: {
          total: 1,
          partial: true,
          hits: [
            { coordinates: { identifier: [{ name: 'id', value: 'evt-9' }] }, score: 1, fields: {} },
          ],
        },
      },
    });
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');

    await expect(page.getByRole('status')).toContainText('Partial results');
  });

  test('scopes the search to a selected index', async ({ page }) => {
    await installMocks(page);
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');
    await expect(page.getByText('2 result(s)')).toBeVisible();

    // The scope selector (a styled dropdown) is populated from /v1/indexes; picking one re-runs.
    const scope = page.getByRole('button', { name: 'Index', exact: true });
    await expect(scope).toBeVisible();
    await scope.click();
    await page.getByRole('option', { name: 'telemetry' }).click();
    await expect(page.getByText('2 result(s)')).toBeVisible();
  });

  test('sorts by a field and scrolls with keyset Load-more', async ({ page }) => {
    const hit = (id: string, score: number) => ({
      coordinates: { identifier: [{ name: 'id', value: id }] },
      score,
      fields: { device_id: `sensor-${id}`, status: 'ok' },
    });
    // A sorted response carries next_cursor → the UI offers keyset "Load more".
    await installMocks(page, {
      search: {
        json: { total: 9, partial: false, next_cursor: 'CUR', hits: [hit('1', 1), hit('2', 1)] },
      },
    });
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');
    await expect(page.locator('.results .hit')).toHaveCount(2);

    // Sorting by a cached field (via the styled dropdown) switches to keyset paging.
    await page.getByRole('button', { name: 'Sort', exact: true }).click();
    await page.getByRole('option', { name: 'device_id' }).click();
    const more = page.getByRole('button', { name: 'Load more' });
    await expect(more).toBeVisible();
    await more.click();
    // The next keyset page is appended (not replaced).
    await expect(page.locator('.results .hit')).toHaveCount(4);
  });

  test('facets refine the query via active-filter chips', async ({ page }) => {
    await installMocks(page);
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');
    await expect(page.getByText('2 result(s)')).toBeVisible();

    // The facets rail renders a server-computed group (device_id) with value + count.
    const rail = page.locator('.col-rail');
    await expect(rail.getByRole('heading', { name: 'Facets' })).toBeVisible();
    const facetVal = rail.getByRole('button', { name: /sensor-1\s*5/ });
    await expect(facetVal).toBeVisible();

    // Selecting a facet value adds a removable filter chip.
    await facetVal.click();
    const chip = page.locator('.filter-chip');
    await expect(chip).toContainText('device_id');
    await expect(chip).toContainText('sensor-1');

    // Removing the chip drops the filter.
    await chip.click();
    await expect(page.locator('.filter-chip')).toHaveCount(0);
  });

  test('time filter scopes the query to a detected timestamp column', async ({
    page,
  }) => {
    await installMocks(page, {
      describeIndex: {
        json: {
          name: 'telemetry',
          snapshot: 42,
          num_docs: 1,
          generation_count: 1,
          checkpoint: 'snap-42',
          time_fields: ['reading_time'],
        },
      },
    });
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');
    await expect(page.getByText('2 result(s)')).toBeVisible();

    // A timestamp column was detected → the time button appears; open it.
    const timeBtn = page.getByRole('button', { name: /Time/ });
    await expect(timeBtn).toBeVisible();
    await timeBtn.click();

    // Pick a relative range and apply → an active time chip on the detected field.
    await page.getByLabel('Range', { exact: true }).selectOption('24h');
    await page.getByRole('button', { name: 'Apply' }).click();
    const chip = page.locator('.filter-chip.time');
    await expect(chip).toContainText('reading_time');
    await expect(page.getByText('2 result(s)')).toBeVisible();

    // Clearing the chip drops the time filter.
    await chip.click();
    await expect(page.locator('.filter-chip.time')).toHaveCount(0);
  });

  test('the stats line shows the shards-scanned ratio', async ({ page }) => {
    await installMocks(page, {
      search: {
        json: {
          total: 1284,
          shards_scanned: 6,
          shards_total: 64,
          hits: [
            { coordinates: { identifier: [{ name: 'id', value: 'evt-1' }] }, score: 1, fields: {} },
          ],
        },
      },
    });
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');

    const statbar = page.locator('.statbar .count');
    await expect(statbar).toContainText('1284 result(s)');
    await expect(statbar).toContainText('6/64 shards');
  });

  test('table cells highlight matched terms and format DATE columns', async ({
    page,
  }) => {
    const micros = Date.UTC(2026, 5, 30, 12, 34, 56) * 1000; // 2026-06-30 12:34:56 UTC
    await installMocks(page, {
      describeIndex: {
        json: {
          name: 'telemetry',
          snapshot: 42,
          num_docs: 1,
          generation_count: 1,
          checkpoint: 'snap-42',
          time_fields: ['ingest_ts'],
        },
      },
      search: {
        json: {
          total: 1,
          hits: [
            {
              coordinates: { identifier: [{ name: 'id', value: 'evt-9' }] },
              score: 2,
              fields: { body: 'temperature within range', ingest_ts: micros, status: 'ok' },
            },
          ],
        },
      },
    });
    await page.goto('/');
    await page.fill('#query', 'temperature');
    await page.press('#query', 'Enter');
    await expect(page.getByText('1 result(s)')).toBeVisible();

    // The query term is highlighted inside the body cell.
    await expect(page.locator('.hit mark')).toHaveText('temperature');
    // The DATE column (epoch micros) renders formatted UTC in its cell.
    await expect(page.locator('.hit')).toContainText('2026-06-30 12:34:56');
    // Every cached field is a cell in the same row — including the status value.
    await expect(page.locator('.hit')).toContainText('ok');
  });

  test('time filter stays disabled when the index reports no DATE columns', async ({
    page,
  }) => {
    // Default describeIndex mock carries no `time_fields`. The backend always populates the field,
    // so empty means "no DATE column" — the time control must stay disabled, NOT re-derive fields
    // from the mapping (the removed client-side fallback).
    await installMocks(page);
    await page.goto('/');
    await page.fill('#query', 'status:ok');
    await page.press('#query', 'Enter');
    await expect(page.getByText('2 result(s)')).toBeVisible();

    // (The disabled button's accessible name is the "no columns" aria-label, so target by class.)
    await expect(page.locator('.time-btn')).toBeDisabled();
  });

  test('saved searches load from the server when authenticated', async ({ page }) => {
    await installMocks(page, {
      savedQueries: {
        json: {
          queries: [
            {
              id: 'sq1',
              name: 'critical sensors',
              query: 'status:critical',
              state: '{"query":"status:critical","syntax":"lucene"}',
              shared: true,
            },
          ],
        },
      },
    });
    // A bearer token makes the UI use the API instead of localStorage.
    await page.addInitScript(() => sessionStorage.setItem('growlerdb.token', 'test.jwt.token'));
    await page.goto('/');

    // The rail is populated from /v1/saved-queries (by name), with a shared marker.
    const item = page.locator('.col-rail .saved-q', { hasText: 'critical sensors' });
    await expect(item).toBeVisible();

    // Restoring re-applies the saved query state and runs it.
    await item.click();
    await expect(page.locator('#query')).toHaveValue('status:critical');
    await expect(page.getByText('2 result(s)')).toBeVisible();
  });

  test('toggles the KQL syntax selector', async ({ page }) => {
    await installMocks(page);
    await page.goto('/');

    const kql = page.getByRole('button', { name: 'KQL', pressed: false });
    await kql.click();
    await expect(page.getByRole('button', { name: 'KQL' })).toHaveAttribute('aria-pressed', 'true');
  });
});
