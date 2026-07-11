import { test, expect } from '@playwright/test';
import { installMocks } from './mocks';

// The Observability screen organises the SLIs into sections (Search · Runtime · Data · Ingestion ·
// Source · Access) with a persistent Alerts strip. `/ingestion` redirects here.

test.describe('Observability', () => {
  test('renders the Search section — hero + SLI cards, no alerts firing', async ({ page }) => {
    await installMocks(page);
    await page.goto('/observability');

    await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible();
    // Search is the default sub-tab: its hero chart + one card per SLI.
    await expect(page.getByText('Query latency', { exact: false })).toBeVisible();
    await expect(page.locator('.dc-metric')).toHaveCount(8);
    await expect(page.getByText('Query rate', { exact: true })).toBeVisible();
    await expect(page.getByText('Cold cache hit rate')).toBeVisible();
    // Alerts strip: server rules answered (empty) ⇒ the "Server rules" badge + nothing firing.
    await expect(page.getByText('Server rules')).toBeVisible();
    await expect(page.getByText('No alerts firing')).toBeVisible();
    // The Grafana deep-link (runtime-provided URL).
    await expect(page.getByRole('link', { name: /Open in Grafana/ })).toBeVisible();
    // Metrics resolved, so the error banner is absent.
    await expect(page.getByRole('alert')).toHaveCount(0);
  });

  test('sub-tabs expose the other sections, Access last', async ({ page }) => {
    await installMocks(page);
    await page.goto('/observability');

    const tabs = page.getByRole('tab');
    await expect(tabs).toHaveText(['Search', 'Runtime', 'Data', 'Ingestion', 'Source', 'Access']);

    // Source section surfaces the source-health cards.
    await page.getByRole('tab', { name: 'Source' }).click();
    await expect(page.getByText('Avg file size')).toBeVisible();
    await expect(page.getByText('Data files', { exact: true })).toBeVisible();
  });

  test('a card expands into a detail chart, closed with Escape', async ({ page }) => {
    await installMocks(page);
    await page.goto('/observability');

    await page.getByRole('button', { name: 'Expand Query rate' }).click();
    const dialog = page.getByRole('dialog', { name: 'Query rate' });
    await expect(dialog).toBeVisible();
    await expect(
      dialog.getByText('Completed searches per second across the cluster.'),
    ).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
  });

  test('hides the Grafana link when no URL is configured', async ({ page }) => {
    await installMocks(page, { config: { json: { auth_required: false } } });
    await page.goto('/observability');
    await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible();
    await expect(page.getByRole('link', { name: /Open in Grafana/ })).toHaveCount(0);
  });

  test('renders server-side firing alerts in the strip', async ({ page }) => {
    await installMocks(page, {
      alerts: {
        json: {
          alerts: [
            {
              name: 'HighQueryErrorRate',
              severity: 'critical',
              summary: '0.12 query errors/s',
              state: 'firing',
            },
            { name: 'HighQueryLatency', severity: 'warning', summary: 'p99 1.8s', state: 'firing' },
          ],
        },
      },
    });
    await page.goto('/observability');

    await expect(page.getByText('Server rules')).toBeVisible();
    const critical = page.locator('.alert-row.critical');
    await expect(critical).toContainText('HighQueryErrorRate');
    await expect(critical).toContainText('0.12 query errors/s');
    await expect(critical).toContainText('Critical');
    const warning = page.locator('.alert-row:not(.critical)');
    await expect(warning).toContainText('HighQueryLatency');
    await expect(warning).toContainText('Warning');
    await expect(page.getByText('No alerts firing')).toHaveCount(0);
  });

  test('falls back to local SLI checks when the alerts proxy is down', async ({
    page,
  }) => {
    await installMocks(page, { alerts: { status: 502, json: {} } });
    await page.goto('/observability');
    await expect(page.getByText('Local checks')).toBeVisible();
    await expect(page.getByText('No alerts firing')).toBeVisible();
  });

  test('shows a metrics banner when the stats proxy is wholly down', async ({ page }) => {
    await installMocks(page, { statsRange: { status: 502, json: {} } });
    await page.goto('/observability');
    await expect(page.getByRole('alert')).toContainText('Metrics unavailable');
  });
});

test.describe('Observability › Ingestion section', () => {
  test('shows per-index + per-shard ingestion, folded in from the old tab', async ({ page }) => {
    await installMocks(page);
    await page.goto('/observability');
    await page.getByRole('tab', { name: 'Ingestion' }).click();

    // The "keep up?" hero + the per-index binding.
    await expect(page.getByText('Does GrowlerDB keep up?', { exact: false })).toBeVisible();
    await expect(page.getByText('factory.telemetry')).toBeVisible();
    // Worst-shard rollup: one shard is behind by 45s.
    await expect(page.getByText('behind 45s')).toBeVisible();

    // Expand the index → per-shard table (ordinal · node · committed · state · lag).
    await page.locator('.idx-row').first().click();
    await expect(page.getByText('node-a')).toBeVisible();
    await expect(page.getByText('node-b')).toBeVisible();
    await expect(page.getByText('in_sync', { exact: true })).toBeVisible();
    await expect(page.locator('.shard-tbl td').filter({ hasText: '45s' })).toBeVisible();
  });

  test('shows the empty state with no indexes', async ({ page }) => {
    await installMocks(page, { ingestion: { json: { items: [] } } });
    await page.goto('/observability');
    await page.getByRole('tab', { name: 'Ingestion' }).click();
    await expect(page.getByText('No indexes registered yet', { exact: false })).toBeVisible();
  });

  test('surfaces an ingestion-status error inline (without blanking the SLIs)', async ({
    page,
  }) => {
    await installMocks(page, { ingestion: { status: 503, json: {} } });
    await page.goto('/observability');
    await page.getByRole('tab', { name: 'Ingestion' }).click();
    await expect(page.getByRole('alert')).toContainText('Ingestion status unavailable');
    // The SLI cards still render — an ingestion-feed failure is non-fatal.
    await expect(page.getByText('Throughput')).toBeVisible();
  });

  test('/ingestion redirects into the Observability screen', async ({ page }) => {
    await installMocks(page);
    await page.goto('/ingestion');
    await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible();
  });
});
