import { test, expect } from '@playwright/test';
import { installMocks } from './mocks';

test.describe('Indexes', () => {
  test('lists indexes and expands per-index stats', async ({ page }) => {
    await installMocks(page);
    await page.goto('/indexes');

    await expect(page.getByRole('button', { name: 'telemetry' })).toBeVisible();
    await expect(page.getByText('ready', { exact: true })).toBeVisible();
    // Summary line + reconciled columns: 1 index, shards "3 × 2" (3 shards, 2 copies —
    // shard 0 has a replica), and a real backup cell (mock reports a present backup at snapshot 42).
    await expect(page.getByText('1 index(es)')).toBeVisible();
    const row = page.locator('.ix-table tbody tr').first();
    await expect(row).toContainText('3 × 2');
    await expect(row).toContainText('snapshot 42');

    await page.getByRole('button', { name: 'telemetry' }).click();
    // The detail stats strip + Policies cards surface num_docs and the checkpoint.
    await expect(page.getByText('12345').first()).toBeVisible();
    await expect(page.getByText('snap-42').first()).toBeVisible();
    // Back navigation returns to the list.
    await page.getByRole('button', { name: '← All indexes' }).click();
    await expect(page.getByRole('button', { name: 'Create index' })).toBeVisible();
  });

  test('the list summarizes rebuilding indexes and backup states', async ({ page }) => {
    await installMocks(page, {
      indexes: {
        json: {
          indexes: [
            { name: 'telemetry', status: 'ready' },
            { name: 'events', status: 'reindexing' },
          ],
        },
      },
      backupStatus: { json: { configured: false, present: false } },
    });
    await page.goto('/indexes');

    // Subtitle counts total + rebuilding; an unconfigured backup target renders "Off".
    await expect(page.getByText('2 index(es) · 1 rebuilding')).toBeVisible();
    await expect(page.locator('.ix-table tbody tr').first()).toContainText('Off');
  });

  test('the Mapping tab renders per-field flags + the blocked-field callout', async ({
    page,
  }) => {
    await installMocks(page);
    await page.goto('/indexes');
    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Mapping' }).click();

    const table = page.locator('.map-table');
    await expect(table).toBeVisible();
    // The identifier carries a PK badge.
    await expect(table.locator('tr', { hasText: 'id' }).getByText('PK')).toBeVisible();
    // A cached fast field; a blocked field with the warning callout.
    await expect(table.getByText('device_id')).toBeVisible();
    await expect(table.locator('tr.blocked', { hasText: 'body' })).toBeVisible();
    await expect(table.getByText('blocked').first()).toBeVisible();
    await expect(page.getByText(/can’t be cached \(D23\)/)).toBeVisible();
  });

  test('the Shards tab renders the health grid + primary/replica counts', async ({
    page,
  }) => {
    await installMocks(page);
    await page.goto('/indexes');
    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Shards' }).click();

    // 2 shards have a primary; 1 has a replica.
    await expect(page.getByText('2 primaries · 1 replicas')).toBeVisible();
    // One cell per shard (3), colored by state.
    await expect(page.locator('.shard-cell')).toHaveCount(3);
    await expect(page.locator('.shard-cell.warn')).toHaveCount(1); // the building shard
  });

  test('Maintenance compact + backup actions run over REST', async ({ page }) => {
    await installMocks(page);
    page.on('dialog', (d) => d.accept()); // accept the confirm()s
    await page.goto('/indexes');
    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Maintenance' }).click();

    // Backup target is configured (mock) → the last-backup line shows.
    await expect(page.getByText('Last backup: snapshot 42')).toBeVisible();

    // Compact reports the before/after segment counts.
    await page.getByRole('button', { name: 'Compact segments' }).click();
    await expect(page.getByText('Compacted: 4 → 1 segments')).toBeVisible();

    // Backup reports the result.
    await page.getByRole('button', { name: 'Back up now' }).click();
    await expect(page.getByText('Backed up 12 files at snapshot 42')).toBeVisible();
  });

  test('the Activity tab renders the lifecycle event stream', async ({ page }) => {
    await installMocks(page);
    await page.goto('/indexes');
    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Activity' }).click();

    const log = page.locator('.activity');
    await expect(log.getByText('alias `live` → `telemetry` swapped')).toBeVisible();
    await expect(log.getByText('index `telemetry` created')).toBeVisible();
  });

  test('creates an index from source introspection', async ({ page }) => {
    await installMocks(page);
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'Create index' }).click();
    const form = page.locator('form[aria-label="Create index"]');
    await expect(form).toBeVisible();

    await form.locator('#c-name').fill('telemetry_v2');
    await form.locator('#c-table').fill('factory.telemetry');
    await form.getByRole('button', { name: 'Introspect' }).click();

    // Introspection populated the schema → the field-selection fieldset renders.
    await expect(form.getByText('Field selection')).toBeVisible();

    await form.getByRole('button', { name: 'Create', exact: true }).click();

    // On success the form closes (showCreate=false) and the list refreshes — no error surfaced.
    await expect(form).toHaveCount(0);
    await expect(page.getByRole('alert')).toHaveCount(0);
  });

  test('declares a timestamp column so the definition carries a format override', async ({
    page,
  }) => {
    await installMocks(page);
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'Create index' }).click();
    const form = page.locator('form[aria-label="Create index"]');
    await form.locator('#c-name').fill('telemetry_v2');
    await form.locator('#c-table').fill('factory.telemetry');
    await form.getByRole('button', { name: 'Introspect' }).click();

    // The Time field section renders after introspection; pick an epoch column + its format.
    await expect(form.getByText('Time field', { exact: true })).toBeVisible();
    await form.locator('#c-time-field').selectOption('reading');
    await form.locator('#c-time-format').selectOption('epoch_ms');

    const createReq = page.waitForRequest(
      (r) => r.url().endsWith('/v1/indexes') && r.method() === 'POST',
    );
    await form.getByRole('button', { name: 'Create', exact: true }).click();
    const body = JSON.parse((await createReq).postData() ?? '{}') as { definition: string };
    expect(body.definition).toContain('{ path: reading, format: epoch_ms, fast: true }');

    await expect(form).toHaveCount(0);
  });

  test('configures time windowing over the declared time field', async ({ page }) => {
    await installMocks(page);
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'Create index' }).click();
    const form = page.locator('form[aria-label="Create index"]');
    await form.locator('#c-name').fill('telemetry_v2');
    await form.locator('#c-table').fill('factory.telemetry');
    await form.getByRole('button', { name: 'Introspect' }).click();

    // Windowing needs a time field; declare one, then enable + configure windowing.
    await form.locator('#c-time-field').selectOption('reading');
    await form.getByText('Enable time windowing').click();
    await form.locator('#c-granularity').selectOption('weekly');
    await form.locator('#c-hot-windows').fill('3');

    const createReq = page.waitForRequest(
      (r) => r.url().endsWith('/v1/indexes') && r.method() === 'POST',
    );
    await form.getByRole('button', { name: 'Create', exact: true }).click();
    const body = JSON.parse((await createReq).postData() ?? '{}') as { definition: string };
    expect(body.definition).toContain(
      'windowing: { field: reading, granularity: weekly, hot_windows: 3 }',
    );
    expect(body.definition).toContain('{ path: reading, format: epoch_ms, fast: true }');

    await expect(form).toHaveCount(0);
  });

  test('reindexes the selected index and shows the rebuilt result', async ({ page }) => {
    await installMocks(page);
    page.on('dialog', (d) => d.accept()); // accept the confirm()
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Maintenance' }).click();
    await page.getByRole('button', { name: 'Reindex' }).click();

    await expect(page.getByRole('status')).toContainText('Rebuilt: 12345 docs at snapshot 43');
  });

  test('surfaces the write-fence guardrail when a reindex is already running', async ({ page }) => {
    await installMocks(page, {
      reindex: {
        status: 412,
        body: JSON.stringify({ message: 'a reindex is already in progress' }),
      },
    });
    page.on('dialog', (d) => d.accept());
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'telemetry' }).click();
    await page.getByRole('tab', { name: 'Maintenance' }).click();
    await page.getByRole('button', { name: 'Reindex' }).click();

    await expect(page.getByRole('alert')).toContainText('already in progress');
  });

  test('blocks a create with the server reason (D23 cached-field hard-block)', async ({ page }) => {
    await installMocks(page, {
      createIndex: { status: 400, body: JSON.stringify({ message: 'cached field not allowed' }) },
    });
    await page.goto('/indexes');

    await page.getByRole('button', { name: 'Create index' }).click();
    const form = page.locator('form[aria-label="Create index"]');
    await form.locator('#c-name').fill('bad');
    await form.locator('#c-table').fill('factory.telemetry');
    await form.getByRole('button', { name: 'Create', exact: true }).click();

    await expect(form.getByRole('alert')).toContainText('cached field not allowed');
  });
});
