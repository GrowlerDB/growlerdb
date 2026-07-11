import { test, expect } from '@playwright/test';
import { installMocks } from './mocks';

test.describe('App shell', () => {
  test('the account menu opens and navigates to Settings', async ({ page }) => {
    await installMocks(page);
    await page.goto('/');

    await page.getByRole('button', { name: 'Account menu' }).click();
    await page.getByRole('button', { name: 'Settings', exact: true }).click();

    await expect(page).toHaveURL(/\/settings$/);
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
  });

  test('Settings shows appearance, connection, about, and admin-only notes for a non-admin', async ({
    page,
  }) => {
    await installMocks(page);
    await page.goto('/settings');

    await expect(page.getByRole('heading', { name: 'Appearance' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Connection' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'About' })).toBeVisible();
    // Non-admin: Users & roles + API tokens show read-only notes (both admin-only).
    await expect(page.getByText('Only admins can manage role bindings')).toBeVisible();
    await expect(
      page.getByText('Long-lived programmatic tokens are managed by admins'),
    ).toBeVisible();
    // No management controls for a non-admin.
    await expect(page.getByRole('button', { name: '+ New token' })).toHaveCount(0);
  });

  test('a bookmarked /cluster folds into Observability', async ({ page }) => {
    await installMocks(page);
    await page.goto('/cluster');
    await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible();
  });

  test('an admin manages role bindings in Settings', async ({ page }) => {
    await installMocks(page, {
      me: { json: { authenticated: true, subject: 'ada', display_name: 'Ada', roles: ['admin'] } },
      users: { json: { users: [{ subject: 'bob', roles: ['reader'] }] } },
    });
    await page.goto('/settings');

    // The Users & roles table is live for admins: a row per binding, a column per role.
    const table = page.locator('.users-table');
    await expect(table.getByText('bob')).toBeVisible();
    await expect(table.getByRole('columnheader', { name: 'operator' })).toBeVisible();
    // bob's `reader` is checked, `admin` is not.
    await expect(table.getByRole('checkbox', { name: 'bob: reader' })).toBeChecked();
    await expect(table.getByRole('checkbox', { name: 'bob: admin' })).not.toBeChecked();
    // Granting a role fires the API (no error surfaces).
    await table.getByRole('checkbox', { name: 'bob: admin' }).check();
    await expect(page.getByRole('alert')).toHaveCount(0);
  });

  test('an admin issues an API token (shown once) in Settings', async ({ page }) => {
    await installMocks(page, {
      me: { json: { authenticated: true, subject: 'ada', display_name: 'Ada', roles: ['admin'] } },
      tokens: {
        json: {
          tokens: [{ id: 'old', label: 'pipeline', prefix: 'gdb_live_zz', roles: ['reader'] }],
        },
      },
    });
    await page.goto('/settings');

    // Existing tokens are listed by prefix (masked) — never the full secret.
    await expect(page.getByText('pipeline')).toBeVisible();
    await expect(page.getByText('gdb_live_zz…')).toBeVisible();

    // Issue a new token → the secret is shown once.
    await page.getByPlaceholder('Token label').fill('ci-runner');
    await page.getByRole('button', { name: '+ New token' }).click();
    await expect(page.getByText('shown only once', { exact: false })).toBeVisible();
    await expect(page.getByText('gdb_live_abcd1234secret')).toBeVisible();
  });

  test('the account menu shows the verified identity from /v1/me', async ({ page }) => {
    await installMocks(page, {
      me: {
        json: {
          authenticated: true,
          subject: 'alice@corp',
          display_name: 'Alice Operator',
          email: 'alice@corp.example',
          roles: ['operator', 'reader'],
        },
      },
    });
    await page.goto('/');
    // The verified name shows in the top bar beside the avatar (design-QA T4)…
    await expect(page.getByRole('button', { name: 'Account menu' })).toContainText(
      'Alice Operator',
    );
    await page.getByRole('button', { name: 'Account menu' }).click();
    // …and again in the opened menu, with roles (real identity from the server, not a placeholder).
    const menu = page.getByRole('dialog');
    await expect(menu.getByText('Alice Operator')).toBeVisible();
    await expect(menu.getByText('operator, reader')).toBeVisible();
  });
});
