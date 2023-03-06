import {test, expect} from '@playwright/test';

const authFile = 'playwright/.auth/user.json';
test('authenticate', async ({page}) => {
    await page.goto('http://localhost:4173/login?next=/dashboards');
    const loginForm = await page.locator('.login-form');
    await loginForm.locator('[name=login-email]').fill('login');
    await loginForm.locator('[name=login-password]').fill('password');
    await loginForm.locator('button:text("Log in")').click();
    // End of authentication steps.

    await page.context().storageState({path: authFile});
});