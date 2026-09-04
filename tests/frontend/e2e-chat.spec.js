// Playwright E2E: 真实浏览器全流程 (注册→登录→发送→接收)
// 运行前提: wxlike-go 已在 127.0.0.1:8081 运行 (CI/本地手动起服)
const { test, expect } = require('@playwright/test');

const BASE = 'http://127.0.0.1:8081/';
const RAND = Date.now().toString(36);

test('完整用户流程: 注册→登录→发消息→接收→退出', async ({ page }) => {
  // 1. 打开页面
  await page.goto(BASE);
  await expect(page.locator('#auth')).toBeVisible();

  // 2. 注册 (用户名随机避免重复)
  const userA = 'e2e_a_' + RAND;
  const userB = 'e2e_b_' + RAND;
  await page.fill('#aUser', userA);
  await page.fill('#aPass', 'testpass');
  await page.click('#regBtn');
  // 注册成功后直接登录, auth 消失
  await expect(page.locator('#app')).toBeVisible({ timeout: 5000 });

  // 3. 身份显示
  await expect(page.locator('#meUser')).toHaveText(userA);

  // 4. 打开另一个 tab 注册登录 B
  const pageB = await page.context().newPage();
  await pageB.goto(BASE);
  await pageB.fill('#aUser', userB);
  await pageB.fill('#aPass', 'testpass');
  await pageB.click('#regBtn');
  await expect(pageB.locator('#app')).toBeVisible({ timeout: 5000 });

  // 5. A 发消息给 B
  await page.fill('#inp', 'hello from e2e');
  await page.click('#sendbtn');
  await expect(page.locator('#msgs')).toContainText('hello from e2e', { timeout: 5000 });

  // 6. B 打开会话并看到消息 (MAM 历史)
  await pageB.fill('#inp', 'hi back');
  await pageB.click('#sendbtn');
  await expect(pageB.locator('#msgs')).toContainText('hi back', { timeout: 5000 });

  // 7. 退出登录
  await page.click('#logout');
  await expect(page.locator('#auth')).toBeVisible();
  await pageB.close();
});

test('断线重连 (token_login)', async ({ page }) => {
  // 登录态 token 存 localStorage, 刷新页面应自动重连
  const user = 'e2e_re_' + RAND;
  await page.goto(BASE);
  await page.fill('#aUser', user);
  await page.fill('#aPass', 'testpass');
  await page.click('#regBtn');
  await expect(page.locator('#app')).toBeVisible({ timeout: 5000 });

  await page.reload();
  // 刷新后应自动用 token 重连并留在 app (不弹回登录页)
  await expect(page.locator('#app')).toBeVisible({ timeout: 5000 });
});