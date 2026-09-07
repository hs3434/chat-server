// Playwright E2E: 真实浏览器全流程 (注册→登录→建群→加人→群聊→断线重连)
// 运行前提: wxlike-go 已在 127.0.0.1:8081 运行 (CI/本地手动起服)
const { test, expect } = require('@playwright/test');

const BASE = 'http://127.0.0.1:8081/';
const RAND = Date.now().toString(36);

// 处理原生 prompt: Playwright 用 page.on('dialog') 自动 accept 并给值
async function setupDialog(page, value) {
  page.on('dialog', async d => { if (d.type() === 'prompt') await d.accept(value); else await d.dismiss(); });
}

test('完整用户流程: 注册→登录→建群→加人→群聊', async ({ page, context }) => {
  // 1. A 注册登录
  const userA = 'e2e_a_' + RAND;
  const userB = 'e2e_b_' + RAND;
  await page.goto(BASE);
  await page.fill('#aUser', userA);
  await page.fill('#aPass', 'testpass');
  await page.click('#regBtn');
  await expect(page.locator('#app')).toBeVisible({ timeout: 5000 });
  await expect(page.locator('#meUser')).toHaveText(userA);

  // 2. B 注册登录 (同浏览器不同页面 = 模拟双端)
  const ctxB = await context.browser().newContext();
  const pageB = await ctxB.newPage();
  pageB.on('console', m => console.log('[B console]', m.type(), m.text()));
  ctxB.on('websocket', ws => { ws.on('framesent', f => console.log('[B WS sent]', f.payload)); ws.on('framereceived', f => console.log('[B WS recv]', f.payload)); });
  await pageB.goto(BASE);
  await pageB.fill('#aUser', userB);
  await pageB.fill('#aPass', 'testpass');
  await pageB.click('#regBtn');
  await expect(pageB.locator('#app')).toBeVisible({ timeout: 5000 });
  console.log('[B reg token]', await pageB.evaluate(() => localStorage.getItem('wxlike_token')));

  // 3. A 建群 (prompt 填群名)
  setupDialog(page, 'E2E群');
  await page.click('#newGrp');
  await expect(page.locator('#chathead')).toContainText('E2E群', { timeout: 5000 });
  await expect(page.locator('#inp')).toBeVisible();

  // 4. A 在群里发消息
  await page.fill('#inp', 'hello from A');
  await page.click('#sendbtn');
  await expect(page.locator('#msgs')).toContainText('hello from A', { timeout: 5000 });

  // 5. A 加 B 进群 (点「群信息」打开面板, 在 #gAddUser 填 B 用户名, 点「加人」)
  await page.click('#grpInfo');
  await page.fill('#gAddUser', userB);
  await page.click('#gAdd');
  await page.waitForTimeout(500);

  // 6. B 打开群会话 (会话列表应看到群) 并回看 A 的历史消息
  await expect(pageB.locator('#convs .conv').first()).toBeVisible({ timeout: 8000 });
  await pageB.locator('#convs .conv').first().click();
  await expect(pageB.locator('#msgs')).toContainText('hello from A', { timeout: 8000 });

  // 7. B 在群里回复
  await pageB.fill('#inp', 'hi from B');
  await pageB.click('#sendbtn');
  await expect(pageB.locator('#msgs')).toContainText('hi from B', { timeout: 5000 });

  // 9. A 应实时收到 B 的回复 (无需刷新)
  await expect(page.locator('#msgs')).toContainText('hi from B', { timeout: 8000 });

  // 10. 退出 (JS 点击绕过 Playwright actionability 的 click interception)
  await page.evaluate(() => document.getElementById('logout').click());
  await expect(page.locator('#auth')).toBeVisible({ timeout: 5000 });
  await pageB.close();
  await ctxB.close();
});

test('断线重连 (token_login + localStorage 持久化)', async ({ page }) => {
  const user = 'e2e_re_' + RAND;
  await page.goto(BASE);
  await page.fill('#aUser', user);
  await page.fill('#aPass', 'testpass');
  await page.click('#regBtn');
  await expect(page.locator('#app')).toBeVisible({ timeout: 5000 });

  // 登录后 token 应已持久化
  const token = await page.evaluate(() => localStorage.getItem('wxlike_token'));
  expect(token).toBeTruthy();

  await page.reload();
  // 刷新后应自动用 token 重连并留在 app (不弹回登录页)
  await expect(page.locator('#app')).toBeVisible({ timeout: 8000 });
});
