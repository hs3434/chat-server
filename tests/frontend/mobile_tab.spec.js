// E2E: 手机视口 (iPhone 12) 下验证 Tab 栏可见 + 通讯录可用
// 手机视口用 Chromium 模拟 (沙箱无 webkit), viewport+UA+触屏对齐 iPhone 12
const { test, expect } = require('@playwright/test');
const iphone = { viewport: { width: 390, height: 844 }, userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1', hasTouch: true, isMobile: true };
const BASE = process.env.BASE || 'http://127.0.0.1:8081/';
const rnd = Date.now().toString(36).slice(-6);
const UA = 'mob_a_' + rnd, UB = 'mob_b_' + rnd, P = 'testpass';

test.use(iphone);

test('mobile viewport: tabbar visible and contacts tab works', async ({ page }) => {
  await page.goto(BASE);
  await page.fill('#aUser', UA);
  await page.fill('#aPass', P);
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)');

  // B 注册
  await page.evaluate(async ([u, p]) => {
    const ws = new WebSocket((location.origin + '/ws').replace('http', 'ws'));
    await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
    ws.send(JSON.stringify({ type: 'register', user: u, pass: p, seq: 1 }));
    await new Promise(r => setTimeout(r, 400));
    ws.close();
  }, [UB, P]);
  await page.waitForTimeout(300);

  // 加好友 (弹层)
  await page.click('#addFriend');
  await page.waitForSelector('#friendModal:not(.hidden)');
  await page.fill('#friendSearch', UB);
  await page.click('#friendSearchBtn');
  await page.waitForTimeout(600);
  await page.locator('#friendResult .member-pick:not(.disabled)').first().click();
  await page.waitForTimeout(600);
  await page.click('#friendClose');
  await page.waitForTimeout(200);

  // 手机视口下 Tab 栏必须可见 (关键断言: 之前用户反馈手机上没有)
  const tab = page.locator('#tabContacts');
  await expect(tab).toBeVisible();
  const box = await tab.boundingBox();
  console.log('TAB-BOX', JSON.stringify(box));
  test.expect(box.height).toBeGreaterThan(0);

  // 切通讯录 -> 好友行存在
  await tab.click();
  await page.waitForTimeout(500);
  await expect(page.locator('#contactsTab:not(.hidden)')).toBeVisible();
  await expect(page.locator('#contactsTab .ct-row[data-u="' + UB + '"]')).toHaveCount(1);

  // 点好友 -> 开聊
  await page.locator('#contactsTab .ct-row[data-u="' + UB + '"]').click();
  await page.waitForTimeout(500);
  await expect(page.locator('#chathead:not(.hidden)')).toBeVisible();
});
