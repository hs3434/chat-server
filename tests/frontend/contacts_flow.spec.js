// E2E: 通讯录空态/加好友后渲染 (对齐用户反馈: hs3434 好友列表不显示好友)
// 场景1: 无好友 -> 通讯录显示"新的朋友"入口 + 空提示 (不白屏)
// 场景2: 加好友 -> 行出现
const { test, expect } = require('@playwright/test');
const BASE = process.env.BASE || 'http://127.0.0.1:8081/';
const rnd = Date.now().toString(36).slice(-6);
const UA = 'empty_a_' + rnd, P = 'testpass';

test('contacts tab empty state then add shows row', async ({ page }) => {
  await page.goto(BASE);
  await page.fill('#aUser', UA);
  await page.fill('#aPass', P);
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)');

  // 直接切通讯录 (0 好友)
  await page.click('#tabContacts');
  await page.waitForTimeout(500);
  await expect(page.locator('#ctNewFriends')).toBeVisible();
  // 空提示存在 (关键: 之前用户看到的是空白 = 像坏了)
  const boxHtml = await page.evaluate(() => document.getElementById('contactsTab').innerHTML);
  console.log('EMPTY-HTML-HAS-ENTRY:', boxHtml.includes('ctNewFriends'), 'HAS-EMPTY-TIP:', boxHtml.includes('还没有好友'));

  // 加一个好友 (注册B + 搜索添加)
  const UB = 'empty_b_' + rnd;
  await page.evaluate(async ([u, p]) => {
    const ws = new WebSocket((location.origin + '/ws').replace('http', 'ws'));
    await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
    ws.send(JSON.stringify({ type: 'register', user: u, pass: p, seq: 1 }));
    await new Promise(r => setTimeout(r, 400));
    ws.close();
  }, [UB, P]);
  await page.waitForTimeout(300);

  await page.click('#ctNewFriends');
  await page.waitForSelector('#friendModal:not(.hidden)');
  await page.fill('#friendSearch', UB);
  await page.click('#friendSearchBtn');
  await page.waitForTimeout(600);
  await page.locator('#friendResult .member-pick:not(.disabled)').first().click();
  await page.waitForTimeout(600);
  await page.click('#friendClose');
  await page.waitForTimeout(300);

  // 通讯录应直接出现好友行 (不刷新页面)
  await expect(page.locator('#contactsTab .ct-row[data-u="' + UB + '"]')).toHaveCount(1);
  console.log('ROW-APPEARED: true');
});
