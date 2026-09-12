// E2E: 通讯录 Tab 交互验证 (真实浏览器)
// 流程: A注册登录 -> B注册 -> A加B好友 -> 切到通讯录Tab -> 断言好友分组行 -> 点好友开聊
const { test, expect } = require('@playwright/test');

const BASE = process.env.BASE || 'http://127.0.0.1:8081/';
const rnd = Date.now().toString(36).slice(-6);
const UA = 'cta_a_' + rnd, UB = 'cta_b_' + rnd, P = 'testpass';

test('contacts tab renders WeChat-style directory and opens chat', async ({ page }) => {
  // A 注册即登录 (regBtn=doLogin(true))
  await page.goto(BASE);
  await page.fill('#aUser', UA);
  await page.fill('#aPass', P);
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)');

  // B 注册 (独立 WS 直调, 避开 UI)
  await page.evaluate(async ([u, p]) => {
    const ws = new WebSocket((location.origin + '/ws').replace('http', 'ws'));
    await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
    ws.send(JSON.stringify({ type: 'register', user: u, pass: p, seq: 1 }));
    await new Promise(r => setTimeout(r, 400));
    ws.close();
  }, [UB, P]);
  await page.waitForTimeout(300);

  // 用 UI 弹层加好友 (顺带验证弹层可用): 顶栏 +好友 -> 搜索 -> 添加
  await page.click('#addFriend');
  await page.waitForSelector('#friendModal:not(.hidden)');
  await page.fill('#friendSearch', UB);
  await page.click('#friendSearchBtn');
  await page.waitForTimeout(600);
  const addBtn = page.locator('#friendResult .member-pick:not(.disabled)').first();
  await addBtn.click();
  await page.waitForTimeout(600);
  // 关掉好友弹层 (加完不自动关, 手动关)
  await page.click('#friendClose');
  await page.waitForTimeout(200);

  // 切到通讯录 Tab
  await page.click('#tabContacts');
  await page.waitForTimeout(400);
  await expect(page.locator('#contactsTab:not(.hidden)')).toBeVisible();
  // 好友行存在且含 B (昵称回落用户名)
  const row = page.locator('#contactsTab .ct-row[data-u="' + UB + '"]');
  await expect(row).toHaveCount(1);
  await expect(row.locator('.ct-name')).toHaveText(UB);
  // 分组头存在
  await expect(page.locator('#contactsTab .ct-group').first()).toBeVisible();
  // "新的朋友"入口存在
  await expect(page.locator('#ctNewFriends')).toBeVisible();
  // tab 高亮
  await expect(page.locator('#tabContacts.active')).toBeVisible();

  // 点好友行 -> 开聊 (chathead 显示对方名)
  await row.click();
  await page.waitForTimeout(500);
  await expect(page.locator('#chathead:not(.hidden)')).toBeVisible();

  // 手机布局下先点返回键收起聊天页, 再切回消息 Tab
  await page.click('#chatback').catch(async () => {
    await page.evaluate(() => document.getElementById('chatbox').classList.remove('open'));
  });
  await page.evaluate(() => document.getElementById('chatbox').classList.remove('open'));
  await page.waitForTimeout(200);
  await page.click('#tabMsgs');
  await page.waitForTimeout(200);
  await expect(page.locator('#convs:not(.hidden)')).toBeVisible();
  await expect(page.locator('#contactsTab.hidden')).toHaveCount(1);
});
