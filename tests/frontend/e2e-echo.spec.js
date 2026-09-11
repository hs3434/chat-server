const { test, expect } = require('@playwright/test');
const crypto = require('crypto');
const BASE = 'http://127.0.0.1:8081/';

// 回归测试: 发送者本地回显 (线上 bug: 单聊发送后聊天框不显示, 重进才回显)
test('单聊发送后立即本地回显', async ({ page }) => {
  const u = 'echo_' + crypto.randomBytes(3).toString('hex');
  await page.goto(BASE);
  await page.fill('#aUser', u);
  await page.fill('#aPass', 'pass' + crypto.randomBytes(3).toString('hex'));
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 6000 });
  await page.waitForTimeout(800);

  // 进入与自己的单聊视图 (openChat 为 async, 调用后等它就绪)
  await page.evaluate((me) => { openChat(me); }, u);
  await page.waitForTimeout(1000);
  expect(await page.evaluate(() => State.view)).toBe(u);

  const body = '回显测试-' + crypto.randomBytes(2).toString('hex');
  await page.fill('#inp', body);
  await page.click('#sendbtn');
  // 关键断言: 不刷新不重进, 1s 内自己的消息应在本地消息数组里 (乐观回显)
  await page.waitForTimeout(1000);
  const last = await page.evaluate(() => {
    const arr = State.msgs[State.view] || [];
    return arr.length ? arr[arr.length - 1] : null;
  });
  expect(last).not.toBeNull();
  expect(last.from).toBe(u);
  expect(last.body).toBe(body);
});
