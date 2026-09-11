const { test, expect } = require('@playwright/test');
const crypto = require('crypto');
const BASE = 'http://127.0.0.1:8081/';

// 账号安全 E2E: 修改密码 / 邮箱绑定入口 / 忘记密码界面
test('资料卡-修改密码 全流程', async ({ page }) => {
  const u = 'sx_' + crypto.randomBytes(3).toString('hex');
  const p = 'orig' + crypto.randomBytes(3).toString('hex');
  await page.goto(BASE);
  await page.fill('#aUser', u);
  await page.fill('#aPass', p);
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 6000 });

  // 打开我的资料
  await page.click('#meInfo');
  await expect(page.locator('#myModal')).toBeVisible();

  // 修改密码: 错旧密码
  await page.fill('#myOldPass', 'wrongpass');
  await page.fill('#myNewPass', 'brandnew1');
  await page.click('#myPassBtn');
  await expect(page.locator('#myPassTip')).toContainText('不正确', { timeout: 5000 });

  // 正确旧密码 (等上一条请求彻底结束后再操作)
  await page.waitForTimeout(300);
  await page.fill('#myOldPass', p);
  await page.fill('#myNewPass', 'brandnew1');
  await page.click('#myPassBtn');
  await expect(page.locator('#myPassTip')).toContainText('成功', { timeout: 5000 });

  // 新密码可登录 (退出后重登)
  await page.click('#myClose');
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.waitForTimeout(1500);
  await page.fill('#aUser', u);
  await page.fill('#aPass', 'brandnew1');
  await page.click('#loginBtn');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 6000 });
  expect(await page.isVisible('#app')).toBe(true);
});

test('绑定邮箱入口 + 忘记密码弹窗可用', async ({ page }) => {
  const u = 'em_' + crypto.randomBytes(3).toString('hex');
  await page.goto(BASE);
  await page.fill('#aUser', u);
  await page.fill('#aPass', 'pass' + crypto.randomBytes(3).toString('hex'));
  await page.click('#regBtn');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 6000 });

  // 资料卡: 邮箱输入 + 获取验证码按钮存在
  await page.click('#meInfo');
  await expect(page.locator('#myEmail')).toBeVisible();
  await expect(page.locator('#myEmailBtn')).toBeVisible();
  // 非法邮箱应提示
  await page.fill('#myEmail', 'not-an-email');
  await page.click('#myEmailBtn');
  await expect(page.locator('#myEmailTip')).toContainText('格式', { timeout: 4000 });
  await page.click('#myClose');

  // 忘记密码弹窗
  await page.evaluate(() => localStorage.clear());
  await page.reload({ waitUntil: 'load' });
  await page.waitForTimeout(2000);   // 等 ws 建连就绪 (send 会排队, 但也给它时间)
  await page.click('#forgotLink');
  await expect(page.locator('#forgotModal')).toBeVisible();
  await page.fill('#fgIdent', 'someone');
  await page.click('#fgSendBtn');
  // 防枚举: 任何输入都回"验证码已发送/若绑定"
  await expect(page.locator('#fgTip')).toContainText('验证码', { timeout: 8000 });
  await expect(page.locator('#fgCodeRow')).toBeVisible();
});
