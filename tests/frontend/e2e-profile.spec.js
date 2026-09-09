// Playwright E2E: 个人资料前端全流程 (真实浏览器; 服务端 profile 协议已由 test_profile.py 覆盖)
// 运行前提: wxlike-go 已在 127.0.0.1:8081 运行 (带 --web)
// 覆盖: 1)自我资料卡改昵称/签名+保存持久化 2)头像上传落盘 + /uploads 图片可访问(含相对/绝对)
const { test, expect } = require('@playwright/test');

const BASE = 'http://127.0.0.1:8081/';
const PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
  'base64'
);

async function regGoto(page) {
  const user = 'pf_' + Date.now().toString(36) + Math.floor(Math.random() * 1e4);
  await page.goto(BASE);
  await page.fill('#aUser', user);
  await page.fill('#aPass', 'pass123');
  await page.click('#regBtn');
  await expect(page.locator('#app')).toBeVisible({ timeout: 6000 });
  return user;
}

test('自我资料卡: 改昵称+签名可保存, token 恢复后仍保留(服务器落库)', async ({ context }) => {
  const page = await context.newPage();
  await regGoto(page);

  await page.locator('#meInfo').click();
  await expect(page.locator('#myModal')).toBeVisible();
  await page.locator('#myName').fill('老王');
  await page.locator('#mySig').fill('这是我的测试签名');
  await page.locator('#mySaveBtn').click();
  await expect(page.locator('#myTip')).toContainText('已保存', { timeout: 6000 });
  // 顶栏显示昵称
  await expect(page.locator('#meName')).toHaveText('老王', { timeout: 4000 });

  // 断线/刷新后 token_login 恢复, 昵称仍在 (login_ok 带 nickname)
  await page.reload();
  await expect(page.locator('#app')).toBeVisible({ timeout: 7000 });
  await expect(page.locator('#meName')).toHaveText('老王', { timeout: 6000 });
  // 会话保留不测(无关): 直接二次重载确认稳定
  await page.reload();
  await expect(page.locator('#meName')).toHaveText('老王', { timeout: 6000 });
});

test('自我资料卡: 上传头像 -> /uploads 图片服务 200 且 me 区头像就绪', async ({ context }) => {
  const page = await context.newPage();
  await regGoto(page);

  await page.locator('#meInfo').click();
  await expect(page.locator('#myModal')).toBeVisible();

  // 上传 (触发隐藏 file input 的 onAvatarChosen -> POST /upload?token)
  await page.locator('#myAvFile').setInputFiles({
    name: 'av.png', mimeType: 'image/png', buffer: PNG,
  });
  await expect(page.locator('#myTip')).toContainText('头像已上传', { timeout: 7000 });
  await page.locator('#mySaveBtn').click();
  await expect(page.locator('#myTip')).toContainText('已保存', { timeout: 6000 });

  // me 区头像应已是 <img> (有图而非首字母占位)
  const meHasImg = await page.locator('#meAvatar img').count();
  expect(meHasImg).toBe(1);

  // 拿 avatar URL, 直接 GET 应 200 (服务端已压缩落盘 web/uploads)
  const avUrl = await page.evaluate(() => {
    const im = document.querySelector('#meAvatar img');
    return im ? im.getAttribute('src') : null;
  });
  console.log('[avatar url]', avUrl);
  expect(avUrl).toBeTruthy();
  // 直接用页面 fetch 该 url (同源 /uploads/<file>) 断言服务端已压缩落盘可访问
  const status = await page.evaluate(async (u) => (await fetch(u)).status, avUrl);
  expect(status).toBe(200);

  // 刷新后 token 恢复: username/头像仍在 (login_ok 带 avatar)
  await page.reload();
  await expect(page.locator('#app')).toBeVisible({ timeout: 7000 });
  await expect(page.locator('#meAvatar img')).toHaveCount(1, { timeout: 6000 });
});
