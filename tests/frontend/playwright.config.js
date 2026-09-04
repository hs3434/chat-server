// Playwright 配置: 仅 E2E workflow / 本地手动跑
// 注意: @playwright/test 是 devDependency, 用 npx playwright test 时自动发现
const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: '.',                    // tests/frontend (spec 文件在此)
  testMatch: '**/*.spec.js',
  timeout: 15000,
  retries: 1,                      // WS 偶发重连, 重试一次
  workers: 1,                      // 共享同一服务器, 串行
  use: {
    headless: true,
    viewport: { width: 390, height: 844 },  // 手机竖屏 (项目定位手机浏览器)
    actionTimeout: 8000,
  },
  reporter: [['list']],
});