// ESLint 配置: 浏览器环境原生 JS 前端 (wxlike)
export default {
  languageOptions: {
    ecmaVersion: 2022,
    sourceType: 'script',               // 非模块 (纯 script)
    globals: {
      // 浏览器全局 (app.js 内定义的 $/State 由其自身声明, 不在此列)
      window: 'readonly', document: 'readonly', location: 'readonly',
      console: 'readonly', alert: 'readonly', prompt: 'readonly', confirm: 'readonly',
      WebSocket: 'readonly', Event: 'readonly',
      setTimeout: 'readonly', clearTimeout: 'readonly', setInterval: 'readonly',
      JSON: 'readonly', Math: 'readonly', Date: 'readonly', Promise: 'readonly',
      Array: 'readonly', Object: 'readonly', String: 'readonly', Number: 'readonly',
      Boolean: 'readonly', RegExp: 'readonly', Error: 'readonly', Symbol: 'readonly',
      localStorage: 'readonly', sessionStorage: 'readonly',
      FormData: 'readonly', fetch: 'readonly', URL: 'readonly',
    },
  },
  rules: {
    // 语法/未定义变量 (最核心: 抓 typo 和引用断链)
    'no-undef': 'error',
    'no-unused-vars': ['warn', { vars: 'all', args: 'none' }],
    'no-redeclare': 'error',
    'no-constant-condition': 'error',
    'no-dupe-keys': 'error',
    'no-unreachable': 'error',
    // 常见坏味道
    'no-extra-semi': 'warn',
    'no-multiple-empty-lines': ['warn', { max: 2 }],
  },
};