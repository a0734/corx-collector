/**
 * CORX IoT 采集 — 腾讯云 SCF 云函数版
 *
 * 原理：每次被 cron 触发 → 登录 CORX → 读云端配置 → 采集一轮写入 Supabase → 返回退出
 * 无状态、无循环、无 sleep，天然适合 Serverless
 *
 * 部署步骤见同目录 corx-scf-deploy-guide.md
 */

// ===== 配置从环境变量读取（在 SCF 控制台设置） =====
const CORX_TEL = process.env.CORX_TEL || ''
const CORX_PASSWORD = process.env.CORX_PASSWORD || ''
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://sdzmynpobsulpztavpqs.supabase.co'
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNkem15bnBvYnN1bHB6dGF2cHFzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc1NzgwOTYsImV4cCI6MjEwMzE1NDA5Nn0.uslI8V3RT-yH3vNF_UIBVyn8xUPn3qt-bxGFLo97UYI'

const CORX_BASE = 'https://api.corxnet.com'

// ===== 通用工具 =====

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19)
}

async function corxPost(path, body = {}) {
  const r = await fetch(CORX_BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`CORX ${path} HTTP ${r.status}`)
  return r.json()
}

async function sbGet(path) {
  const r = await fetch(SUPABASE_URL.replace(/\/+$/, '') + path, {
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
    },
  })
  if (!r.ok) throw new Error(`Supabase GET ${path} HTTP ${r.status}`)
  return r.json()
}

async function sbPost(path, body) {
  const r = await fetch(SUPABASE_URL.replace(/\/+$/, '') + path, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
    },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`Supabase POST ${path} HTTP ${r.status}`)
  return r.ok
}

// ===== CORX 登录 + 设备加载 =====

async function corxLogin() {
  const r = await corxPost('/api/v1/sign_in', { tel: CORX_TEL, password: CORX_PASSWORD })
  if (r.result !== 1 || !r.data?.token) {
    throw new Error('CORX 登录失败：' + JSON.stringify(r))
  }
  return { uid: r.data.uid, token: r.data.token }
}

async function corxLoadAll(auth) {
  const r = await corxPost('/api/v1/load', { uid: auth.uid, token: auth.token })
  if (r.result !== 1 || !Array.isArray(r.data)) {
    // token 过期，重登
    const reauth = await corxLogin()
    const r2 = await corxPost('/api/v1/load', { uid: reauth.uid, token: reauth.token })
    if (r2.result !== 1 || !Array.isArray(r2.data)) throw new Error('无法加载设备列表')
    return parseDevices(r2.data)
  }
  return parseDevices(r.data)
}

function parseDevices(groups) {
  const devices = []
  const hws = []
  for (const g of groups || []) {
    if (g.devices) {
      for (const d of g.devices) {
        devices.push(d)
        if (d.hws) {
          for (const h of d.hws) {
            hws.push({ ...h, devId: d.devId, mac: d.mac })
          }
        }
      }
    }
  }
  return { devices, hws }
}

function normMac(mac) {
  return (mac || '').replace(/^C0RX/, 'CORX')
}

async function corxHistory(mac, addr, start, end) {
  const r = await corxPost('/v1/history/get', { timestart: start, timeend: end, mac: normMac(mac), addr })
  return (r.result === 0 && Array.isArray(r.data)) ? r.data : []
}

// ===== 读云端配置 =====

async function fetchCloudConfig() {
  const rows = await sbGet('/rest/v1/report_config?id=eq.1&select=*')
  if (Array.isArray(rows) && rows.length > 0) {
    const row = rows[0]
    return {
      hwIds: Array.isArray(row.hw_ids) ? row.hw_ids.map(Number) : [],
      intervalMin: Number(row.interval_min) || 5,
      enabled: Boolean(row.enabled),
    }
  }
  return null
}

// ===== 单轮采集 =====

async function collectOnce() {
  // 0. 校验环境变量
  if (!CORX_TEL || !CORX_PASSWORD) {
    throw new Error('缺少环境变量 CORX_TEL / CORX_PASSWORD，请在 SCF 控制台配置')
  }

  // 1. 读云端配置
  const cloudCfg = await fetchCloudConfig()
  if (!cloudCfg) {
    return { ok: true, msg: '云端暂无配置，跳过（请在 Web 控制台配置并启动记录）', written: 0 }
  }
  if (!cloudCfg.enabled || cloudCfg.hwIds.length === 0) {
    return { ok: true, msg: '记录已停用或无点位，跳过', written: 0 }
  }

  // 2. 登录 + 加载设备
  const auth = await corxLogin()
  const { hws } = await corxLoadAll(auth)

  // 3. 采集每个点位最新值
  const nowSec = Math.floor(Date.now() / 1000)
  const lookback = Math.max(Math.floor(cloudCfg.intervalMin * 90), 900)
  const v = {}

  for (const hwId of cloudCfg.hwIds) {
    const hw = hws.find(x => x.id === hwId)
    if (!hw) {
      v[hwId] = null
      continue
    }
    try {
      const arr = await corxHistory(hw.mac, hw.addr, nowSec - lookback, nowSec)
      v[hwId] = arr.length > 0 ? arr[arr.length - 1][1] : null
    } catch (e) {
      v[hwId] = null
    }
  }

  // 4. 写入 Supabase
  const snapshot = { t: new Date().toISOString(), v }
  await sbPost('/rest/v1/report_records', [snapshot])

  const summary = Object.entries(v).map(([k, val]) => `${k}=${val}`).join(', ')
  return { ok: true, msg: `已写入：${summary}`, written: 1, pointCount: cloudCfg.hwIds.length }
}

// ===== SCF 入口 =====

exports.main_handler = async (event, context) => {
  console.log(`[${ts()}] SCF 触发`, JSON.stringify(event?.TriggerName || event).slice(0, 200))
  try {
    const result = await collectOnce()
    console.log(`[${ts()}] 采集完成：${result.msg}`)
    return result
  } catch (e) {
    console.error(`[${ts()}] 采集失败：`, e.message)
    return { ok: false, error: e.message }
  }
}

// ===== 独立运行入口（GitHub Actions / 本地 node index.js） =====

if (require.main === module) {
  collectOnce()
    .then(r => {
      console.log(`[${ts()}] 采集完成：${r.msg}`)
      process.exit(0)
    })
    .catch(e => {
      console.error(`[${ts()}] 采集失败：`, e.message)
      process.exit(1)
    })
}
