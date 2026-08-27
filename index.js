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

async function sbPost(path, body, extraHeaders = {}) {
  const r = await fetch(SUPABASE_URL.replace(/\/+$/, '') + path, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      Prefer: 'return=minimal',
      ...extraHeaders,
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
  // 实际结构：每个分组含 deviceList（设备）+ hwList（点位，含 id/devId/mac/addr）
  const devices = []
  const hws = []
  for (const g of groups || []) {
    for (const d of g.deviceList || []) devices.push(d)
    for (const h of g.hwList || []) hws.push(h)
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

/**
 * 补录缺失时段：GitHub Actions cron 偶发漏触发（高峰期/仓库空闲/上轮未结束），
 * 每次触发需把「上次记录 → 当前网格」之间所有缺失的整点全部补上。
 * 数据从设备历史接口一次性拉取，按整点选最接近的实测值，避免整点漂移。
 * 单次最多补 24 个时段（约 1 天），避免 cron 长期停摆时一次性爆发。
 */
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

  // 1.5 网格对齐 + 补录范围
  const intervalMs = cloudCfg.intervalMin * 60 * 1000
  const S_now = Math.floor(Date.now() / intervalMs) * intervalMs
  const MAX_FILL_SLOTS = 24
  const MAX_FILL_MS = MAX_FILL_SLOTS * intervalMs

  // 取最近 200 条记录 → 已记录的网格集合（防重复写）
  const recent = await sbGet('/rest/v1/report_records?select=t&order=t.desc&limit=200')
  const recordedSlots = new Set()
  if (Array.isArray(recent)) {
    for (const r of recent) {
      const ts = Date.parse(r.t)
      if (!Number.isNaN(ts)) recordedSlots.add(Math.floor(ts / intervalMs) * intervalMs)
    }
  }

  // 补录范围：最近 24 格内的所有缺失网格（含中间空洞——之前写入失败/被删的记录会被自动重试）
  const rangeStart = S_now - MAX_FILL_MS

  // 2. 登录 + 加载设备
  const auth = await corxLogin()
  const { hws } = await corxLoadAll(auth)

  // 3. 按点位一次性拉取 [rangeStart-30min, now] 设备历史
  //    注意窗口必须足够宽：设备上报周期 ~3.5 分钟，若只查补录起点附近几分钟，
  //    经常一条数据都捞不到 → 整条记录全 null。前扩 30 分钟保证每个网格附近都有实测值
  const startSec = Math.max(0, Math.floor(rangeStart / 1000) - 1800)
  const endSec = Math.floor(Date.now() / 1000)
  const pointData = {}  // hwId -> [[ts_sec, val], ...]
  const pointMissing = [] // 缺失点（设备列表里找不到 hw）
  for (const hwId of cloudCfg.hwIds) {
    const hw = hws.find(x => x.id === hwId)
    if (!hw) {
      pointMissing.push(hwId)
      pointData[hwId] = []
      continue
    }
    try {
      const arr = await corxHistory(hw.mac, hw.addr, startSec, endSec)
      pointData[hwId] = Array.isArray(arr) ? arr : []
    } catch {
      pointData[hwId] = []
    }
  }

  // 4. 为每个缺失网格构造记录：从该点位的历史数据中选取最接近网格点的值
  //    容差 = 半个间隔 + 10 分钟：超过说明设备离线太久，宁可留待重试也不错配远处的值
  //    注意：历史接口的时间戳是 ISO 字符串（带 +08:00 时区），不是数字，需兼容解析
  const TOL_MS = intervalMs / 2 + 10 * 60 * 1000
  const records = []
  for (let s = rangeStart; s <= S_now; s += intervalMs) {
    if (recordedSlots.has(s)) continue  // 已记录跳过
    const v = {}
    for (const hwId of cloudCfg.hwIds) {
      const arr = pointData[hwId] || []
      let best = null
      let bestDelta = Infinity
      for (const [rawTs, val] of arr) {
        let ms
        if (typeof rawTs === 'number') ms = rawTs * 1000
        else if (typeof rawTs === 'string') {
          const p = Date.parse(rawTs)
          if (Number.isNaN(p)) continue
          ms = p
        } else continue
        const delta = Math.abs(ms - s)
        if (delta < bestDelta) { bestDelta = delta; best = val }
      }
      v[hwId] = (best !== null && bestDelta <= TOL_MS) ? best : null
    }
    // 全空记录不写入：留给下一轮重试（历史接口偶发失败 / 设备短暂离线均可自愈）
    if (Object.values(v).every(x => x === null)) continue
    records.push({ t: new Date(s).toISOString(), v })
  }

  if (records.length === 0) {
    return { ok: true, msg: `最近 ${MAX_FILL_SLOTS} 格均已记录或暂无数据，跳过`, written: 0 }
  }

  // 5. 批量写入（代码层已用 recordedSlots 去重，无需 on_conflict）
  await sbPost('/rest/v1/report_records', records)

  const skipped = pointMissing.length
  const summary = `补录 ${records.length} 个时段（${new Date(rangeStart).toISOString().slice(0, 16)} ~ ${new Date(S_now).toISOString().slice(0, 16)} UTC）`
  return { ok: true, msg: summary, written: records.length, pointCount: cloudCfg.hwIds.length, skipped }
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
