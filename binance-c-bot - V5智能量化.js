/**
 * INSTITUTIONAL-GRADE BINANCE BOT (V6 IRON DEFENSE - 铁壁保本版)
 * 
 * [V6 核心升级]:
 * 1. [状态锁死]: 保本防御一旦触发，永久锁死直到平仓，防止网络波动/重启导致重置。
 * 2. [交易所止损]: 直接向交易所发送 STOP_MARKET 触发单，不依赖内存状态。
 * 3. [四级移动止损]: 0.6%->+0.3%, 1.5%->+1%, 3%->+2%, 5%->追踪止盈(当前-2%)
 * 4. [半仓后强制保本]: 半仓止盈后，剩余仓位自动挂保本止损单。
 * 5. [BTC同向过滤]: 开单前检查BTC趋势，只允许与BTC方向一致的交易。
 * 6. [止损单防重复]: 缓存当前止损档位，避免频繁撤单挂单。
 * 7. [断线恢复]: 重启后自动检查并恢复交易所止损单。
 * 8. [BTC缓存]: BTC方向结果缓存60秒，减少API请求。
 */

const axios = require("axios");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const { ADX, RSI, ATR, BollingerBands, EMA, MFI } = require("technicalindicators"); 
require("dotenv").config();

const POS_DB = path.resolve(__dirname, "positions.json"); 
const HISTORY_DB = path. resolve(__dirname, "history.json");

const ENV_FORCE_REAL = process.env. FORCE_REAL === "true";
const ENV_VERBOSE = process.env. VERBOSE === "true";
const ENV_ALLOW_OLD_SIGNALS = process.env. ALLOW_OLD_SIGNALS === "true";
const ENV_INSTITUTIONAL_FILTER = process.env. INSTITUTIONAL_FILTER !== "false"; 

// ====== 配置 (实战参数) ======
const config = {
  apiKey: process.env.BINANCE_API_KEY || "",
  apiSecret: process.env. BINANCE_API_SECRET || "",
  useTestnet: process.env. BINANCE_TESTNET === "true" || false,
  
  aSignalsUrl: process.env.A_SIGNALS_URL || "http://43.156.96.140/api/a_signals",
  cSignalsUrl: process.env.C_SIGNALS_URL || "http://43. 156.96.140/api/c_signals",
  extraSignalsUrl: process.env.EXTRA_SIGNALS_URL || "http://43.156.96. 140:8082/api/signals",
  
  fetchIntervalMs: Number(process.env. FETCH_INTERVAL_MS) || 60 * 1000,
  checkCloseIntervalMs: 15 * 1000,

  dryRun: (process.env.DRY_RUN === "true") && !ENV_FORCE_REAL,
  forceReal: ENV_FORCE_REAL,
  verbose: ENV_VERBOSE,
  allowOldSignals: ENV_ALLOW_OLD_SIGNALS,
  institutionalFilter: ENV_INSTITUTIONAL_FILTER, 

  usdtPerTrade: Number(process. env.USDT_PER_TRADE) || 10,
  usdtPerTradeIsMargin: (process.env. USDT_PER_TRADE_IS_MARGIN === "true") || false,
  defaultLeverage: Number(process.env.DEFAULT_LEVERAGE) || 10,
  maxRetries: Number(process.env.MAX_RETRIES) || 3,
  
  stopLossPercent: 2.5, 
  maxHoldTimeMinutes: 60, 

  logFile: path.resolve(__dirname, process.env.LOG_FILE || "bot.log"),
  
  maxSignalAgeMinutes: 15, 
  reopenCooldownMinutes: Number(process.env. REOPEN_COOLDOWN_MINUTES || 0),

  limitOrderTimeoutSeconds: 5,
  
  maxDailyDrawdownPct: 20. 0,

  // [V6新增] BTC同向过滤配置
  btcFilter: {
    enabled: process.env.BTC_FILTER_ENABLED !== "false",
    symbol: "BTCUSDT",
    emaPeriod: 20,
    interval: "15m",
    cacheTtlMs: 60 * 1000,
    useMultiTimeframe: true,
  },

  // [V6新增] 移动止损档位配置
  trailingStop: {
    enabled: true,
    tiers: [
      { triggerPct: 0.6, stopPct: 0.3 },
      { triggerPct: 1.5, stopPct: 1. 0 },
      { triggerPct: 3.0, stopPct: 2.0 },
      { triggerPct: 5.0, stopPct: null, trailing: 2.0 },
    ],
    tierBuffer: 0.1,
  },

  volatilitySizing: {
    enabled: process.env.VOLATILITY_SIZING_ENABLED !== "false", 
    targetVolatility: Number(process.env.TARGET_VOLATILITY) || 2.0,
    minScale: 0.5,
    maxScale: 2.0,
    maxUsdtCap: Number(process.env. MAX_USDT_CAP) || 500
  },

  adaptive: {
    enabled: process.env. ADAPTIVE_EXIT_ENABLED !== "false",
    choppyAdxThreshold: 30, 
    choppyTakeProfit: 1.2, 
    rsiOverbought: 75, 
    rsiOversold: 25,   
    zombieHours: Number(process.env. ZOMBIE_HOURS) || 4,    
    zombieProfit: Number(process.env. ZOMBIE_PROFIT_THRESHOLD) || 0. 5  
  },

  profitMonitor: {
    enterPct: 4, 
    tierUpgradePct: 7,
    tierA_closeToPct: 2. 5,
    tierB_drawdownPct: 40,
    quickProtectEnterPct: 1.2, 
    quickProtectCloseToPct: 0.3 
  },

  partialCloseEnabled: true, 
  partialClosePct: 1.5, 

  reqMinIntervalMs: Number(process.env. REQ_MIN_INTERVAL_MS || 120),
  reqPerMinuteLimit: Number(process. env.REQ_PER_MINUTE_LIMIT || 110),

  marketStreamType: (process.env. MARKET_STREAM_TYPE || "trade").toLowerCase(),
  signalBlacklist: (process.env. SIGNAL_BLACKLIST || "").split(/\s*,\s*/). filter(Boolean),

  maxSignalsPerLoop: Number(process. env.MAX_SIGNALS_PER_LOOP || 50), 
  wsStaleTimeoutMs: Number(process.env. WS_STALE_TIMEOUT_MS || 300 * 1000), 
  rateStatsIntervalMs: Number(process.env. RATE_STATS_INTERVAL_MS || 30 * 1000),

  atrStopMultiplier: 2.0, 
  chandelierExitMultiplier: 1.8 
};

// ====== 全局变量 ======
let userWs = null;
let userListenKey = null;
let userWsStarting = false;
let userReconnectAttempts = 0;
let listenKeyKeepAliveInterval = null;
let lastUserWsMsgAt = 0;
let lastMarketWsMsgAt = 0;

let initialAccountBalance = 0;
let isGlobalStop = false;
let lastUserWsHeartbeat = 0;

// [V6新增] BTC方向缓存
let btcDirectionCache = {
  direction: null,
  timestamp: 0,
  details: {}
};

// [V6新增] 止损单缓存
const stopOrderCache = {};

const MAX_SIGNAL_AGE_MS = config.maxSignalAgeMinutes * 60 * 1000;
const REOPEN_COOLDOWN_MS = config.reopenCooldownMinutes * 60 * 1000;

const API_BASE = config.useTestnet
  ? (process.env.TESTNET_API_BASE || "https://testnet.binancefuture.com")
  : "https://fapi.binance.com";
const USERDATA_WS_HOST = config.useTestnet
  ? "wss://stream.binancefuture.com" 
  : "wss://fstream.binance.com";

function log(... args) {
  const line = `[${new Date().toISOString()}] ${args.map(a => typeof a === "string" ?  a : JSON.stringify(a)).join(" ")}`;
  console.log(line);
  try { fs.appendFileSync(config.logFile, line + "\n"); } catch (e) {}
}

function parseSignalTimeToMs(t) {
  if (t === undefined || t === null || t === "") return NaN;
  if (typeof t === "number") {
    if (t > 1e12) return t;
    if (t > 1e9) return t * 1000;
    return NaN;
  }
  const s = String(t). trim();
  if (/^\d+$/.test(s)) {
    if (s.length >= 13) return Number(s);
    if (s.length === 10) return Number(s) * 1000;
  }
  let ms = Date.parse(s);
  if (! isNaN(ms)) return ms;
  ms = Date.parse(s. replace(/-/g, "/"));
  if (! isNaN(ms)) return ms;
  return NaN;
}

function loadPositions() {
  try {
    if (! fs.existsSync(POS_DB)) return {};
    return JSON.parse(fs.readFileSync(POS_DB, "utf8") || "{}");
  } catch (e) {
    log("loadPositions error:", e.message || e);
    try {
      const bad = POS_DB + ".corrupt." + Date.now();
      fs.renameSync(POS_DB, bad);
      log("Moved corrupt positions. json to", bad);
    } catch (ie) {}
    return {};
  }
}

function savePositions(obj) {
  try {
    const data = JSON.stringify(obj, null, 2);
    const dir = path.dirname(POS_DB);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(POS_DB, data, { encoding: 'utf8', flag: 'w' });
  } catch (e) {
    log("savePositions fatal error:", e.message || e);
  }
}

function appendToHistory(positionData) {
  try {
    if (!positionData.qty && !positionData. amount) {
        return; 
    }
    let history = [];
    if (fs.existsSync(HISTORY_DB)) {
      try {
        const raw = fs.readFileSync(HISTORY_DB, 'utf8');
        history = JSON.parse(raw) || [];
        if (! Array.isArray(history)) history = [];
      } catch(e) { history = []; }
    }
    const uniqueId = positionData.id || `${positionData. symbol}_${positionData. openedAt}_${positionData. closedAt || Date.now()}`;
    const existingIndex = history. findIndex(h => {
       const hId = h.id || `${h. symbol}_${h. openedAt}_${h.closedAt}`;
       return hId === uniqueId;
    });
    const recordToSave = { ... positionData, id: uniqueId };
    if (existingIndex >= 0) {
      history[existingIndex] = recordToSave;
    } else {
      history.push(recordToSave);
      log(`[HISTORY] Archived new record for ${positionData.symbol} PnL: ${positionData. realizedProfit}`);
    }
    if (history.length > 2000) history = history.slice(-2000);
    fs.writeFileSync(HISTORY_DB, JSON.stringify(history, null, 2));
  } catch (e) {
    log("[HISTORY] Failed to save:", e.message);
  }
}

function sanitizeSymbol(raw) {
  if (! raw) return "";
  return String(raw).toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function signQuery(paramsStr) {
  return crypto.createHmac("sha256", config.apiSecret).update(paramsStr). digest("hex");
}

function nowMs(){ return Date.now(); }

const SIGNAL_BLACKLIST_SET = new Set((config.signalBlacklist || []).map(s => sanitizeSymbol(s)). filter(Boolean));
function isBlacklisted(symbol) {
  if (!symbol) return false;
  return SIGNAL_BLACKLIST_SET.has(sanitizeSymbol(symbol));
}

const axiosInstance = axios. create({
  timeout: 10000,
  httpAgent: new http.Agent({ keepAlive: true }),
  httpsAgent: new https.Agent({ keepAlive: true })
});

const requestQueue = [];
let processingQueue = false;
const recentSignedRequests = [];

async function enqueueSignedRequest(fn) {
  return new Promise((resolve, reject) => {
    requestQueue.push({ fn, resolve, reject });
    if (!processingQueue) processQueue();
  });
}

async function processQueue() {
  processingQueue = true;
  while (requestQueue.length) {
    const now = Date.now();
    while (recentSignedRequests.length && (now - recentSignedRequests[0]) > 60000) recentSignedRequests.shift();
    if (recentSignedRequests.length >= config.reqPerMinuteLimit) {
      const waitMs = 60000 - (now - recentSignedRequests[0]) + 20;
      if (config.verbose) log("RateLimiter: reached per-minute cap, sleeping", waitMs, "ms");
      await new Promise(r => setTimeout(r, waitMs));
      continue;
    }
    const job = requestQueue.shift();
    try {
      const start = Date.now();
      const res = await job.fn();
      recentSignedRequests.push(Date.now());
      const took = Date.now() - start;
      const waitAfter = Math.max(0, config.reqMinIntervalMs - took);
      if (waitAfter > 0) await new Promise(r => setTimeout(r, waitAfter));
      job.resolve(res);
    } catch (e) {
      const status = e?. response?.status;
      if (status === 429 || status === 418) {
        const backoffMs = 1000 + Math.floor(Math. random() * 500);
        log("RateLimiter: got 429/418, backing off", backoffMs, "ms");
        await new Promise(r => setTimeout(r, backoffMs));
        requestQueue.push(job);
      } else {
        job.reject(e);
      }
    }
  }
  processingQueue = false;
}

setInterval(() => {
  try {
    log("RateStats queueLen=", requestQueue.length, "recentSignedRequestsLastMinute=", recentSignedRequests. length);
  } catch (e) { }
}, config.rateStatsIntervalMs);

function extractExecutedPriceFromOrder(order) {
  if (!order) return null;
  if (order.avgPrice && Number(order.avgPrice)) return Number(order.avgPrice);
  if (order.avg_price && Number(order.avg_price)) return Number(order.avg_price);
  const fills = order.fills || order.fill || order.fillsList || [];
  if (Array.isArray(fills) && fills.length) {
    let totalQty = 0;
    let weighted = 0;
    for (const f of fills) {
      const fq = Number(f. qty || f.executedQty || f.amount || 0);
      const fp = Number(f. price || f.avgPrice || f.execPrice || 0);
      if (fq > 0 && fp > 0) {
        totalQty += fq;
        weighted += fq * fp;
      }
    }
    if (totalQty > 0) return weighted / totalQty;
  }
  if (order.price && Number(order.price) && Number(order.price) !== 0) return Number(order.price);
  if (order. executedPrice && Number(order. executedPrice)) return Number(order. executedPrice);
  return null;
}

// ====== [V6] 软删除 (Soft Close) ======
function performSoftClose(symbol, side) {
    const pos = loadPositions();
    if (!pos[symbol]) pos[symbol] = { symbol };
    
    pos[symbol].open = false;
    pos[symbol].lastClosedAt = Date.now();
    pos[symbol].lastClosedSide = side;
    pos[symbol].ironDefenseActive = false;
    pos[symbol].currentStopTier = null;
    
    if(pos[symbol].lastOrder) delete pos[symbol].lastOrder;
    
    if (stopOrderCache[symbol]) {
        delete stopOrderCache[symbol];
    }
    
    savePositions(pos);
}

// ====== [V6] 延迟 GC ======
function cleanOldClosedPositions() {
    const pos = loadPositions();
    const now = Date.now();
    const cooldownMs = (config.reopenCooldownMinutes || 60) * 60 * 1000;
    let changed = false;

    for (const key of Object.keys(pos)) {
        const p = pos[key];
        if (p && p.open === false && p.lastClosedAt) {
            if (now - p. lastClosedAt > cooldownMs) {
                delete pos[key];
                changed = true;
            }
        }
    }
    if (changed) savePositions(pos);
}
// ====== 继续第二部分 ======

async function computeAndPersistClose(symbol, orderResult, prevSide, qtyUsed) {
  try {
    const executedPriceFromOrder = extractExecutedPriceFromOrder(orderResult);
    let closePrice = executedPriceFromOrder;
    if ((! closePrice || ! isFinite(closePrice)) && typeof getMarkPrice === 'function') {
      try {
        const m = await getMarkPrice(symbol);
        if (m && isFinite(m)) closePrice = m;
      } catch (e) {}
    }

    const posData = loadPositions();
    posData[symbol] = posData[symbol] || {};
    const p = posData[symbol];

    let entry = null;
    if (p.entryPrice && isFinite(Number(p.entryPrice)) && Number(p. entryPrice) !== 0) entry = Number(p.entryPrice);
    else if (p.initial_price && isFinite(Number(p.initial_price)) && Number(p.initial_price) !== 0) entry = Number(p.initial_price);
    else if (p.fallbackEntryPrice && isFinite(Number(p.fallbackEntryPrice)) && Number(p.fallbackEntryPrice) !== 0) entry = Number(p.fallbackEntryPrice);
    else if (p.lastOrder) {
      const ex = extractExecutedPriceFromOrder(p.lastOrder);
      if (ex && isFinite(ex)) entry = ex;
    }

    const qty = qtyUsed || Number(p.amount || p.qty || p.positionAmt || 0) || 0;

    let realized = null;
    let realizedPct = null;
    const usedSide = (prevSide || p.side || ""). toString().toUpperCase();

    if (entry && closePrice && qty) {
      if (usedSide === "LONG" || usedSide === "BUY") {
        realized = (closePrice - entry) * qty;
      } else {
        realized = (entry - closePrice) * qty;
      }
      const cost = Math.abs(entry * qty) || 0;
      realizedPct = cost > 0 ? Number(((realized / cost) * 100).toFixed(4)) : null;
      realized = Number(realized.toFixed(6));
    }

    const record = {
        ... p,
        symbol: symbol,
        qty: qtyUsed,
        amount: qtyUsed,
        closedAt: Date.now(),
        closePrice: closePrice,
        realizedProfit: realized,
        realizedPct: realizedPct,
        lastOrder: orderResult
    };
    appendToHistory(record);

    await cancelStopOrder(symbol);

    log(`[CLOSE] Position closed for ${symbol}.  PnL: ${realized ?  realized.toFixed(2) : 0}`);
    performSoftClose(symbol, prevSide);

    return { closePrice, realized, realizedPct };
  } catch (e) {
    log("computeAndPersistClose error for", symbol, e?. message || e);
    return null;
  }
}

async function handlePartialClose(symbol, closeQty, prevSide, orderResult) {
  try {
    const executedPrice = extractExecutedPriceFromOrder(orderResult);
    let closePrice = executedPrice;
    if ((!closePrice || !isFinite(closePrice)) && typeof getMarkPrice === 'function') {
      try {
        const m = await getMarkPrice(symbol);
        if (m && isFinite(m)) closePrice = m;
      } catch (e) {}
    }

    const posData = loadPositions();
    posData[symbol] = posData[symbol] || {};
    const p = posData[symbol];

    let entry = null;
    if (p.entryPrice && isFinite(Number(p.entryPrice)) && Number(p.entryPrice) !== 0) entry = Number(p.entryPrice);
    else if (p.lastOrder) {
      const ex = extractExecutedPriceFromOrder(p.lastOrder);
      if (ex && isFinite(ex)) entry = ex;
    }

    let realized = null;
    let realizedPct = null;
    const usedSide = (prevSide || p.side || "").toString().toUpperCase();

    if (entry && closePrice && closeQty) {
      if (usedSide === "LONG" || usedSide === "BUY") {
        realized = (closePrice - entry) * closeQty;
      } else {
        realized = (entry - closePrice) * closeQty;
      }
      const cost = Math.abs(entry * closeQty) || 0;
      realizedPct = cost > 0 ? Number(((realized / cost) * 100).toFixed(4)) : null;
      realized = Number(realized.toFixed(6));
    }

    const prevAmt = Number(p.amount || p. qty || 0);
    let remaining = prevAmt - Number(closeQty || 0);
    if (remaining < 1e-7) remaining = 0; 

    p.amount = remaining;
    p.lastOrder = orderResult || p.lastOrder || null;
    p. realizedProfit = (p.realizedProfit || 0) + (realized || 0);
    p.realized_profit = p.realizedProfit;
    p.realized_usdt = p.realizedProfit;

    if (p.amount > 0) {
      p.open = true;
      p.lastPartialClosedAt = Date.now();
      p.lastPartialClosedQty = Number(closeQty || 0);
      p.partialClosed = true;
      
      // [V6 核心] 半仓后立即激活铁壁保本
      p.ironDefenseActive = true;
      p.ironDefenseTriggeredAt = Date.now();
      log(`[IRON DEFENSE] ${symbol} - Partial close triggered. Iron Defense ACTIVATED.`);
      
      // [V6] 立即挂保本止损单
      if (entry) {
        const stopPrice = usedSide === "LONG" 
          ? entry * (1 + 0.003)
          : entry * (1 - 0.003);
        await placeOrUpdateStopOrder(symbol, usedSide, remaining, stopPrice, 0);
      }
      
      savePositions(posData);
    } else {
      const record = { ...p, symbol, closedAt: Date. now(), realizedProfit: realized };
      appendToHistory(record);
      await cancelStopOrder(symbol);
      log(`[CLOSE] Partial close resulted in full close for ${symbol}. `);
      performSoftClose(symbol, prevSide);
      return { closePrice, realized, realizedPct, remaining };
    }

    const historyRecord = {
        ...p,
        symbol: symbol,
        qty: closeQty,
        amount: closeQty,
        closedAt: Date.now(),
        realizedProfit: realized,
        realizedPct: realizedPct,
        type: "PARTIAL"
    };
    appendToHistory(historyRecord);

    return { closePrice, realized, realizedPct, remaining };
  } catch (e) {
    log("handlePartialClose error for", symbol, e?. message || e);
    return null;
  }
}

async function sendSignedRequest(method, pathUrl, params = {}) {
  if (config. dryRun) {
    if (params.type === 'LIMIT' || params.type === 'MARKET' || params.type === 'STOP_MARKET') {
       log("[DRYRUN] Order Request:", method, params);
       return { orderId: "sim_" + Date.now(), status: "FILLED", avgPrice: params.price || params.stopPrice || 100, executedQty: params.quantity };
    }
    log("[DRYRUN] Signed request (skipped):", method, pathUrl, params);
    return null;
  }

  if (config.verbose) log("[VERBOSE] Enqueuing signed request:", method, pathUrl, "params:", params);

  return enqueueSignedRequest(async () => {
    for (let i = 0; i < config.maxRetries; i++) {
      try {
        const timestamp = Date.now();
        const qsObj = { ... params, timestamp };
        const qs = new URLSearchParams(qsObj).toString();
        const signature = signQuery(qs);
        const fullQs = qs + `&signature=${signature}`;
        const url = `${API_BASE}${pathUrl}? ${fullQs}`;
        const headers = { "X-MBX-APIKEY": config.apiKey };

        const res = await axiosInstance({ method, url, headers, timeout: 10000 });
        if (config.verbose) log("[VERBOSE] response status:", res.status, "data:", res.data && (typeof res.data === "object" ? JSON.stringify(res.data) : res.data));
        return res. data;
      } catch (e) {
        const status = e?.response?. status;
        const data = e?.response?. data;
        log("sendSignedRequest error:", method, pathUrl, "attempt", i + 1, "status:", status, "data:", data || e. message);
        if (status === 429 || status === 418) {
          const backoffMs = 1000 * (i + 1) + Math.floor(Math.random() * 400);
          await new Promise(r => setTimeout(r, backoffMs));
          continue;
        }
        if (data && data.code === -1021) {
           throw e; 
        }
        await new Promise(r => setTimeout(r, 500 * (i + 1)));
      }
    }
    throw new Error("sendSignedRequest failed after retries");
  });
}

async function sendPublicRequest(pathUrl, params = {}) {
  const url = `${API_BASE}${pathUrl}${Object.keys(params). length ?  ("?" + new URLSearchParams(params).toString()) : ""}`;
  if (config.dryRun && ! url.includes("bookTicker") && !url.includes("klines") && !url.includes("premiumIndex")) {
    log("[DRYRUN] Public request (will still fetch):", url);
  }
  const res = await axiosInstance. get(url, { timeout: 10000 });
  if (config.verbose) log("[VERBOSE] public response:", pathUrl, res.status);
  return res. data;
}

async function getBookTicker(symbol) {
    try {
        const d = await sendPublicRequest("/fapi/v1/ticker/bookTicker", { symbol });
        return { bid: Number(d. bidPrice), ask: Number(d.askPrice) };
    } catch(e) { return null; }
}

async function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

// ====== [V6 核心] BTC 同向过滤器 ======
async function getBTCDirection() {
  if (!config.btcFilter. enabled) {
    return { direction: null, pass: true, reason: "BTC Filter Disabled" };
  }

  const now = Date.now();
  if (btcDirectionCache. direction && (now - btcDirectionCache.timestamp) < config.btcFilter.cacheTtlMs) {
    return { 
      direction: btcDirectionCache. direction, 
      pass: true, 
      reason: "Cached",
      details: btcDirectionCache. details 
    };
  }

  try {
    const symbol = config.btcFilter. symbol;
    
    const klines15m = await sendPublicRequest("/fapi/v1/klines", { 
      symbol, 
      interval: "15m", 
      limit: 50 
    });
    
    if (! klines15m || klines15m.length < 25) {
      return { direction: null, pass: true, reason: "BTC Data Error" };
    }

    const closes15m = klines15m.map(k => Number(k[4]));
    const currentPrice = closes15m[closes15m.length - 1];
    
    const ema20_15m = EMA. calculate({ period: 20, values: closes15m });
    const currentEma15m = ema20_15m[ema20_15m.length - 1];
    
    let direction15m = currentPrice > currentEma15m ? "LONG" : "SHORT";
    let finalDirection = direction15m;
    let details = { 
      price: currentPrice, 
      ema20_15m: currentEma15m,
      direction15m 
    };

    if (config.btcFilter. useMultiTimeframe) {
      try {
        const klines1h = await sendPublicRequest("/fapi/v1/klines", { 
          symbol, 
          interval: "1h", 
          limit: 30 
        });
        
        if (klines1h && klines1h. length >= 25) {
          const closes1h = klines1h.map(k => Number(k[4]));
          const ema20_1h = EMA.calculate({ period: 20, values: closes1h });
          const currentEma1h = ema20_1h[ema20_1h.length - 1];
          const direction1h = currentPrice > currentEma1h ? "LONG" : "SHORT";
          
          details. ema20_1h = currentEma1h;
          details.direction1h = direction1h;
          
          if (direction15m === direction1h) {
            finalDirection = direction15m;
            details.confirmed = true;
          } else {
            finalDirection = direction15m;
            details.confirmed = false;
            details.conflict = true;
          }
        }
      } catch (e) {
        log("[BTC FILTER] 1h data fetch failed, using 15m only");
      }
    }

    btcDirectionCache = {
      direction: finalDirection,
      timestamp: now,
      details
    };

    log(`[BTC FILTER] Direction: ${finalDirection} | Price: ${currentPrice. toFixed(2)} | EMA20(15m): ${currentEma15m. toFixed(2)}`);
    
    return { direction: finalDirection, pass: true, reason: "OK", details };
    
  } catch (e) {
    log("[BTC FILTER] Error:", e.message);
    return { direction: null, pass: true, reason: "Error" };
  }
}

async function checkBTCAlignment(intendedSide) {
  if (! config.btcFilter.enabled) {
    return { aligned: true, reason: "Filter Disabled" };
  }

  const btc = await getBTCDirection();
  
  if (!btc.direction) {
    log(`[BTC FILTER] Cannot determine BTC direction, allowing trade with caution`);
    return { aligned: true, reason: "Unknown BTC Direction", caution: true };
  }

  const aligned = btc.direction === intendedSide;
  
  if (! aligned) {
    log(`[BTC FILTER BLOCKED] Trade ${intendedSide} rejected.  BTC trending ${btc.direction}`);
  }
  
  return { 
    aligned, 
    btcDirection: btc.direction,
    reason: aligned ? "Aligned" : `BTC is ${btc.direction}`,
    details: btc.details
  };
}

// ====== [V6 核心] 交易所止损单管理 ======
let exchangeInfoCache = null;

async function getExchangeInfo() {
  if (exchangeInfoCache) return exchangeInfoCache;
  try {
    const data = await sendPublicRequest("/fapi/v1/exchangeInfo");
    exchangeInfoCache = data;
    return data;
  } catch (e) {
    log("getExchangeInfo error:", e?. response?.data || e. message);
    return null;
  }
}

function getPricePrecision(symbol) {
  if (!exchangeInfoCache) return 2;
  const s = (exchangeInfoCache. symbols || []).find(v => v.symbol === symbol);
  if (!s) return 2;
  const filter = s.filters. find(f => f.filterType === "PRICE_FILTER");
  if (! filter) return 2;
  const tickSize = Number(filter.tickSize);
  return Math.max(0, Math.round(Math.log10(1 / tickSize)));
}

function roundPrice(symbol, price) {
  const precision = getPricePrecision(symbol);
  const factor = Math.pow(10, precision);
  return Math.round(price * factor) / factor;
}

function lotRound(symbol, qty) {
  if (!exchangeInfoCache) return Number(qty. toFixed(6));
  const s = (exchangeInfoCache.symbols || []).find(v => v.symbol === symbol);
  if (! s) return Number(qty.toFixed(6));
  const filter = s.filters.find(f => f.filterType === "LOT_SIZE");
  if (! filter) return Number(qty.toFixed(6));
  const step = Number(filter. stepSize);
  const precision = Math.max(0, Math. round(Math.log10(1 / step)));
  const factor = Math.pow(10, precision);
  return Math. floor(qty * factor) / factor;
}

async function cancelStopOrder(symbol) {
  const cached = stopOrderCache[symbol];
  if (! cached || !cached.orderId) return;
  
  try {
    await sendSignedRequest("DELETE", "/fapi/v1/order", { 
      symbol, 
      orderId: cached.orderId 
    });
    log(`[STOP ORDER] Cancelled stop order for ${symbol}, orderId: ${cached.orderId}`);
    delete stopOrderCache[symbol];
  } catch (e) {
    if (e?. response?.data?.code === -2011) {
      delete stopOrderCache[symbol];
    } else {
      log(`[STOP ORDER] Cancel failed for ${symbol}:`, e.message);
    }
  }
}

async function cancelAllStopOrders(symbol) {
  try {
    const openOrders = await sendSignedRequest("GET", "/fapi/v1/openOrders", { symbol });
    if (! openOrders || ! openOrders.length) return;
    
    for (const order of openOrders) {
      if (order.type === "STOP_MARKET" || order.type === "STOP") {
        try {
          await sendSignedRequest("DELETE", "/fapi/v1/order", { 
            symbol, 
            orderId: order.orderId 
          });
          log(`[STOP ORDER] Cancelled existing stop order ${order.orderId} for ${symbol}`);
        } catch (e) {}
      }
    }
  } catch (e) {
    log(`[STOP ORDER] cancelAllStopOrders error for ${symbol}:`, e.message);
  }
}

async function placeOrUpdateStopOrder(symbol, side, qty, stopPrice, tierIndex) {
  const cached = stopOrderCache[symbol];
  
  if (cached) {
    if (cached.tier === tierIndex) {
      const priceDiff = Math.abs(cached.stopPrice - stopPrice) / cached.stopPrice;
      if (priceDiff < 0.001) {
        return;
      }
    }
    await cancelStopOrder(symbol);
  }
  
  const closeSide = side === "LONG" ? "SELL" : "BUY";
  const roundedStopPrice = roundPrice(symbol, stopPrice);
  const roundedQty = lotRound(symbol, qty);
  
  if (roundedQty <= 0) return;
  
  try {
    const params = {
      symbol,
      side: closeSide,
      type: "STOP_MARKET",
      stopPrice: roundedStopPrice. toString(),
      quantity: roundedQty. toString(),
      reduceOnly: "true",
      workingType: "MARK_PRICE"
    };
    
    const result = await sendSignedRequest("POST", "/fapi/v1/order", params);
    
    if (result && result.orderId) {
      stopOrderCache[symbol] = {
        orderId: result.orderId,
        tier: tierIndex,
        stopPrice: roundedStopPrice,
        placedAt: Date. now()
      };
      
      log(`[STOP ORDER] Placed STOP_MARKET for ${symbol} @ ${roundedStopPrice} (Tier ${tierIndex})`);
    }
    
    return result;
  } catch (e) {
    log(`[STOP ORDER] Failed to place stop order for ${symbol}:`, e.message);
    return null;
  }
}

function calculateStopTier(profitPercent, currentTier) {
  const tiers = config.trailingStop.tiers;
  const buffer = config.trailingStop.tierBuffer;
  
  let newTier = -1;
  
  for (let i = tiers.length - 1; i >= 0; i--) {
    const tier = tiers[i];
    const triggerWithBuffer = (currentTier >= i) 
      ? tier.triggerPct - buffer
      : tier.triggerPct;
    
    if (profitPercent >= triggerWithBuffer) {
      newTier = i;
      break;
    }
  }
  
  // [V6 铁壁原则] 只能升级不能降级
  if (currentTier !== null && currentTier !== undefined && currentTier >= 0 && newTier < currentTier) {
    return currentTier;
  }
  
  return newTier;
}

function calculateStopPrice(entry, side, profitPercent, tierIndex) {
  const tiers = config.trailingStop.tiers;
  
  if (tierIndex < 0 || tierIndex >= tiers.length) return null;
  
  const tier = tiers[tierIndex];
  let stopPct;
  
  if (tier.trailing) {
    stopPct = profitPercent - tier.trailing;
  } else {
    stopPct = tier.stopPct;
  }
  
  if (side === "LONG") {
    return entry * (1 + stopPct / 100);
  } else {
    return entry * (1 - stopPct / 100);
  }
}
// ====== 继续第三部分 ======

// ====== Gatekeeper 5.0 ======
class InstitutionalGatekeeper {
  constructor() {}

  async getMarketContext(symbol) {
    try {
      const [klinesRaw, premiumIndex] = await Promise. all([
        sendPublicRequest("/fapi/v1/klines", { symbol, interval: "15m", limit: 200 }), 
        sendPublicRequest("/fapi/v1/premiumIndex", { symbol })
      ]);

      if (!klinesRaw || klinesRaw.length < 150) return null;

      const closes = klinesRaw.map(k => Number(k[4]));
      const highs = klinesRaw.map(k => Number(k[2]));
      const lows = klinesRaw.map(k => Number(k[3]));
      const volumes = klinesRaw.map(k => Number(k[5]));
      
      return { 
        closes, highs, lows, volumes,
        fundingRate: Number(premiumIndex. lastFundingRate || 0),
        currentPrice: closes[closes.length - 1]
      };
    } catch (e) {
      log(`[Gatekeeper] Data fetch failed for ${symbol}: ${e. message}`);
      return null;
    }
  }

  async evaluate(symbol, intendedSide) {
    if (!config.institutionalFilter) {
      return { pass: true, confidence: 1. 0, reason: "Filter Disabled", score: 100 };
    }

    const ctx = await this.getMarketContext(symbol);
    if (! ctx) return { pass: true, confidence: 0.5, reason: "Data Error", score: 0 };

    const { closes, highs, lows, volumes, currentPrice } = ctx;
    let score = 100;
    let reasons = [];
    let confidence = 1.0;

    const rsi = RSI.calculate({ period: 14, values: closes }). pop();
    const adxRes = ADX.calculate({ high: highs, low: lows, close: closes, period: 14 });
    const adx = adxRes. length ?  adxRes[adxRes.length - 1]. adx : 0;
    const bb = BollingerBands. calculate({ period: 20, stdDev: 2, values: closes }). pop();
    const mfi = MFI.calculate({ high: highs, low: lows, close: closes, volume: volumes, period: 14 }). pop();

    const isSuperTrend = adx > 50;
    const isSqueezed = ((bb.upper - bb. lower) / bb.middle) < 0. 10;

    if (isSqueezed) {
        if (intendedSide === "LONG" && currentPrice > bb. upper && mfi > 50) {
            reasons.push(`BB Squeeze Breakout (UP)`);
            confidence = 1.3;
            return { pass: true, confidence, score: 100, reason: reasons.join(", ") };
        }
        if (intendedSide === "SHORT" && currentPrice < bb.lower && mfi < 50) {
            reasons.push(`BB Squeeze Breakout (DOWN)`);
            confidence = 1.3;
            return { pass: true, confidence, score: 100, reason: reasons.join(", ") };
        }
    }

    if (intendedSide === "LONG") {
        if (rsi < 50 && ! isSuperTrend) {
            if (rsi < 30 && currentPrice < bb.lower * 1.01) {
                reasons.push("Oversold Bounce Setup");
                confidence = 1.2;
            } else {
                score -= 100; reasons.push(`Weak(RSI=${rsi. toFixed(0)})`);
            }
        } else if (rsi > 70) {
            if (isSuperTrend) {
                reasons.push(`RSI>70 but SuperTrend -> ALLOWED`);
            } else {
                score -= 80; reasons.push(`Overbought(RSI=${rsi.toFixed(0)})`);
            }
        }
    } 
    else if (intendedSide === "SHORT") {
        if (rsi > 50 && !isSuperTrend) {
            if (rsi > 70 && currentPrice > bb.upper * 0.99) {
                reasons.push("Overbought Rejection Setup");
                confidence = 1.2;
            } else {
                score -= 100; reasons. push(`Strong(RSI=${rsi.toFixed(0)})`);
            }
        } else if (rsi < 30) {
            if (isSuperTrend) {
                reasons.push(`RSI<30 but SuperTrend -> ALLOWED`);
            } else {
                score -= 80; reasons.push(`Oversold(RSI=${rsi.toFixed(0)})`);
            }
        }
    }

    const pass = score >= 60; 
    if (score >= 90 && ! confidence) confidence = 1.1; 

    if (! pass) {
        log(`[GATEKEEPER BLOCKED] ${symbol} ${intendedSide} | Score:${score} | ${reasons.join(", ")}`);
    } else {
        log(`[GATEKEEPER PASSED] ${symbol} ${intendedSide} | Score:${score} | Conf:${confidence.toFixed(2)}`);
    }

    return { pass, confidence, score, reason: reasons.join(", ") };
  }
}
const gatekeeper = new InstitutionalGatekeeper();

async function getAllPositions() {
  try {
    const data = await sendSignedRequest("GET", "/fapi/v2/positionRisk", {});
    return Array.isArray(data) ? data : [];
  } catch (e) {
    log("getAllPositions error:", e?.response?.data || e.message);
    return [];
  }
}

async function setMarginType(symbol, marginType = "ISOLATED") {
  if (config. dryRun) {
    log("[DRYRUN] setMarginType (skipped)", symbol, marginType);
    return null;
  }
  try {
    return await sendSignedRequest("POST", "/fapi/v1/marginType", { symbol, marginType });
  } catch (e) {
    // 可能已经是该模式
    return null;
  }
}

async function getMarkPrice(symbol) {
  try {
    const data = await sendPublicRequest("/fapi/v1/premiumIndex", { symbol });
    if (! data) return null;
    return Number(data. markPrice || data.price || data. bidPrice || data.askPrice);
  } catch (e) {
    log("getMarkPrice error:", e?. response?.data || e. message);
    return null;
  }
}

async function getPosition(symbol) {
  try {
    const data = await sendSignedRequest("GET", "/fapi/v2/positionRisk", {});
    if (!data) return null;
    const p = (data || []).find(i => i.symbol === symbol);
    return p || null;
  } catch (e) {
    log("getPosition error:", e?. response?.data || e.message);
    return null;
  }
}

async function setLeverage(symbol, leverage) {
  if (config. dryRun) { log("[DRYRUN] setLeverage (skipped)", symbol, leverage); return null; }
  try {
    return await sendSignedRequest("POST", "/fapi/v1/leverage", { symbol, leverage });
  } catch (e) {
    return null;
  }
}

async function placeMarketOrder(symbol, side, quantity, reduceOnly = false) {
  const params = {
    symbol,
    side,
    type: "MARKET",
    quantity: quantity.toString(),
    recvWindow: 60000,
    reduceOnly: reduceOnly ?  "true" : "false",
    newOrderRespType: "RESULT"
  };
  log("placing market order", symbol, side, quantity, "reduceOnly=", reduceOnly);
  if (config.dryRun) {
    return { simulated: true, symbol, side, quantity, reduceOnly };
  }
  return await sendSignedRequest("POST", "/fapi/v1/order", params);
}

async function placeOrderSmart(symbol, side, qty) {
    if (config.dryRun) {
        log("[DRYRUN] Smart Entry sim.");
        return { orderId: "sim_smart", avgPrice: 100, executedQty: qty, status: "FILLED" };
    }

    log(`[ENTRY] Smart Entry ${symbol} ${side} Qty:${qty} -> Attempting Limit Maker... `);
    let limitOrderId = null;
    
    try {
        const book = await getBookTicker(symbol);
        if (!book) throw new Error("Book unavailable");
        
        const price = side === "BUY" ? book.bid : book.ask;
        
        const limitParams = {
            symbol, side, type: "LIMIT", quantity: qty.toString(),
            price: price.toString(), timeInForce: "GTX" 
        };
        
        const limitRes = await sendSignedRequest("POST", "/fapi/v1/order", limitParams);
        limitOrderId = limitRes.orderId;
        
        await sleep(config.limitOrderTimeoutSeconds * 1000);
        
        const statusRes = await sendSignedRequest("GET", "/fapi/v1/order", { symbol, orderId: limitOrderId });
        if (statusRes.status === "FILLED") {
            log(`[ENTRY] Limit Filled @ ${statusRes.avgPrice}`);
            return statusRes;
        }
        
        log(`[ENTRY] Limit Timeout.  Switching to Market...`);
        try { await sendSignedRequest("DELETE", "/fapi/v1/order", { symbol, orderId: limitOrderId }); } catch(e) {}
        
        const finalCheck = await sendSignedRequest("GET", "/fapi/v1/order", { symbol, orderId: limitOrderId });
        const executed = Number(finalCheck.executedQty);
        const remaining = qty - executed;
        
        if (remaining <= 0) return finalCheck;
        
        const marketParams = {
            symbol, side, type: "MARKET", quantity: remaining.toString()
        };
        return await sendSignedRequest("POST", "/fapi/v1/order", marketParams);

    } catch (e) {
        log(`[ENTRY] Smart Entry failed (${e.message}), fallback to Market. `);
        return await placeMarketOrder(symbol, side, qty, false);
    }
}

async function getAccountBalance() {
    if (config.dryRun) return 10000;
    try {
        const data = await sendSignedRequest("GET", "/fapi/v2/balance");
        const usdt = data.find(a => a.asset === "USDT");
        return usdt ? Number(usdt.balance) : 0;
    } catch(e) { return 0; }
}

async function checkGlobalDrawdown() {
    if (initialAccountBalance <= 0) return;
    const current = await getAccountBalance();
    if (current <= 0) return;
    const drawdownPct = ((initialAccountBalance - current) / initialAccountBalance) * 100;
    
    if (drawdownPct >= config.maxDailyDrawdownPct && ! isGlobalStop) {
        log(`[RISK MELTDOWN] Global Drawdown ${drawdownPct. toFixed(2)}% >= Limit ${config.maxDailyDrawdownPct}%.  STOPPING NEW POSITIONS.`);
        isGlobalStop = true;
    }
}

function parseSignalItem(raw) {
  if (!raw) return null;
  const rawSymbol = raw["币种"] || raw. symbol || raw.sym || raw.ticker || raw["coin"] || raw["pair"] || raw. symbolName || "";
  const symbol = sanitizeSymbol(rawSymbol);
  const priceVal = raw["当前价格"] || raw["price"] || raw["当前价"] || raw["close"] || raw. p || raw. price_usd || raw. lastPrice;
  const trigger_price = priceVal !== undefined && priceVal !== null ? Number(priceVal) : NaN;
  const timeVal = raw["时间"] || raw. time || raw. ts || raw. timestamp || raw["date"] || raw["datetime"];
  const trigger_time = (typeof timeVal === "number" || typeof timeVal === "string") ? timeVal : (raw.time_str || raw.datetime || raw["时间"]);
  const suggestion = raw["持仓建议"] || raw. advice || raw. action || raw.recommendation || raw. position || raw["建议"] || "";
  const suggestionStr = (suggestion || "").toString();
  const sTypeRaw = raw["信号类型"] || raw["方向"] || raw. signal_type || raw. direction || raw.side || raw["type"] || "";
  let signal_type = null;
  if (sTypeRaw) {
    const s = String(sTypeRaw).toLowerCase();
    if (s.includes("buy") || s.includes("多") || s.includes("long")) signal_type = "多头";
    else if (s.includes("sell") || s. includes("空") || s.includes("short")) signal_type = "空头";
    else signal_type = String(sTypeRaw);
  } else if (suggestionStr) {
    const s = suggestionStr.toLowerCase();
    if (s.includes("buy") || s.includes("多") || s.includes("long")) signal_type = "多头";
    else if (s.includes("sell") || s.includes("空") || s.includes("short")) signal_type = "空头";
  }

  const openKeywords = ["新开仓", "open", "entry", "enter", "开仓"];
  const closeKeywords = ["close", "平仓", "平掉", "退出", "stop", "close position"];

  const suggestionIndicatesOpen = openKeywords.some(k => (suggestionStr || ""). toString().toLowerCase().includes(k. toLowerCase()));
  const suggestionIndicatesClose = closeKeywords. some(k => (suggestionStr || "").toString().toLowerCase(). includes(k.toLowerCase()));

  const isOpen = (! suggestionIndicatesClose) && (suggestionIndicatesOpen || (sTypeRaw && !!signal_type));

  if (! symbol || !isFinite(trigger_price) || !trigger_time || !isOpen) return null;

  return {
    raw,
    symbol,
    signal_type,
    trigger_price: Number(trigger_price),
    trigger_time
  };
}

async function fetchFromUrl(url, sinceISO) {
  try {
    const full = url + (sinceISO ? ("? since=" + encodeURIComponent(sinceISO)) : "");
    if (config.verbose) log("fetchFromUrl:", full);
    const res = await axiosInstance.get(full, { timeout: 10000 });
    return Array.isArray(res. data) ? res. data : (res.data && Array.isArray(res.data. data) ? res.data.data : []);
  } catch (e) {
    log("fetchFromUrl error:", url, e?.response?.data || e. message);
    return [];
  }
}

async function fetchCSignalsSince(sinceISO) {
  try {
    const urls = [];
    if (config.cSignalsUrl) urls.push(config. cSignalsUrl);
    if (config.extraSignalsUrl) urls. push(config.extraSignalsUrl);
    if (config.aSignalsUrl) urls.push(config. aSignalsUrl);

    const results = await Promise.all(urls.map(u => fetchFromUrl(u, sinceISO)));
    const combined = [];
    for (const arr of results) {
      if (Array.isArray(arr)) {
        for (const item of arr) {
          const parsed = parseSignalItem(item);
          if (! parsed) continue;
          if (isBlacklisted(parsed.symbol)) {
            if (config.verbose) log("fetchCSignalsSince: skipping blacklisted signal for", parsed.symbol);
            continue;
          }
          combined.push(parsed);
        }
      }
    }
    return combined;
  } catch (e) {
    log("fetchCSignals error:", e?.response?.data || e. message);
    return [];
  }
}

async function closeExistingPosition(symbol, existingSide, exchangePosAmt, localPosAmt) {
  const amt = (exchangePosAmt && exchangePosAmt > 0) ? Math.abs(exchangePosAmt) : Math.abs(localPosAmt || 0);
  if (! amt || amt <= 0) {
    log("nothing to close for", symbol, "amt=", amt);
    return { simulated: true, note: "no-amt" };
  }
  const qty = lotRound(symbol, amt);
  if (! qty || qty <= 0) {
    log("calculated close qty 0 for", symbol, "amt:", amt);
    return { simulated: true, note: "qty-0" };
  }
  const closeSide = (existingSide === "LONG" || existingSide === "BUY") ? "SELL" : "BUY";
  log(`attempting to close existing position ${symbol} side=${existingSide} qty=${qty}`);
  
  // 先撤销止损单
  await cancelStopOrder(symbol);
  
  const res = await placeMarketOrder(symbol, closeSide, qty, true); 
  log("closeExistingPosition result:", res);

  try {
    await computeAndPersistClose(symbol, res, existingSide, qty);
  } catch (e) {
    log("computeAndPersistClose error after closeExistingPosition:", e?. message || e);
  }

  return res;
}

async function waitForPositionZero(symbol, maxWaitMs = 10000, intervalMs = 800) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const pos = await getPosition(symbol);
      const amt = pos ? Number(pos.positionAmt || 0) : 0;
      if (! amt || Math.abs(amt) < 1e-8) return true;
    } catch (e) {
      log("waitForPositionZero getPosition error for", symbol, e?. message || e);
    }
    await sleep(intervalMs);
  }
  return false;
}

async function openPositionIfNeeded(signal) {
  if (isGlobalStop) {
      log(`[RISK] Global Stop Active. Ignoring signal for ${signal.symbol}`);
      return;
  }

  let symbol = signal.symbol;
  
  if (symbol === "USDT" || symbol === "USDC" || symbol === "BUSD" || symbol. length < 6) {
      log(`skip symbol (invalid pair name: ${symbol})`);
      return;
  }

  if (!/USDT$/. test(symbol)) {
    log("skip symbol (not USDT pair):", symbol);
    return;
  }

  symbol = sanitizeSymbol(symbol);

  if (isBlacklisted(symbol)) {
    log("skip openPosition: symbol is blacklisted:", symbol);
    return;
  }

  try {
    const rawTime = signal.trigger_time || (signal.raw && (signal.raw["时间"] || signal. raw.time)) || "";
    const sigTs = parseSignalTimeToMs(rawTime);
    const now = Date.now();
    if (! config.allowOldSignals && (isNaN(sigTs) || (now - sigTs) > MAX_SIGNAL_AGE_MS)) {
      log("signal too old or invalid, skipping openPosition:", symbol, "signal_time=", signal.trigger_time, "age_ms=", isNaN(sigTs) ? "NaN" : (now - sigTs));
      return;
    }
  } catch (e) {
    log("failed to parse signal time, skipping:", e. // ====== 继续 openPositionIfNeeded 函数 ======

async function openPositionIfNeeded(signal) {
  if (isGlobalStop) {
      log(`[RISK] Global Stop Active.  Ignoring signal for ${signal.symbol}`);
      return;
  }

  let symbol = signal. symbol;
  
  if (symbol === "USDT" || symbol === "USDC" || symbol === "BUSD" || symbol. length < 6) {
      log(`skip symbol (invalid pair name: ${symbol})`);
      return;
  }

  if (!/USDT$/.test(symbol)) {
    log("skip symbol (not USDT pair):", symbol);
    return;
  }

  symbol = sanitizeSymbol(symbol);

  if (isBlacklisted(symbol)) {
    log("skip openPosition: symbol is blacklisted:", symbol);
    return;
  }

  try {
    const rawTime = signal.trigger_time || (signal. raw && (signal. raw["时间"] || signal.raw. time)) || "";
    const sigTs = parseSignalTimeToMs(rawTime);
    const now = Date.now();
    if (! config.allowOldSignals && (isNaN(sigTs) || (now - sigTs) > MAX_SIGNAL_AGE_MS)) {
      log("signal too old or invalid, skipping openPosition:", symbol, "signal_time=", signal. trigger_time, "age_ms=", isNaN(sigTs) ? "NaN" : (now - sigTs));
      return;
    }
  } catch (e) {
    log("failed to parse signal time, skipping:", e. message || e);
    return;
  }

  const intendedSide = signal.signal_type === "多头" ? "LONG" : "SHORT";

  // [V6 核心] BTC 同向过滤
  const btcCheck = await checkBTCAlignment(intendedSide);
  if (! btcCheck.aligned) {
    log(`[BTC FILTER] Blocked ${symbol} ${intendedSide} - BTC is ${btcCheck.btcDirection}`);
    return;
  }

  // 冷却时间检查
  const posCheck = loadPositions();
  if (posCheck[symbol]) {
      if (posCheck[symbol].open && posCheck[symbol]. side === intendedSide) {
          return; 
      }
      
      if (posCheck[symbol].open === false && posCheck[symbol].lastClosedAt) {
          const diff = Date.now() - posCheck[symbol].lastClosedAt;
          if (diff < REOPEN_COOLDOWN_MS) {
              if (posCheck[symbol].lastClosedSide === intendedSide) {
                  log(`[COOLDOWN] Skipping ${symbol} ${intendedSide}. Closed ${Math.floor(diff/60000)}m ago (Limit ${config.reopenCooldownMinutes}m). `);
                  return;
              }
          }
      }
  }

  let confidenceMultiplier = 1. 0;
  let calculatedScore = null; 

  if (! config. dryRun && config.institutionalFilter) {
     const evaluation = await gatekeeper.evaluate(symbol, intendedSide);
     if (! evaluation.pass) {
       log(`[FILTER BLOCKED] ${symbol} ${intendedSide} - ${evaluation.reason}`);
       return; 
     }
     confidenceMultiplier = evaluation.confidence || 1.0;
     calculatedScore = evaluation.score; 
  }

  const perSignalLeverage = Number(signal.raw && signal.raw.desired_leverage) || config.defaultLeverage;
  let perSignalUsdt = Number(signal.raw && signal.raw.usdt_amount) || config.usdtPerTrade;
  
  if (config.volatilitySizing. enabled && !config. dryRun) {
     try {
       const ctx = await gatekeeper.getMarketContext(symbol);
       if (ctx && ctx.closes && ctx.closes.length) {
          const input = { high: ctx.highs, low: ctx.lows, close: ctx.closes, period: 14 };
          const atrResult = ATR. calculate(input);
          const currentATR = atrResult.length ?  atrResult[atrResult.length - 1] : null;
          
          if (currentATR) {
             const currentPrice = ctx.currentPrice;
             const volatilityPct = (currentATR / currentPrice) * 100;
             const target = config.volatilitySizing.targetVolatility;
             let volScaler = target / volatilityPct;
             volScaler = Math. max(config.volatilitySizing.minScale, Math.min(volScaler, config.volatilitySizing.maxScale));
             log(`[VOLATILITY SIZING] ${symbol} ATR%=${volatilityPct.toFixed(2)}% Target=${target}% -> Scaler=${volScaler. toFixed(2)}x`);
             confidenceMultiplier = confidenceMultiplier * volScaler;
          }
       }
     } catch (e) {
        log(`[VOLATILITY SIZING] Error calculating ATR for ${symbol}, using default size. `, e.message);
     }
  }

  perSignalUsdt = perSignalUsdt * confidenceMultiplier;
  
  if (config.volatilitySizing && config.volatilitySizing.maxUsdtCap) {
     if (perSignalUsdt > config.volatilitySizing.maxUsdtCap) {
        perSignalUsdt = config.volatilitySizing.maxUsdtCap;
     }
  }

  const perSignalUsdtIsMargin = (signal.raw && (String(signal.raw. usdt_is_margin) === "true")) ?  true : config.usdtPerTradeIsMargin;

  const posData = loadPositions();

  const exchangePos = await getPosition(symbol);
  const exchangeAmt = exchangePos ? Number(exchangePos. positionAmt || 0) : 0;
  const exchangeSide = exchangeAmt > 0 ? "LONG" : (exchangeAmt < 0 ? "SHORT" : null);

  if (Math.abs(exchangeAmt) > 0. 0000001) {
    if (exchangeSide === intendedSide) {
      log("exchange already reports same-side position for", symbol, "amount:", exchangeAmt, "will track but skip opening new");
      
      let entryFromExchange = null;
      try {
        entryFromExchange = exchangePos && (exchangePos.entryPrice || exchangePos.entry_price || exchangePos.entry || exchangePos.avgEntryPrice || exchangePos.entryAvgPrice);
        if (entryFromExchange && ! isFinite(Number(entryFromExchange))) entryFromExchange = null;
        if (entryFromExchange) entryFromExchange = Number(entryFromExchange);
      } catch (e) {
        entryFromExchange = null;
      }
      if (! entryFromExchange) {
        try {
          const m = await getMarkPrice(symbol);
          if (m && isFinite(m)) entryFromExchange = m;
        } catch (e) {}
      }

      const oldPos = posData[symbol] || {};
      posData[symbol] = {
        open: true,
        side: exchangeSide,
        amount: Math.abs(exchangeAmt),
        openedAt: oldPos.openedAt || Date.now(), 
        entryPrice: entryFromExchange || oldPos.entryPrice || null,
        source: "existing_exchange",
        lastSyncedFromExchangeAt: Date.now(),
        gatekeeperScore: oldPos.gatekeeperScore || null,
        gatekeeperRawScore: oldPos. gatekeeperRawScore || null, 
        monitor: oldPos.monitor || { active: false, peak: 0, tier: null, enteredAt: null },
        protection: oldPos.protection || { active: false, peak: 0, triggerPct: null, closeToPct: null, enteredAt: null },
        highestPrice: oldPos.highestPrice || null, 
        highestPriceATR: oldPos. highestPriceATR || null,
        ironDefenseActive: oldPos.ironDefenseActive || false,
        currentStopTier: oldPos. currentStopTier || null
      };

      savePositions(posData);
      startMarketWsForSymbols(Object.keys(loadPositions()). filter(s=>loadPositions()[s]. open));
      return;
    } else {
      log("exchange reports OPPOSITE position for", symbol, `exchangeSide=${exchangeSide} intended=${intendedSide} amount=${exchangeAmt}.  Will close then open opposite. `);
      try {
        await closeExistingPosition(symbol, exchangeSide, Math.abs(exchangeAmt), null);
        performSoftClose(symbol, exchangeSide);

        const cleared = await waitForPositionZero(symbol, 10000, 800);
        if (! cleared) {
          log("warning: position not cleared on exchange after close for", symbol, "aborting open to avoid conflict");
          return;
        }
      } catch (e) {
        log("error while closing exchange opposite position for", symbol, e?. response?.data || e. message || e);
        return;
      }
    }
  }

  if (posData[symbol] && posData[symbol].open) {
    const localSide = (posData[symbol].side || ""). toUpperCase();
    if (localSide === intendedSide) {
      log("already have tracked open position for", symbol, "same side", localSide, "skip");
      return;
    } else {
      log("found tracked OPPOSITE-side local position for", symbol, "trackedSide=", localSide, "intended=", intendedSide, "-> will close tracked then open opposite");
      try {
        const exchangePos2 = await getPosition(symbol);
        const exchangeAmt2 = exchangePos2 ? Math.abs(Number(exchangePos2.positionAmt || 0)) : 0;
        const localAmt = Math.abs(Number(posData[symbol].amount || 0));
        await closeExistingPosition(symbol, localSide, exchangeAmt2, localAmt);
        
        performSoftClose(symbol, localSide);

        const cleared2 = await waitForPositionZero(symbol, 10000, 800);
        if (!cleared2) {
          log("warning: position not cleared on exchange after closing local tracked for", symbol, "aborting open to avoid conflict");
          return;
        }

      } catch (e) {
        log("error while closing local tracked opposite position for", symbol, e?.response?.data || e.message || e);
        return;
      }
    }
  }

  const exchangePosFinal = await getPosition(symbol);
  const exchangeAmtFinal = exchangePosFinal ? Number(exchangePosFinal.positionAmt || 0) : 0;
  if (Math.abs(exchangeAmtFinal) > 0.0000001) {
    const sideFinal = exchangeAmtFinal > 0 ?  "LONG" : "SHORT";
    if (sideFinal !== intendedSide) {
      log("after attempted close, exchange still reports opposite position for", symbol, "side=", sideFinal, "amt=", exchangeAmtFinal, "aborting to avoid conflict");
      return;
    }
  }

  try {
    await setMarginType(symbol, "ISOLATED");
  } catch (e) {
    log("setMarginType failed:", e.message || e);
  }

  try {
    await setLeverage(symbol, perSignalLeverage);
  } catch (e) {
    log("setLeverage failed:", e.message || e);
  }

  const mark = await getMarkPrice(symbol);
  if (!mark || !isFinite(mark)) {
    log("cannot get mark price for", symbol, "skip");
    return;
  }

  const exInfo = await getExchangeInfo();
  if (exInfo) exchangeInfoCache = exInfo;

  const leverage = perSignalLeverage || config.defaultLeverage || 1;
  let notional;
  if (perSignalUsdtIsMargin) {
    notional = perSignalUsdt * leverage;
  } else {
    notional = perSignalUsdt;
  }
  const rawQty = notional / mark;
  const qty = lotRound(symbol, rawQty);

  log("openPosition debug:", { symbol, perSignalUsdt, perSignalUsdtIsMargin, leverage, mark, rawQty, qty, intendedSide });

  if (! qty || qty <= 0) {
    log("calculated qty 0 for", symbol, "rawQty:", rawQty, "consider increasing USDT_PER_TRADE or using per-signal usdt_amount");
    return;
  }

  const side = intendedSide === "LONG" ?  "BUY" : "SELL";

  try {
    const send_order_ts = nowMs();
    const order = await placeOrderSmart(symbol, side, qty); 
    const ack_ts = nowMs();
    log("open order result (timing):", { symbol, send_order_ts, ack_ts, took_ms: ack_ts - send_order_ts, order: config.verbose ?  order : (order && order.orderId ? { orderId: order. orderId } : {}) });

    let executedPrice = Number(order. avgPrice) || mark;

    const pos = loadPositions();
    pos[symbol] = {
      open: true,
      side: intendedSide,
      amount: qty,
      openedAt: Date.now(),
      entryPrice: executedPrice,
      source: "C_strategy_Smart",
      signal: signal,
      gatekeeperScore: confidenceMultiplier, 
      gatekeeperRawScore: calculatedScore, 
      lastOrder: order || null,
      highestPrice: mark, 
      highestPriceATR: null,
      ironDefenseActive: false,
      currentStopTier: null,
      peakProfitPct: 0
    };
    
    savePositions(pos);

    startMarketWsForSymbols(Object.keys(loadPositions()).filter(s=>loadPositions()[s].open));
  } catch (e) {
    log("openPosition error:", e?.response?.data || e. message);
  }
}
// ====== 第四部分 - 监控和主循环 ======

// [V6 核心] monitorProfitAndClose - 集成铁壁止损
async function monitorProfitAndClose() {
  const pos = loadPositions();
  let changed = false;
  const now = Date.now();

  for (const symbol of Object.keys(pos)) {
    const p = pos[symbol];
    if (!p || !p.open) continue;

    const entry = p.entryPrice ?  Number(p.entryPrice) : null;
    if (! entry || entry === 0) continue;

    const ctx = await gatekeeper.getMarketContext(symbol); 
    if (!ctx || !ctx.closes || ctx.closes.length === 0) continue;
    
    const currentPrice = ctx.currentPrice; 
    const mark = await getMarkPrice(symbol); 
    
    if (!mark) continue;
    
    const atrRes = ATR.calculate({ high: ctx.highs, low: ctx.lows, close: ctx. closes, period: 14 });
    const currentATR = atrRes.length ? atrRes[atrRes.length - 1] : (currentPrice * 0.01); 

    let profitPercent = null;
    const sideUp = (p.side || "").toString().toUpperCase();
    
    if (sideUp === "LONG") {
      profitPercent = ((mark - entry) / entry) * 100;
    } else {
      profitPercent = ((entry - mark) / entry) * 100;
    }

    // 更新最高浮盈
    if (! p.peakProfitPct || profitPercent > p.peakProfitPct) {
        p.peakProfitPct = profitPercent;
        changed = true;
    }

    if (! p.highestPrice || (sideUp === "LONG" && mark > p.highestPrice) || (sideUp === "SHORT" && mark < p.highestPrice)) {
        p. highestPrice = mark;
        p.highestPriceATR = currentATR; 
        changed = true;
    }
    if (! p.highestPrice) { p.highestPrice = mark; p.highestPriceATR = currentATR; }

    // ============================================================
    // [V6 核心] 四级移动止损逻辑
    // ============================================================
    if (config.trailingStop. enabled) {
        const currentTier = p.currentStopTier !== null && p.currentStopTier !== undefined ?  p.currentStopTier : -1;
        const newTier = calculateStopTier(profitPercent, currentTier);
        
        // [V6 铁壁原则] 一旦进入某个档位，永不降级
        if (newTier > currentTier || (p.ironDefenseActive && currentTier >= 0)) {
            const effectiveTier = Math.max(newTier, currentTier >= 0 ? currentTier : newTier);
            
            if (effectiveTier >= 0) {
                const stopPrice = calculateStopPrice(entry, sideUp, profitPercent, effectiveTier);
                
                if (stopPrice) {
                    const posAmt = Number(p.amount || 0);
                    await placeOrUpdateStopOrder(symbol, sideUp, posAmt, stopPrice, effectiveTier);
                    
                    if (p.currentStopTier !== effectiveTier) {
                        p. currentStopTier = effectiveTier;
                        p.ironDefenseActive = true;
                        log(`[IRON DEFENSE] ${symbol} upgraded to Tier ${effectiveTier} | Profit: ${profitPercent.toFixed(2)}% | Stop @ ${stopPrice.toFixed(4)}`);
                        changed = true;
                    }
                }
            }
        }
    }

    // ============================================================
    // 动态ATR半仓止盈
    // ============================================================
    const atrPct = (currentATR / entry) * 100;
    const dynamicTakeProfitPct = Math.max(0. 8, Math.min(1.5, atrPct * 1.5));

    if (config.partialCloseEnabled && ! p.partialClosed && profitPercent >= dynamicTakeProfitPct) {
        try {
            const totalAmt = Number(p.amount || 0);
            let closeQty = lotRound(symbol, totalAmt * 0.5);
            
            if (closeQty > 0) {
                log(`[REALITY EXIT] ${symbol} Profit=${profitPercent. toFixed(2)}% >= Target(${dynamicTakeProfitPct.toFixed(2)}%).  Partial 50% Close.`);
                const closeSide = (sideUp === "LONG") ? "SELL" : "BUY";
                const r = await placeMarketOrder(symbol, closeSide, closeQty, true);
                await handlePartialClose(symbol, closeQty, p.side, r);
                
                // handlePartialClose 会自动激活 ironDefense 并挂保本止损
                changed = true;
                continue; 
            }
        } catch (e) {
            log("partial close error:", e.message);
        }
    }

    let shouldClose = false;
    let closeReason = null;

    const heldTimeMs = now - (p.openedAt || now);
    const heldMinutes = heldTimeMs / 1000 / 60;
    const maxMinutes = config.maxHoldTimeMinutes || 60;

    // 时间衰减逻辑
    if (heldMinutes > 30 && heldMinutes <= 45 && profitPercent < -0.2) {
         shouldClose = true; closeReason = `TimeDecay_30m_Weak`;
    } else if (heldMinutes > 45 && profitPercent < 0.1) {
         shouldClose = true; closeReason = `TimeDecay_45m_NoProfit`;
    } else if (heldMinutes > maxMinutes) {
         let trendHealthy = false;
         if (ctx.closes. length >= 20) {
             const ema20 = EMA. calculate({ period: 20, values: ctx.closes }). pop();
             trendHealthy = (sideUp === "LONG") ? (mark > ema20) : (mark < ema20);
         }

         if (trendHealthy) {
             log(`[TIME EXTEND] ${symbol} held > 60m but Price > EMA20 (Trend Healthy). Holding... `);
         } else {
             shouldClose = true; closeReason = `Time_Expiry_TrendBroken`;
         }
    }

    // 中轨止盈
    const bb = BollingerBands. calculate({ period: 20, stdDev: 2, values: ctx.closes }). pop();
    if (! shouldClose && bb) {
        if (sideUp === "LONG" && mark < bb.middle && profitPercent > -0.5) {
             shouldClose = true; closeReason = "MIDDLE_BAND_BREAK";
        }
        if (sideUp === "SHORT" && mark > bb.middle && profitPercent > -0.5) {
             shouldClose = true; closeReason = "MIDDLE_BAND_BREAK";
        }
    }

    // 硬止损
    if (! shouldClose && profitPercent < -config.stopLossPercent) {
        shouldClose = true;
        closeReason = `HARD_STOP_LOSS`;
    }

    // ATR止损
    if (!shouldClose) {
        const atrStopMultiplier = config. atrStopMultiplier || 2. 0; 
        const atrStopDist = currentATR * atrStopMultiplier;

        if (sideUp === "LONG" && mark < (entry - atrStopDist)) {
            shouldClose = true;
            closeReason = `ATR_Stop`;
        } else if (sideUp === "SHORT" && mark > (entry + atrStopDist)) {
            shouldClose = true;
            closeReason = `ATR_Stop`;
        }
    }

    // [V6] 铁壁保本检查 (内存级备份)
    if (!shouldClose && p.ironDefenseActive) {
        const feeBuffer = entry * 0.003; // 0.3%
        
        if (sideUp === "LONG" && mark < (entry + feeBuffer)) {
            shouldClose = true;
            closeReason = `IronDefense_Breakeven`;
        } else if (sideUp === "SHORT" && mark > (entry - feeBuffer)) {
            shouldClose = true;
            closeReason = `IronDefense_Breakeven`;
        }
    }

    if (shouldClose) {
      try {
        log(`REALITY EXIT: closing ${symbol} due to ${closeReason} (profit=${profitPercent. toFixed(4)}%)`);
        const exchangePos = await getPosition(symbol);
        const posAmt = exchangePos ? Math.abs(Number(exchangePos.positionAmt || 0)) : Math.abs(Number(p.amount || 0));
        
        if (! posAmt || posAmt <= 0) {
          log(`[WARN] Position amount missing for ${symbol}, performing soft close. `);
          await cancelStopOrder(symbol);
          performSoftClose(symbol, p.side);
          changed = true; 
          continue;
        }

        const qty = lotRound(symbol, posAmt);
        const closeSide = (p.side === "LONG") ? "SELL" : "BUY";
        
        await cancelStopOrder(symbol);
        const r = await placeMarketOrder(symbol, closeSide, qty, true);
        
        await computeAndPersistClose(symbol, r, p.side, qty);
        
        changed = true;
      } catch (e) {
        log("monitor close error:", e. message);
      }
    } else {
        p.monitor = { active: true, peak: profitPercent || 0 };
        if (changed) savePositions(pos);
    }
  }
  
  if (changed) savePositions(pos);
}

async function checkAndCloseExpiredPositions() {
    const pos = loadPositions();
    const now = Date.now();
    const DAY_MS = 24 * 3600 * 1000;
    let changed = false;

    for (const symbol of Object.keys(pos)) {
        const p = pos[symbol];
        if (! p || !p. open) continue;
        const openedAt = Number(p.openedAt || 0);
        
        if ((now - openedAt) >= DAY_MS) {
            const exchangePos = await getPosition(symbol);
            const posAmt = exchangePos ?  Math.abs(Number(exchangePos. positionAmt || 0)) : Math.abs(Number(p.amount || 0));
            if (posAmt > 0) {
                log(`Hard Expiry 24h close for ${symbol}`);
                const qty = lotRound(symbol, posAmt);
                const side = (p.side === "LONG") ? "SELL" : "BUY";
                await cancelStopOrder(symbol);
                await placeMarketOrder(symbol, side, qty, true);
                performSoftClose(symbol, p.side);
                changed = true;
            }
        }
    }
    if (changed) savePositions(pos);
}

// [V6 新增] 启动时恢复止损单
async function recoverStopOrders() {
    log("[RECOVERY] Checking for positions that need stop orders.. .");
    const pos = loadPositions();
    
    for (const symbol of Object.keys(pos)) {
        const p = pos[symbol];
        if (!p || !p.open) continue;
        
        const entry = p.entryPrice ? Number(p.entryPrice) : null;
        if (!entry) continue;
        
        const mark = await getMarkPrice(symbol);
        if (!mark) continue;
        
        const sideUp = (p.side || ""). toUpperCase();
        let profitPercent = (sideUp === "LONG") 
            ? ((mark - entry) / entry) * 100 
            : ((entry - mark) / entry) * 100;
        
        // 如果之前有止损档位，恢复它
        if (p.currentStopTier !== null && p.currentStopTier !== undefined && p.currentStopTier >= 0) {
            const stopPrice = calculateStopPrice(entry, sideUp, profitPercent, p.currentStopTier);
            if (stopPrice) {
                const posAmt = Number(p.amount || 0);
                await placeOrUpdateStopOrder(symbol, sideUp, posAmt, stopPrice, p.currentStopTier);
                log(`[RECOVERY] Restored stop order for ${symbol} at Tier ${p.currentStopTier}`);
            }
        }
        // 如果有铁壁防御但没有止损档位，挂保本止损
        else if (p. ironDefenseActive || p.partialClosed) {
            const stopPrice = sideUp === "LONG" 
                ? entry * 1.003 
                : entry * 0.997;
            const posAmt = Number(p.amount || 0);
            await placeOrUpdateStopOrder(symbol, sideUp, posAmt, stopPrice, 0);
            log(`[RECOVERY] Restored breakeven stop for ${symbol}`);
        }
    }
}

async function createListenKeyRaw() {
  if (config. dryRun) return "mock_listen_key";
  try {
    const res = await axiosInstance({
      method: "POST",
      url: `${API_BASE}/fapi/v1/listenKey`,
      headers: { "X-MBX-APIKEY": config. apiKey }
    });
    return res.data. listenKey;
  } catch (e) {
    log("createListenKeyRaw error:", e.message);
    return null;
  }
}

async function keepAliveListenKey() {
  if (config. dryRun || ! userListenKey) return;
  try {
    await axiosInstance({
      method: "PUT",
      url: `${API_BASE}/fapi/v1/listenKey`,
      headers: { "X-MBX-APIKEY": config.apiKey }
    });
  } catch (e) {
    log("keepAliveListenKey error:", e. message);
  }
}

async function startUserDataWs() {
  if (userWsStarting) return;
  userWsStarting = true;
  try {
    userListenKey = await createListenKeyRaw();
    if (! userListenKey) {
      log("startUserDataWs: failed to create listenKey (will retry)");
      userWsStarting = false;
      userReconnectAttempts++;
      const backoff = Math.min(60000, 1000 * Math. pow(2, Math.max(0, userReconnectAttempts - 1)));
      setTimeout(startUserDataWs, backoff);
      return;
    }
    const url = `${USERDATA_WS_HOST}/ws/${userListenKey}`;
    if (userWs) try { userWs.terminate(); } catch (e) {}
    userWs = new WebSocket(url);

    userWs. on("open", () => {
      userReconnectAttempts = 0;
      userWsStarting = false;
      lastUserWsMsgAt = Date.now();
      lastUserWsHeartbeat = Date. now();
      log("userData ws open");
      if (listenKeyKeepAliveInterval) clearInterval(listenKeyKeepAliveInterval);
      listenKeyKeepAliveInterval = setInterval(() => {
        keepAliveListenKey(). catch(e => log("keepAliveListenKey failed:", e && (e.message || e)));
      }, 30 * 60 * 1000);
    });

    userWs.on("ping", () => { userWs.pong(); lastUserWsHeartbeat = Date.now(); });
    userWs. on("pong", () => { lastUserWsHeartbeat = Date. now(); });

    userWs.on("message", async (msg) => {
      lastUserWsMsgAt = Date.now();
      lastUserWsHeartbeat = Date. now();
      try {
        const data = JSON.parse(msg. toString());
        if (data && data.e === "ORDER_TRADE_UPDATE") {
          if (config.verbose) log("ORDER_TRADE_UPDATE:", JSON.stringify(data));
          const o = data.o || data. order || data;
          const symbol = (o.s || o.symbol || "").toUpperCase();
          const side = (o.S || o.side || "").toUpperCase();
          const status = (o.X || o.x || o.status || "").toUpperCase();
          const orderType = (o.o || o.type || "").toUpperCase();
          const executedQty = Number(o.z || o.executedQty || 0);
          const lastExecutedQty = Number(o.l || 0); 
          const price = Number(o. L || o.avgPrice || o.p || o.price || 0);
          
          const isReduceOnly = (o.R === true);

          // [V6] 检测止损单被触发
          if (orderType === "STOP_MARKET" && status === "FILLED") {
              log(`[STOP ORDER TRIGGERED] ${symbol} Stop order filled @ ${price}`);
              const pos = loadPositions();
              if (pos[symbol] && pos[symbol]. open) {
                  await computeAndPersistClose(symbol, o, pos[symbol]. side, executedQty);
              }
              continue;
          }

          const pos = loadPositions();
          const tracked = pos[symbol];
          try {
            if (tracked && tracked.open) {
              if (lastExecutedQty > 0 && (status === "FILLED" || status === "TRADED" || status === "TRADE")) {
                const trackedSide = (tracked.side || "").toUpperCase();
                if ((trackedSide === "LONG" && side === "SELL") || (trackedSide === "SHORT" && side === "BUY")) {
                  log("userWs: detected close/partial-close for", symbol, "order", o);
                  await handlePartialClose(symbol, lastExecutedQty, tracked.side, o);
                  startMarketWsForSymbols(Object.keys(loadPositions()). filter(s=>loadPositions()[s]. open));
                } else {
                  if (tracked && (! tracked.entryPrice || tracked. entryPrice === null)) {
                    const exPrice = extractExecutedPriceFromOrder(o);
                    if (exPrice) {
                      tracked.entryPrice = exPrice;
                      tracked.lastOrder = o;
                      savePositions(pos);
                    }
                  }
                }
              }
            } else {
              if (lastExecutedQty > 0 && (status === "FILLED" || status === "TRADE" || status === "TRADED")) {
                if (isReduceOnly) return;

                if (tracked && tracked.lastClosedAt && (Date.now() - tracked.lastClosedAt < 5000)) return;

                const ex = extractExecutedPriceFromOrder(o) || price || null;
                const sideKind = (side === "BUY") ? "LONG" : (side === "SELL") ? "SHORT" : null;
                if (sideKind) {
                  log(`userWs: Detected NEW position ${symbol} ${sideKind}`);
                  const oldScore = (tracked && tracked.gatekeeperScore) ?  tracked.gatekeeperScore : null;
                  const oldRawScore = (tracked && tracked.gatekeeperRawScore) ? tracked. gatekeeperRawScore : null; 
                  pos[symbol] = {
                    open: true,
                    side: sideKind,
                    amount: executedQty, 
                    openedAt: Date. now(),
                    entryPrice: ex,
                    source: "userws_sync",
                    gatekeeperScore: oldScore, 
                    gatekeeperRawScore: oldRawScore,
                    lastOrder: o,
                    highestPrice: ex, 
                    highestPriceATR: null,
                    ironDefenseActive: false,
                    currentStopTier: null,
                    peakProfitPct: 0
                  };
                  savePositions(pos);
                  startMarketWsForSymbols(Object.keys(loadPositions()).filter(s=>loadPositions()[s].open));
                }
              }
            }
          } catch (e) {
            log("userWs ORDER_TRADE_UPDATE handling error:", e && (e.message || e));
          }
        }
      } catch (e) { if (config.verbose) log("userWs message parse err:", e && e.message); }
    });

    userWs.on("close", (code, reason) => {
      log("userData ws closed", code, reason && reason.toString());
      if (listenKeyKeepAliveInterval) clearInterval(listenKeyKeepAliveInterval);
      userListenKey = null;
      userWs = null;
      userReconnectAttempts++;
      const backoff = Math.min(300000, 1000 * Math.pow(2, Math. max(0, userReconnectAttempts - 1)) + Math.floor(Math.random() * 2000));
      setTimeout(startUserDataWs, backoff);
    });
    userWs.on("error", (err) => {
      log("userWs error:", err && (err.message || err));
      try { userWs.terminate(); } catch (e) {}
    });
  } catch (e) {
    log("startUserDataWs error:", e && (e.message || e));
    userWsStarting = false;
    userReconnectAttempts++;
    const backoff = Math.min(300000, 1000 * Math.pow(2, Math. max(0, userReconnectAttempts - 1)));
    setTimeout(startUserDataWs, backoff);
  }
}

let marketWs = null;
let marketSubscribedSymbols = new Set();

function buildMarketStreamUrl(symbols) {
  if (! symbols || symbols.length === 0) return null;
  const t = (config.marketStreamType || "trade").toLowerCase();
  const suffix = (t === "trade") ? "@trade" : (t === "aggtrade" ? "@aggTrade" : "@markPrice");
  const parts = symbols.map(s => s.toLowerCase() + suffix);
  return `wss://fstream.binance.com/stream?streams=${parts.join("/")}`;
}

function startMarketWsForSymbols(symbols) {
  try {
    const uniq = Array.from(new Set(symbols || []));
    const cleaned = uniq.map(s => sanitizeSymbol(s)). filter(Boolean);
    const same = cleaned.length === marketSubscribedSymbols.size && cleaned.every(s => marketSubscribedSymbols.has(s));
    if (same && marketWs && marketWs.readyState === WebSocket.OPEN) return;
    const url = buildMarketStreamUrl(cleaned);
    if (! url) {
      if (marketWs) { try { marketWs. terminate(); } catch(e){} }
      marketSubscribedSymbols = new Set();
      marketWs = null;
      return;
    }
    if (marketWs) try { marketWs. terminate(); } catch (e) {}
    marketSubscribedSymbols = new Set(cleaned);
    marketWs = new WebSocket(url);

    let marketReconnectAttempts = 0;

    marketWs. on("open", () => {
      marketReconnectAttempts = 0;
      lastMarketWsMsgAt = Date. now();
      log("market ws open for symbols:", cleaned.length, "streamType:", config.marketStreamType);
    });
    marketWs. on("message", async (raw) => {
      lastMarketWsMsgAt = Date. now();
      try {
        const msg = JSON.parse(raw.toString());
        const payload = msg.data || msg;
        const sym = (payload.s || payload.symbol || payload.S || "").toUpperCase();
        let price = NaN;
        if (payload.p !== undefined) price = Number(payload.p);
        else if (payload.price !== undefined) price = Number(payload.price);
        else if (payload.markPrice !== undefined) price = Number(payload. markPrice);
        else if (payload. P !== undefined) price = Number(payload.P);
        const ts = payload.E || payload.T || Date.now();
        if (sym && isFinite(price)) {
          await onPriceTick(sym, price, ts);
        }
      } catch (e) {
        if (config.verbose) log("marketWs message parse err:", e && e. message);
      }
    });
    marketWs.on("close", (code, reason) => {
      log("market ws closed", code, reason && reason.toString());
      marketReconnectAttempts++;
      const backoff = Math.min(30000, 1000 * marketReconnectAttempts + Math.floor(Math.random() * 2000));
      setTimeout(()=>startMarketWsForSymbols(Array.from(marketSubscribedSymbols)), backoff);
    });
    marketWs.on("error", (err) => {
      log("marketWs error:", err && (err.message || err));
      try { marketWs.terminate(); } catch (e) {}
    });
  } catch (e) {
    log("startMarketWsForSymbols err:", e && e.message);
  }
}

async function onPriceTick(symbol, price, ts) {
  try {
    const posAll = loadPositions();
    const p = posAll[symbol];
    if (! p || !p. open || !p.entryPrice) return;
    const entry = Number(p.entryPrice);
    if (! entry || entry === 0) return;
    const side = (p.side || "").toUpperCase();
    let profitPercent = (side === "LONG") ?  ((price - entry) / entry) * 100 : ((entry - price) / entry) * 100;

    if (config.verbose) {
      log("tick", { symbol, price, profitPercent: profitPercent. toFixed(4), ts, now: Date.now() });
    }

    // [V6] 实时检查是否需要升级止损档位
    if (config.trailingStop. enabled && p.ironDefenseActive) {
        const currentTier = p.currentStopTier !== null && p.currentStopTier !== undefined ?  p.currentStopTier : -1;
        const newTier = calculateStopTier(profitPercent, currentTier);
        
        if (newTier > currentTier) {
            const stopPrice = calculateStopPrice(entry, side, profitPercent, newTier);
            if (stopPrice) {
                const posAmt = Number(p.amount || 0);
                await placeOrUpdateStopOrder(symbol, side, posAmt, stopPrice, newTier);
                
                p.currentStopTier = newTier;
                savePositions(posAll);
                log(`[TICK UPGRADE] ${symbol} -> Tier ${newTier} @ profit ${profitPercent.toFixed(2)}%`);
            }
        }
    }

    // 快速保护检查
    if (p.protection && p.protection. active && profitPercent <= (p.protection.closeToPct || config.profitMonitor.quickProtectCloseToPct)) {
      log("onPriceTick: quick-protect firing for", symbol, "profit%", profitPercent. toFixed(4));
      try {
        const exchangePos = await getPosition(symbol);
        const posAmt = exchangePos ? Math. abs(Number(exchangePos.positionAmt || 0)) : Math.abs(Number(p.amount || 0));
        if (! posAmt || posAmt <= 0) {
          await cancelStopOrder(symbol);
          performSoftClose(symbol, p.side);
          return;
        }
        const qty = lotRound(symbol, posAmt);
        if (! qty || qty <= 0) return;
        const closeSide = (p.side === "LONG") ? "SELL" : "BUY";
        await cancelStopOrder(symbol);
        const r = await placeMarketOrder(symbol, closeSide, qty, true);
        await computeAndPersistClose(symbol, r, p.side, qty);
        
        const posAfter = loadPositions();
        if (posAfter[symbol] && posAfter[symbol].open) {
            posAfter[symbol]. closeReason = `protection_quick_closed tick ${profitPercent.toFixed(4)}%`;
            posAfter[symbol].closeResult = r;
            savePositions(posAfter);
        }
      } catch (e) {
        log("onPriceTick quick-protect close err:", e && (e.message || e));
      }
    }
  } catch (e) {
    if (config.verbose) log("onPriceTick err:", e && e.message);
  }
}

let lastSignalFetch = null;
const BOT_START_ISO = new Date(). toISOString();

async function signalFetchLoop() {
  while (true) {
    const start = Date.now();
    try {
      const cutoffIso = new Date(Date.now() - (config.maxSignalAgeMinutes * 60 * 1000)). toISOString();
      if (config.verbose) log("fetchCSignals using cutoff since:", cutoffIso);
      const signals = await fetchCSignalsSince(cutoffIso);
      lastSignalFetch = new Date(). toISOString();
      log("fetched C signals count:", signals.length, "since=", cutoffIso);

      const latestBySymbol = {};
      const now = Date.now();
      const maxAgeMs = MAX_SIGNAL_AGE_MS;

      signals.forEach(s => {
        if (! s || !s.symbol) return;
        let sigTs = parseSignalTimeToMs(s.trigger_time || (s.raw && (s.raw["时间"] || s. raw. time)) || "");
        if (! sigTs || isNaN(sigTs)) {
          return;
        }
        if (! config.allowOldSignals && (now - sigTs) > maxAgeMs) {
          return;
        }
        const prev = latestBySymbol[s.symbol];
        if (! prev || sigTs > prev.ts) {
          latestBySymbol[s.symbol] = { ... s, ts: sigTs };
        }
      });

      const listAll = Object.values(latestBySymbol).sort((a,b) => b.ts - a. ts);
      if (listAll.length === 0) {
        if (config.verbose) log("no signals to process this loop");
      } else {
        const toProcess = listAll. slice(0, config.maxSignalsPerLoop);
        for (const sig of toProcess) {
          try {
            await openPositionIfNeeded(sig);
          } catch (e) {
            log("openPositionIfNeeded threw:", e. message || e);
          }
        }
      }
    } catch (e) {
      log("signal fetch loop error:", e. message || e);
    }
    const elapsed = Date.now() - start;
    const waitMs = Math.max(100, config.fetchIntervalMs - elapsed);
    await sleep(waitMs);
  }
}

async function periodicChecksLoop() {
  while (true) {
    try {
      await monitorProfitAndClose();
    } catch (e) {
      log("monitorProfitAndClose error:", e. message || e);
    }
    try {
      await checkAndCloseExpiredPositions();
    } catch (e) {
      log("checkAndCloseExpiredPositions error:", e.message || e);
    }
    try {
      await checkGlobalDrawdown();
    } catch (e) {
      log("checkGlobalDrawdown error:", e.message);
    }
    try {
      cleanOldClosedPositions();
    } catch (e) {
      log("cleanOldClosedPositions error:", e.message);
    }
    await sleep(config.checkCloseIntervalMs);
  }
}

setInterval(() => {
  const now = Date.now();
  try {
    const lastAlive = Math.max(lastUserWsMsgAt, lastUserWsHeartbeat);
    if (userWs && typeof userWs !== 'undefined' && userWs.readyState === WebSocket. OPEN && now - lastAlive > config.wsStaleTimeoutMs) {
      log("userWs seems stale (no messages/pongs), force restart");
      try { userWs. terminate(); } catch (e) {}
      userListenKey = null;
      startUserDataWs();
    }
    if (marketWs && marketWs.readyState === WebSocket. OPEN && now - lastMarketWsMsgAt > config.wsStaleTimeoutMs) {
      log("marketWs seems stale (no messages), force restart");
      try { marketWs.terminate(); } catch (e) {}
      startMarketWsForSymbols(Array.from(marketSubscribedSymbols));
    }
  } catch (e) { log("ws watchdog err:", e && e.message); }
}, Math.max(30 * 1000, Math.floor(config.wsStaleTimeoutMs / 2)));

async function mainLoop() {
  if (! fs.existsSync(HISTORY_DB)) fs.writeFileSync(HISTORY_DB, "[]");

  log("=======================================================");
  log("  BINANCE BOT V6 - IRON DEFENSE (铁壁保本版) STARTED");
  log("=======================================================");
  log("bot main loop started.  dryRun=", config.dryRun, "testnet=", config.useTestnet, "forceReal=", config.forceReal, "verbose=", config.verbose);
  log("BTC Filter:", config.btcFilter. enabled ? "ENABLED" : "DISABLED");
  log("Trailing Stop Tiers:", JSON.stringify(config. trailingStop. tiers));
  
  initialAccountBalance = await getAccountBalance();
  log(`Initial Balance: ${initialAccountBalance} USDT`);
  
  try { await getExchangeInfo(); } catch (e) { log("getExchangeInfo err:", e.message || e); }

  // 同步交易所现有仓位
  try {
    const all = await getAllPositions();
    if (Array.isArray(all) && all.length) {
      const pos = loadPositions();
      let changed = false;
      for (const ex of all) {
        try {
          const amt = Number(ex. positionAmt || 0);
          if (! amt || Math.abs(amt) < 1e-8) continue;
          const sym = String(ex.symbol || ""). toUpperCase();
          const oldPos = pos[sym] || {}; 
          
          pos[sym] = {
              ... oldPos, 
              open: true,
              side: amt > 0 ?  "LONG" : "SHORT",
              amount: Math.abs(amt),
              source: oldPos.source || "existing_exchange",
              lastSyncedFromExchangeAt: Date.now(),
              ironDefenseActive: oldPos.ironDefenseActive || false,
              currentStopTier: oldPos. currentStopTier !== undefined ? oldPos.currentStopTier : null,
              peakProfitPct: oldPos.peakProfitPct || 0
          };
          
          const exEntry = (ex.entryPrice || ex.entry_price || ex.entry || ex.avgEntryPrice || ex.entryAvgPrice) || null;
          let entryToWrite = null;
          if (exEntry && isFinite(Number(exEntry)) && Number(exEntry) !== 0) entryToWrite = Number(exEntry);

          if (! pos[sym].openedAt) pos[sym].openedAt = Date.now();
          if (! pos[sym].entryPrice && entryToWrite) {
            pos[sym].entryPrice = entryToWrite;
            log("syncExchangePositions: wrote entryPrice from exchange for", sym, entryToWrite);
            changed = true;
          }
          
        } catch (e) {}
      }
      if (changed) savePositions(pos);
    }
  } catch (e) {
    log("sync existing exchange positions failed:", e?. message || e);
  }

  // [V6] 恢复止损单
  try {
    await recoverStopOrders();
  } catch (e) {
    log("recoverStopOrders failed:", e?.message || e);
  }

  startUserDataWs(). catch(e => log("startUserDataWs error:", e && e.message));

  const currentOpen = Object.keys(loadPositions()). filter(s => loadPositions()[s].open);
  startMarketWsForSymbols(currentOpen);

  signalFetchLoop(). catch(e => log("signalFetchLoop fatal:", e && e.message));
  periodicChecksLoop().catch(e => log("periodicChecksLoop fatal:", e && e.message));
}

process.on('unhandledRejection', (r) => { log("unhandledRejection:", r && (r.stack || r)); });
process.on('uncaughtException', (err) => {
  log("uncaughtException:", err && (err.stack || err));
});

(async () => {
  if (! fs.existsSync(POS_DB)) fs.writeFileSync(POS_DB, "{}");
  try {
    await mainLoop();
  } catch (e) {
    log("Fatal error:", e. message || e);
    process.exit(1);
  }
})();