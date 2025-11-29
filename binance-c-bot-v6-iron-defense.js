/**
 * INSTITUTIONAL-GRADE BINANCE BOT (V6 IRON DEFENSE - 铁壁保本版)
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
const HISTORY_DB = path.resolve(__dirname, "history.json");

const ENV_FORCE_REAL = process.env.FORCE_REAL === "true";
const ENV_VERBOSE = process.env.VERBOSE === "true";
const ENV_ALLOW_OLD_SIGNALS = process.env.ALLOW_OLD_SIGNALS === "true";
const ENV_INSTITUTIONAL_FILTER = process.env.INSTITUTIONAL_FILTER !== "false";

const config = {
  apiKey: process.env.BINANCE_API_KEY || "",
  apiSecret: process.env.BINANCE_API_SECRET || "",
  useTestnet: process.env.BINANCE_TESTNET === "true" || false,
  aSignalsUrl: process.env.A_SIGNALS_URL || "http://43.156.96.140/api/a_signals",
  cSignalsUrl: process.env.C_SIGNALS_URL || "http://43.156.96.140/api/c_signals",
  extraSignalsUrl: process.env.EXTRA_SIGNALS_URL || "http://43.156.96.140:8082/api/signals",
  fetchIntervalMs: Number(process.env.FETCH_INTERVAL_MS) || 60000,
  checkCloseIntervalMs: 15000,
  dryRun: (process.env.DRY_RUN === "true") && !ENV_FORCE_REAL,
  forceReal: ENV_FORCE_REAL,
  verbose: ENV_VERBOSE,
  allowOldSignals: ENV_ALLOW_OLD_SIGNALS,
  institutionalFilter: ENV_INSTITUTIONAL_FILTER,
  usdtPerTrade: Number(process.env.USDT_PER_TRADE) || 10,
  usdtPerTradeIsMargin: process.env.USDT_PER_TRADE_IS_MARGIN === "true" || false,
  defaultLeverage: Number(process.env.DEFAULT_LEVERAGE) || 10,
  maxRetries: Number(process.env.MAX_RETRIES) || 3,
  stopLossPercent: 2.5,
  maxHoldTimeMinutes: 60,
  logFile: path.resolve(__dirname, process.env.LOG_FILE || "bot.log"),
  maxSignalAgeMinutes: 15,
  reopenCooldownMinutes: Number(process.env.REOPEN_COOLDOWN_MINUTES || 0),
  limitOrderTimeoutSeconds: 5,
  maxDailyDrawdownPct: 20.0,
  btcFilter: {
    enabled: process.env.BTC_FILTER_ENABLED !== "false",
    symbol: "BTCUSDT",
    emaPeriod: 20,
    interval: "15m",
    cacheTtlMs: 60000,
    useMultiTimeframe: true
  },
  trailingStop: {
    enabled: true,
    tiers: [
      { triggerPct: 0.6, stopPct: 0.3 },
      { triggerPct: 1.5, stopPct: 1.0 },
      { triggerPct: 3.0, stopPct: 2.0 },
      { triggerPct: 5.0, stopPct: null, trailing: 2.0 }
    ],
    tierBuffer: 0.1
  },
  volatilitySizing: {
    enabled: process.env.VOLATILITY_SIZING_ENABLED !== "false",
    targetVolatility: Number(process.env.TARGET_VOLATILITY) || 2.0,
    minScale: 0.5,
    maxScale: 2.0,
    maxUsdtCap: Number(process.env.MAX_USDT_CAP) || 500
  },
  adaptive: {
    enabled: process.env.ADAPTIVE_EXIT_ENABLED !== "false",
    choppyAdxThreshold: 30,
    choppyTakeProfit: 1.2,
    rsiOverbought: 75,
    rsiOversold: 25,
    zombieHours: Number(process.env.ZOMBIE_HOURS) || 4,
    zombieProfit: Number(process.env.ZOMBIE_PROFIT_THRESHOLD) || 0.5
  },
  profitMonitor: {
    enterPct: 4,
    tierUpgradePct: 7,
    tierA_closeToPct: 2.5,
    tierB_drawdownPct: 40,
    quickProtectEnterPct: 1.2,
    quickProtectCloseToPct: 0.3
  },
  partialCloseEnabled: true,
  partialClosePct: 1.5,
  reqMinIntervalMs: Number(process.env.REQ_MIN_INTERVAL_MS || 120),
  reqPerMinuteLimit: Number(process.env.REQ_PER_MINUTE_LIMIT || 110),
  marketStreamType: (process.env.MARKET_STREAM_TYPE || "trade").toLowerCase(),
  signalBlacklist: (process.env.SIGNAL_BLACKLIST || "").split(/\s*,\s*/).filter(Boolean),
  maxSignalsPerLoop: Number(process.env.MAX_SIGNALS_PER_LOOP || 50),
  wsStaleTimeoutMs: Number(process.env.WS_STALE_TIMEOUT_MS || 300000),
  rateStatsIntervalMs: Number(process.env.RATE_STATS_INTERVAL_MS || 30000),
  atrStopMultiplier: 2.0,
  chandelierExitMultiplier: 1.8
};

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
let exchangeInfoCache = null;
let marketWs = null;
let marketSubscribedSymbols = new Set();
let btcDirectionCache = { direction: null, timestamp: 0, details: {} };
const stopOrderCache = {};

const MAX_SIGNAL_AGE_MS = config.maxSignalAgeMinutes * 60 * 1000;
const REOPEN_COOLDOWN_MS = config.reopenCooldownMinutes * 60 * 1000;

const API_BASE = config.useTestnet
  ? (process.env.TESTNET_API_BASE || "https://testnet.binancefuture.com")
  : "https://fapi.binance.com";
const USERDATA_WS_HOST = config.useTestnet
  ? "wss://stream.binancefuture.com"
  : "wss://fstream.binance.com";

function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.map(a => typeof a === "string" ? a : JSON.stringify(a)).join(" ")}`;
  console.log(line);
  try { fs.appendFileSync(config.logFile, line + "\n"); } catch (e) {}
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function nowMs() { return Date.now(); }

function parseSignalTimeToMs(t) {
  if (t === undefined || t === null || t === "") return NaN;
  if (typeof t === "number") {
    if (t > 1e12) return t;
    if (t > 1e9) return t * 1000;
    return NaN;
  }
  const s = String(t).trim();
  if (/^\d+$/.test(s)) {
    if (s.length >= 13) return Number(s);
    if (s.length === 10) return Number(s) * 1000;
  }
  let ms = Date.parse(s);
  if (!isNaN(ms)) return ms;
  ms = Date.parse(s.replace(/-/g, "/"));
  if (!isNaN(ms)) return ms;
  return NaN;
}

function loadPositions() {
  try {
    if (!fs.existsSync(POS_DB)) return {};
    return JSON.parse(fs.readFileSync(POS_DB, "utf8") || "{}");
  } catch (e) {
    log("loadPositions error:", e.message || e);
    return {};
  }
}

function savePositions(obj) {
  try {
    const data = JSON.stringify(obj, null, 2);
    const dir = path.dirname(POS_DB);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(POS_DB, data, { encoding: "utf8", flag: "w" });
  } catch (e) {
    log("savePositions error:", e.message || e);
  }
}

function appendToHistory(positionData) {
  try {
    if (!positionData.qty && !positionData.amount) return;
    let history = [];
    if (fs.existsSync(HISTORY_DB)) {
      try {
        history = JSON.parse(fs.readFileSync(HISTORY_DB, "utf8")) || [];
        if (!Array.isArray(history)) history = [];
      } catch (e) { history = []; }
    }
    const uniqueId = positionData.id || `${positionData.symbol}_${positionData.openedAt}_${positionData.closedAt || Date.now()}`;
    const existingIndex = history.findIndex(h => (h.id || `${h.symbol}_${h.openedAt}_${h.closedAt}`) === uniqueId);
    const recordToSave = { ...positionData, id: uniqueId };
    if (existingIndex >= 0) history[existingIndex] = recordToSave;
    else {
      history.push(recordToSave);
      log(`[HISTORY] Archived record for ${positionData.symbol} PnL: ${positionData.realizedProfit}`);
    }
    if (history.length > 2000) history = history.slice(-2000);
    fs.writeFileSync(HISTORY_DB, JSON.stringify(history, null, 2));
  } catch (e) {
    log("[HISTORY] Failed to save:", e.message);
  }
}

function sanitizeSymbol(raw) {
  if (!raw) return "";
  return String(raw).toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function signQuery(paramsStr) {
  return crypto.createHmac("sha256", config.apiSecret).update(paramsStr).digest("hex");
}

const SIGNAL_BLACKLIST_SET = new Set((config.signalBlacklist || []).map(s => sanitizeSymbol(s)).filter(Boolean));
function isBlacklisted(symbol) {
  return symbol ? SIGNAL_BLACKLIST_SET.has(sanitizeSymbol(symbol)) : false;
}

const axiosInstance = axios.create({
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
      await sleep(waitMs);
      continue;
    }
    const job = requestQueue.shift();
    try {
      const start = Date.now();
      const res = await job.fn();
      recentSignedRequests.push(Date.now());
      const took = Date.now() - start;
      const waitAfter = Math.max(0, config.reqMinIntervalMs - took);
      if (waitAfter > 0) await sleep(waitAfter);
      job.resolve(res);
    } catch (e) {
      const status = e?.response?.status;
      if (status === 429 || status === 418) {
        const backoffMs = 1000 + Math.floor(Math.random() * 500);
        await sleep(backoffMs);
        requestQueue.push(job);
      } else {
        job.reject(e);
      }
    }
  }
  processingQueue = false;
}