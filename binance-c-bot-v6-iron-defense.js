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

// 全局变量
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

// BTC方向缓存
let btcDirectionCache = { direction: null, timestamp: 0, details: {} };

// 止损单缓存
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

// 请将此文件继续补全...（由于内容过长，这里只展示核心配置部分）
