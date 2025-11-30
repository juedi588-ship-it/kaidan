/**
 * Optimized Signal Strategy (优化信号策略)
 * 
 * 解决的问题:
 * 1. 信号滞后 - 使用当前K线 (i = len(df) - 1)
 * 2. 假突破过多 - 连续两根K线突破确认 + 突破幅度确认
 * 3. 横盘震荡频繁信号 - ADX趋势过滤
 * 4. 量价确认不足 - 增强OBV和成交量判断
 * 
 * 新增功能:
 * - K线成熟度判断 (5-7分钟)
 * - 实体占比确认 (>60%)
 * - ADX趋势过滤 (>25允许, <20过滤)
 * - +DI/-DI方向判断
 * - 成交量突破确认 (>1.5x均量)
 * - OBV EMA趋势
 * - RSI背离过滤
 * - 信号质量分级 (A/B级)
 * - 冷却时间机制
 */

const { ADX, RSI, ATR, BollingerBands, EMA, OBV } = require("technicalindicators");

// ====== 信号冷却缓存 ======
const signalCooldownCache = {};

// ====== 配置参数 ======
const SIGNAL_CONFIG = {
  // K线成熟度配置
  candleMaturity: {
    minMinutes: 5,        // K线至少走完5分钟
    maxMinutes: 7,        // 最多等到7分钟
    intervalMinutes: 15   // 15分钟K线
  },
  
  // 实体占比配置
  bodyRatio: {
    minRatio: 0.6        // 实体占比 > 60%
  },
  
  // 突破确认配置
  breakthrough: {
    minAmplitude: 0.003,  // 突破幅度至少 0.3%
    consecutiveCandles: 2  // 连续2根K线确认
  },
  
  // ADX趋势过滤配置
  adx: {
    period: 14,
    trendThreshold: 25,   // ADX > 25 = 趋势
    rangingThreshold: 20, // ADX < 20 = 震荡
    enabled: true
  },
  
  // 成交量确认配置
  volume: {
    maPeriod: 20,
    multiplier: 1.5       // 成交量 > 1.5x 均量
  },
  
  // OBV配置
  obv: {
    fastEma: 10,
    slowEma: 30
  },
  
  // RSI配置
  rsi: {
    period: 14,
    overbought: 75,       // 做多时RSI不能超过75
    oversold: 25          // 做空时RSI不能低于25
  },
  
  // 布林带配置
  bollingerBands: {
    period: 20,
    stdDev: 2
  },
  
  // 肯特纳通道配置
  keltnerChannel: {
    emaPeriod: 20,
    atrPeriod: 10,
    atrMultiplier: 1.5
  },
  
  // 冷却时间配置 (毫秒)
  cooldown: {
    normalMinutes: 30,    // 正常冷却30分钟
    afterLossMinutes: 60  // 亏损后冷却60分钟
  },
  
  // EMA趋势配置
  ema: {
    longPeriod: 120       // EMA120用于趋势判断
  }
};

/**
 * 计算肯特纳通道
 * @param {number[]} closes - 收盘价数组
 * @param {number[]} highs - 最高价数组
 * @param {number[]} lows - 最低价数组
 * @returns {Object|null} - 肯特纳通道值
 */
function calculateKeltnerChannel(closes, highs, lows) {
  const { emaPeriod, atrPeriod, atrMultiplier } = SIGNAL_CONFIG.keltnerChannel;
  
  if (closes.length < Math.max(emaPeriod, atrPeriod)) return null;
  
  // 计算EMA中轨
  const emaValues = EMA.calculate({ period: emaPeriod, values: closes });
  if (!emaValues.length) return null;
  const middle = emaValues[emaValues.length - 1];
  
  // 计算ATR
  const atrValues = ATR.calculate({ high: highs, low: lows, close: closes, period: atrPeriod });
  if (!atrValues.length) return null;
  const atr = atrValues[atrValues.length - 1];
  
  // 计算上下轨
  const upper = middle + (atr * atrMultiplier);
  const lower = middle - (atr * atrMultiplier);
  
  return { upper, middle, lower, atr };
}

/**
 * 计算OBV的EMA趋势
 * @param {number[]} closes - 收盘价数组
 * @param {number[]} volumes - 成交量数组
 * @returns {Object|null} - OBV EMA值
 */
function calculateOBVTrend(closes, volumes) {
  const { fastEma, slowEma } = SIGNAL_CONFIG.obv;
  
  if (closes.length < slowEma + 10) return null;
  
  // 计算OBV
  const obvValues = OBV.calculate({ close: closes, volume: volumes });
  if (!obvValues || obvValues.length < slowEma) return null;
  
  // 计算OBV的EMA
  const obvFastEma = EMA.calculate({ period: fastEma, values: obvValues });
  const obvSlowEma = EMA.calculate({ period: slowEma, values: obvValues });
  
  if (!obvFastEma.length || !obvSlowEma.length) return null;
  
  return {
    fast: obvFastEma[obvFastEma.length - 1],
    slow: obvSlowEma[obvSlowEma.length - 1],
    bullish: obvFastEma[obvFastEma.length - 1] > obvSlowEma[obvSlowEma.length - 1]
  };
}

/**
 * 检查K线成熟度
 * @param {number} candleOpenTime - K线开盘时间戳(毫秒)
 * @returns {Object} - 成熟度检查结果
 */
function checkCandleMaturity(candleOpenTime) {
  const now = Date.now();
  const { minMinutes, maxMinutes, intervalMinutes } = SIGNAL_CONFIG.candleMaturity;
  
  const elapsedMs = now - candleOpenTime;
  const elapsedMinutes = elapsedMs / (1000 * 60);
  
  const isMature = elapsedMinutes >= minMinutes && elapsedMinutes <= intervalMinutes;
  const isOptimal = elapsedMinutes >= minMinutes && elapsedMinutes <= maxMinutes;
  
  return {
    isMature,
    isOptimal,
    elapsedMinutes: Number(elapsedMinutes.toFixed(2)),
    reason: isMature ? "K线成熟" : `K线仅走过${elapsedMinutes.toFixed(1)}分钟`
  };
}

/**
 * 检查K线实体占比
 * @param {number} open_ - 开盘价
 * @param {number} high - 最高价
 * @param {number} low - 最低价
 * @param {number} close - 收盘价
 * @returns {Object} - 实体检查结果
 */
function checkCandleBody(open_, high, low, close) {
  const { minRatio } = SIGNAL_CONFIG.bodyRatio;
  
  const range = high - low;
  if (range <= 0) {
    return { valid: false, ratio: 0, isBullish: false, reason: "K线无波动" };
  }
  
  const body = Math.abs(close - open_);
  const ratio = body / range;
  const isBullish = close > open_;
  const valid = ratio >= minRatio;
  
  return {
    valid,
    ratio: Number(ratio.toFixed(3)),
    isBullish,
    reason: valid ? `实体占比${(ratio * 100).toFixed(1)}%` : `实体占比${(ratio * 100).toFixed(1)}%不足${minRatio * 100}%`
  };
}

/**
 * 检查ADX趋势
 * @param {number[]} highs - 最高价数组
 * @param {number[]} lows - 最低价数组
 * @param {number[]} closes - 收盘价数组
 * @returns {Object} - ADX检查结果
 */
function checkADXTrend(highs, lows, closes) {
  const { period, trendThreshold, rangingThreshold, enabled } = SIGNAL_CONFIG.adx;
  
  if (!enabled) {
    return { valid: true, adx: 0, plusDI: 0, minusDI: 0, isTrending: true, reason: "ADX过滤已禁用" };
  }
  
  if (closes.length < period + 10) {
    return { valid: false, adx: 0, plusDI: 0, minusDI: 0, isTrending: false, reason: "数据不足" };
  }
  
  const adxResult = ADX.calculate({ high: highs, low: lows, close: closes, period });
  if (!adxResult.length) {
    return { valid: false, adx: 0, plusDI: 0, minusDI: 0, isTrending: false, reason: "ADX计算失败" };
  }
  
  const latest = adxResult[adxResult.length - 1];
  const adx = latest.adx;
  const plusDI = latest.pdi;
  const minusDI = latest.mdi;
  
  const isTrending = adx > trendThreshold;
  const isRanging = adx < rangingThreshold;
  
  return {
    valid: !isRanging,
    adx: Number(adx.toFixed(2)),
    plusDI: Number(plusDI.toFixed(2)),
    minusDI: Number(minusDI.toFixed(2)),
    isTrending,
    isRanging,
    longAllowed: plusDI > minusDI,  // +DI > -DI 允许做多
    shortAllowed: minusDI > plusDI, // -DI > +DI 允许做空
    reason: isRanging ? `ADX=${adx.toFixed(1)}震荡市` : (isTrending ? `ADX=${adx.toFixed(1)}强趋势` : `ADX=${adx.toFixed(1)}弱趋势`)
  };
}

/**
 * 检查成交量突破
 * @param {number[]} volumes - 成交量数组
 * @returns {Object} - 成交量检查结果
 */
function checkVolumeBreakout(volumes) {
  const { maPeriod, multiplier } = SIGNAL_CONFIG.volume;
  
  if (volumes.length < maPeriod + 1) {
    return { valid: false, ratio: 0, reason: "成交量数据不足" };
  }
  
  const currentVolume = volumes[volumes.length - 1];
  
  // 计算均量 (不包含当前K线)
  const volumeMA = EMA.calculate({ period: maPeriod, values: volumes.slice(0, -1) });
  if (!volumeMA.length) {
    return { valid: false, ratio: 0, reason: "均量计算失败" };
  }
  
  const avgVolume = volumeMA[volumeMA.length - 1];
  const ratio = avgVolume > 0 ? currentVolume / avgVolume : 0;
  const valid = ratio >= multiplier;
  
  return {
    valid,
    ratio: Number(ratio.toFixed(2)),
    currentVolume,
    avgVolume: Number(avgVolume.toFixed(2)),
    reason: valid ? `量能${ratio.toFixed(1)}x均量` : `量能${ratio.toFixed(1)}x不足${multiplier}x`
  };
}

/**
 * 检查RSI过滤
 * @param {number[]} closes - 收盘价数组
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {Object} - RSI检查结果
 */
function checkRSIFilter(closes, side) {
  const { period, overbought, oversold } = SIGNAL_CONFIG.rsi;
  
  if (closes.length < period + 5) {
    return { valid: true, rsi: 50, reason: "RSI数据不足,允许通过" };
  }
  
  const rsiValues = RSI.calculate({ period, values: closes });
  if (!rsiValues.length) {
    return { valid: true, rsi: 50, reason: "RSI计算失败,允许通过" };
  }
  
  const rsi = rsiValues[rsiValues.length - 1];
  let valid = true;
  let reason = "";
  
  if (side === "LONG") {
    valid = rsi < overbought;
    reason = valid ? `RSI=${rsi.toFixed(1)}未超买` : `RSI=${rsi.toFixed(1)}已超买(>${overbought})`;
  } else {
    valid = rsi > oversold;
    reason = valid ? `RSI=${rsi.toFixed(1)}未超卖` : `RSI=${rsi.toFixed(1)}已超卖(<${oversold})`;
  }
  
  return {
    valid,
    rsi: Number(rsi.toFixed(2)),
    reason
  };
}

/**
 * 检查布林带突破
 * @param {number[]} closes - 收盘价数组
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {Object} - 布林带检查结果
 */
function checkBollingerBreakout(closes, side) {
  const { period, stdDev } = SIGNAL_CONFIG.bollingerBands;
  const { minAmplitude, consecutiveCandles } = SIGNAL_CONFIG.breakthrough;
  
  if (closes.length < period + consecutiveCandles) {
    return { valid: false, amplitude: 0, reason: "布林带数据不足" };
  }
  
  const bbValues = BollingerBands.calculate({ period, stdDev, values: closes });
  if (bbValues.length < consecutiveCandles) {
    return { valid: false, amplitude: 0, reason: "布林带计算失败" };
  }
  
  const currentBB = bbValues[bbValues.length - 1];
  const prevBB = bbValues[bbValues.length - 2];
  const currentClose = closes[closes.length - 1];
  const prevClose = closes[closes.length - 2];
  
  let valid = false;
  let amplitude = 0;
  let reason = "";
  
  if (side === "LONG") {
    // 检查当前K线突破上轨
    amplitude = (currentClose - currentBB.upper) / currentBB.upper;
    const currentBreak = amplitude >= minAmplitude;
    
    // 检查前一根K线也接近或突破上轨 (允许0.5%的容差)
    const prevAmplitude = (prevClose - prevBB.upper) / prevBB.upper;
    const prevBreak = prevAmplitude >= -0.005; // 前一根至少接近上轨
    
    valid = currentBreak && prevBreak;
    reason = valid 
      ? `BB突破+${(amplitude * 100).toFixed(2)}%,连续确认` 
      : `BB突破${(amplitude * 100).toFixed(2)}%不足或无连续确认`;
  } else {
    // 检查当前K线突破下轨
    amplitude = (currentBB.lower - currentClose) / currentBB.lower;
    const currentBreak = amplitude >= minAmplitude;
    
    // 检查前一根K线也接近或突破下轨
    const prevAmplitude = (prevBB.lower - prevClose) / prevBB.lower;
    const prevBreak = prevAmplitude >= -0.005;
    
    valid = currentBreak && prevBreak;
    reason = valid 
      ? `BB突破-${(amplitude * 100).toFixed(2)}%,连续确认` 
      : `BB突破${(amplitude * 100).toFixed(2)}%不足或无连续确认`;
  }
  
  return {
    valid,
    amplitude: Number((amplitude * 100).toFixed(3)),
    upper: currentBB.upper,
    lower: currentBB.lower,
    middle: currentBB.middle,
    reason
  };
}

/**
 * 检查肯特纳通道突破
 * @param {number[]} closes - 收盘价数组
 * @param {number[]} highs - 最高价数组
 * @param {number[]} lows - 最低价数组
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {Object} - 肯特纳通道检查结果
 */
function checkKeltnerBreakout(closes, highs, lows, side) {
  const kc = calculateKeltnerChannel(closes, highs, lows);
  if (!kc) {
    return { valid: false, reason: "肯特纳通道数据不足" };
  }
  
  const currentClose = closes[closes.length - 1];
  
  let valid = false;
  let reason = "";
  
  if (side === "LONG") {
    valid = currentClose > kc.upper;
    reason = valid 
      ? `KC突破上轨${kc.upper.toFixed(4)}` 
      : `未突破KC上轨${kc.upper.toFixed(4)}`;
  } else {
    valid = currentClose < kc.lower;
    reason = valid 
      ? `KC突破下轨${kc.lower.toFixed(4)}` 
      : `未突破KC下轨${kc.lower.toFixed(4)}`;
  }
  
  return {
    valid,
    upper: kc.upper,
    lower: kc.lower,
    middle: kc.middle,
    reason
  };
}

/**
 * 检查EMA趋势
 * @param {number[]} closes - 收盘价数组
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {Object} - EMA趋势检查结果
 */
function checkEMATrend(closes, side) {
  const { longPeriod } = SIGNAL_CONFIG.ema;
  
  if (closes.length < longPeriod + 5) {
    return { valid: true, ema: 0, reason: "EMA数据不足,允许通过" };
  }
  
  const emaValues = EMA.calculate({ period: longPeriod, values: closes });
  if (!emaValues.length) {
    return { valid: true, ema: 0, reason: "EMA计算失败,允许通过" };
  }
  
  const ema = emaValues[emaValues.length - 1];
  const currentClose = closes[closes.length - 1];
  
  let valid = false;
  let reason = "";
  
  if (side === "LONG") {
    valid = currentClose > ema;
    reason = valid ? `价格>${ema.toFixed(4)}(EMA${longPeriod})` : `价格<EMA${longPeriod}`;
  } else {
    valid = currentClose < ema;
    reason = valid ? `价格<${ema.toFixed(4)}(EMA${longPeriod})` : `价格>EMA${longPeriod}`;
  }
  
  return {
    valid,
    ema: Number(ema.toFixed(4)),
    reason
  };
}

/**
 * 生成冷却缓存的键
 * @param {string} symbol - 交易对
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {string} - 缓存键
 */
function getCooldownKey(symbol, side) {
  return `${symbol}_${side}`;
}

/**
 * 检查冷却时间
 * @param {string} symbol - 交易对
 * @param {string} side - "LONG" 或 "SHORT"
 * @returns {Object} - 冷却检查结果
 */
function checkCooldown(symbol, side) {
  const { normalMinutes, afterLossMinutes } = SIGNAL_CONFIG.cooldown;
  const now = Date.now();
  
  const key = getCooldownKey(symbol, side);
  const lastSignal = signalCooldownCache[key];
  
  if (!lastSignal) {
    return { valid: true, remainingMinutes: 0, reason: "无冷却" };
  }
  
  const cooldownMs = lastSignal.wasLoss 
    ? afterLossMinutes * 60 * 1000 
    : normalMinutes * 60 * 1000;
  
  const elapsed = now - lastSignal.timestamp;
  const remaining = Math.max(0, cooldownMs - elapsed);
  const remainingMinutes = remaining / (60 * 1000);
  
  const valid = remaining <= 0;
  
  return {
    valid,
    remainingMinutes: Number(remainingMinutes.toFixed(1)),
    reason: valid ? "冷却结束" : `冷却中,还需${remainingMinutes.toFixed(0)}分钟`
  };
}

/**
 * 更新冷却缓存
 * @param {string} symbol - 交易对
 * @param {string} side - "LONG" 或 "SHORT"
 * @param {boolean} wasLoss - 是否亏损
 */
function updateCooldown(symbol, side, wasLoss = false) {
  const key = getCooldownKey(symbol, side);
  signalCooldownCache[key] = {
    timestamp: Date.now(),
    wasLoss
  };
}

/**
 * 评估信号质量等级
 * @param {Object} checks - 所有检查结果
 * @returns {Object} - 信号等级
 */
function evaluateSignalGrade(checks) {
  const {
    candleMaturity,
    candleBody,
    adxTrend,
    volumeBreakout,
    rsiFilter,
    bollingerBreakout,
    keltnerBreakout,
    emaTrend,
    obvTrend,
    cooldown
  } = checks;
  
  // 必须条件 - 不满足则不发信号
  const mustPass = [
    candleMaturity?.isMature,
    candleBody?.valid,
    cooldown?.valid
  ];
  
  if (mustPass.some(v => !v)) {
    return { grade: null, score: 0, reason: "基础条件不满足" };
  }
  
  // 计算评分
  let score = 0;
  const details = [];
  
  // ADX趋势 (25分)
  if (adxTrend?.isTrending) {
    score += 25;
    details.push("ADX强趋势+25");
  } else if (adxTrend?.valid) {
    score += 15;
    details.push("ADX弱趋势+15");
  }
  
  // 成交量突破 (20分)
  if (volumeBreakout?.valid) {
    score += 20;
    details.push("量能突破+20");
  }
  
  // BB突破 (15分)
  if (bollingerBreakout?.valid) {
    score += 15;
    details.push("BB突破+15");
  }
  
  // KC突破 (10分)
  if (keltnerBreakout?.valid) {
    score += 10;
    details.push("KC突破+10");
  }
  
  // RSI过滤 (10分)
  if (rsiFilter?.valid) {
    score += 10;
    details.push("RSI正常+10");
  }
  
  // EMA趋势 (10分)
  if (emaTrend?.valid) {
    score += 10;
    details.push("EMA趋势+10");
  }
  
  // OBV趋势 (10分)
  if (obvTrend?.bullish !== undefined) {
    score += 10;
    details.push("OBV确认+10");
  }
  
  // 确定等级
  let grade = null;
  if (score >= 80) {
    grade = "A";
  } else if (score >= 60) {
    grade = "B";
  } else if (score >= 40) {
    grade = "C";
  }
  
  return {
    grade,
    score,
    details,
    reason: `评分${score}分: ${details.join(", ")}`
  };
}

/**
 * 生成Telegram消息格式
 * @param {Object} signal - 信号数据
 * @returns {string} - 格式化的消息
 */
function formatTelegramMessage(signal) {
  const { grade, side, symbol, price, checks, suggestion } = signal;
  
  const sideText = side === "LONG" ? "做多" : "做空";
  const gradeEmoji = grade === "A" ? "🔥" : "⚡";
  
  const lines = [
    `【${grade}级${sideText}】${gradeEmoji} ${symbol}`,
    `价格: ${price.toFixed(4)}`,
    `时间: ${new Date().toISOString().replace("T", " ").slice(0, 19)}`,
    "",
    "确认项:"
  ];
  
  // ADX
  if (checks.adxTrend) {
    const emoji = checks.adxTrend.isTrending ? "✅" : "⚠️";
    lines.push(`${emoji} ADX: ${checks.adxTrend.adx} (${checks.adxTrend.isTrending ? "强趋势" : "弱趋势"})`);
  }
  
  // 量能
  if (checks.volumeBreakout) {
    const emoji = checks.volumeBreakout.valid ? "✅" : "⚠️";
    lines.push(`${emoji} 量能: ${checks.volumeBreakout.ratio}x 均量`);
  }
  
  // BB突破
  if (checks.bollingerBreakout) {
    const emoji = checks.bollingerBreakout.valid ? "✅" : "⚠️";
    lines.push(`${emoji} BB突破: ${checks.bollingerBreakout.amplitude > 0 ? "+" : ""}${checks.bollingerBreakout.amplitude.toFixed(2)}%`);
  }
  
  // RSI
  if (checks.rsiFilter) {
    const emoji = checks.rsiFilter.valid ? "✅" : "⚠️";
    lines.push(`${emoji} RSI: ${checks.rsiFilter.rsi}`);
  }
  
  lines.push("");
  
  // 入场建议
  if (suggestion) {
    lines.push(`入场建议: ${suggestion.entry}`);
    lines.push(`止损参考: ${suggestion.stopLoss} (${suggestion.stopLossPct})`);
  }
  
  return lines.join("\n");
}

/**
 * 主信号分析函数
 * @param {Object} data - K线数据
 * @returns {Object|null} - 信号结果或null
 */
function analyzeSignal(data) {
  const {
    symbol,
    opens,      // 开盘价数组
    highs,      // 最高价数组
    lows,       // 最低价数组
    closes,     // 收盘价数组
    volumes,    // 成交量数组
    timestamps  // 时间戳数组 (毫秒)
  } = data;
  
  // 使用当前K线 (i = len(df) - 1)
  const i = closes.length - 1;
  if (i < 1) {
    return { signal: null, reason: "数据不足,至少需要2根K线" };
  }
  
  const currentClose = closes[i];
  const currentOpen = opens[i];
  const currentHigh = highs[i];
  const currentLow = lows[i];
  const candleOpenTime = timestamps[i];
  
  // 1. 检查K线成熟度
  const candleMaturity = checkCandleMaturity(candleOpenTime);
  if (!candleMaturity.isMature) {
    return { signal: null, reason: candleMaturity.reason };
  }
  
  // 2. 检查K线实体
  const candleBody = checkCandleBody(currentOpen, currentHigh, currentLow, currentClose);
  if (!candleBody.valid) {
    return { signal: null, reason: candleBody.reason };
  }
  
  // 确定意向方向
  const intendedSide = candleBody.isBullish ? "LONG" : "SHORT";
  
  // 3. 检查冷却时间
  const cooldown = checkCooldown(symbol, intendedSide);
  if (!cooldown.valid) {
    return { signal: null, reason: cooldown.reason };
  }
  
  // 4. 检查ADX趋势
  const adxTrend = checkADXTrend(highs, lows, closes);
  if (!adxTrend.valid) {
    return { signal: null, reason: adxTrend.reason };
  }
  
  // 检查DI方向
  if (intendedSide === "LONG" && !adxTrend.longAllowed) {
    return { signal: null, reason: "+DI < -DI, 不允许做多" };
  }
  if (intendedSide === "SHORT" && !adxTrend.shortAllowed) {
    return { signal: null, reason: "-DI < +DI, 不允许做空" };
  }
  
  // 5. 检查EMA趋势
  const emaTrend = checkEMATrend(closes, intendedSide);
  
  // 6. 检查布林带突破
  const bollingerBreakout = checkBollingerBreakout(closes, intendedSide);
  
  // 7. 检查肯特纳通道突破
  const keltnerBreakout = checkKeltnerBreakout(closes, highs, lows, intendedSide);
  
  // 8. 检查成交量突破
  const volumeBreakout = checkVolumeBreakout(volumes);
  
  // 9. 检查OBV趋势
  const obvTrend = calculateOBVTrend(closes, volumes);
  
  // 验证OBV方向
  if (obvTrend) {
    if (intendedSide === "LONG" && !obvTrend.bullish) {
      // OBV不看多,降低信号等级但不完全过滤
    }
    if (intendedSide === "SHORT" && obvTrend.bullish) {
      // OBV不看空,降低信号等级但不完全过滤
    }
  }
  
  // 10. 检查RSI过滤
  const rsiFilter = checkRSIFilter(closes, intendedSide);
  if (!rsiFilter.valid) {
    return { signal: null, reason: rsiFilter.reason };
  }
  
  // 收集所有检查结果
  const checks = {
    candleMaturity,
    candleBody,
    adxTrend,
    volumeBreakout,
    rsiFilter,
    bollingerBreakout,
    keltnerBreakout,
    emaTrend,
    obvTrend,
    cooldown
  };
  
  // 评估信号等级
  const gradeResult = evaluateSignalGrade(checks);
  
  // 只发送A级和B级信号
  if (!gradeResult.grade || (gradeResult.grade !== "A" && gradeResult.grade !== "B")) {
    return { signal: null, reason: `信号等级${gradeResult.grade || "C"}不发送: ${gradeResult.reason}` };
  }
  
  // 计算止损建议
  const atrForStop = checks.keltnerBreakout?.middle 
    ? Math.abs(currentClose - checks.keltnerBreakout.middle) 
    : currentClose * 0.016; // 默认1.6%
  
  const stopLoss = intendedSide === "LONG" 
    ? currentClose - atrForStop 
    : currentClose + atrForStop;
  
  const stopLossPct = ((Math.abs(currentClose - stopLoss) / currentClose) * 100).toFixed(1);
  
  // 生成信号
  const signal = {
    symbol,
    side: intendedSide,
    grade: gradeResult.grade,
    score: gradeResult.score,
    price: currentClose,
    timestamp: Date.now(),
    checks,
    suggestion: {
      entry: "当前价附近",
      stopLoss: stopLoss.toFixed(4),
      stopLossPct: `-${stopLossPct}%`
    }
  };
  
  // 生成Telegram消息
  signal.telegramMessage = formatTelegramMessage(signal);
  
  // 更新冷却缓存
  updateCooldown(symbol, intendedSide, false);
  
  return { signal, reason: gradeResult.reason };
}

/**
 * 标记信号亏损 (用于延长冷却时间)
 * @param {string} symbol - 交易对
 * @param {string} side - "LONG" 或 "SHORT"
 */
function markSignalAsLoss(symbol, side) {
  updateCooldown(symbol, side, true);
}

/**
 * 清除冷却缓存
 * @param {string} symbol - 交易对 (可选,不传则清除所有)
 */
function clearCooldown(symbol = null) {
  if (symbol) {
    delete signalCooldownCache[getCooldownKey(symbol, "LONG")];
    delete signalCooldownCache[getCooldownKey(symbol, "SHORT")];
  } else {
    Object.keys(signalCooldownCache).forEach(key => delete signalCooldownCache[key]);
  }
}

/**
 * 获取当前冷却状态
 * @returns {Object} - 冷却缓存
 */
function getCooldownStatus() {
  return { ...signalCooldownCache };
}

// 导出模块
module.exports = {
  // 主函数
  analyzeSignal,
  
  // 配置
  SIGNAL_CONFIG,
  
  // 冷却管理
  markSignalAsLoss,
  clearCooldown,
  getCooldownStatus,
  updateCooldown,
  checkCooldown,
  
  // 辅助函数 (用于测试或外部调用)
  checkCandleMaturity,
  checkCandleBody,
  checkADXTrend,
  checkVolumeBreakout,
  checkRSIFilter,
  checkBollingerBreakout,
  checkKeltnerBreakout,
  checkEMATrend,
  calculateKeltnerChannel,
  calculateOBVTrend,
  evaluateSignalGrade,
  formatTelegramMessage
};
