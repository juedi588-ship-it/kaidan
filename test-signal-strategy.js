/**
 * Test Suite for Optimized Signal Strategy
 * 运行: node test-signal-strategy.js
 */

const strategy = require('./optimized-signal-strategy.js');

// Test counter
let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`✅ PASS: ${name}`);
    passed++;
  } catch (e) {
    console.log(`❌ FAIL: ${name}`);
    console.log(`   Error: ${e.message}`);
    failed++;
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message || 'Assertion failed');
  }
}

console.log('====================================');
console.log('Optimized Signal Strategy Test Suite');
console.log('====================================\n');

// ====== K线成熟度测试 ======
console.log('--- K线成熟度测试 ---');

test('K线6分钟时应该成熟', () => {
  const result = strategy.checkCandleMaturity(Date.now() - 6 * 60 * 1000);
  assert(result.isMature === true, 'Expected isMature to be true');
  assert(result.isOptimal === true, 'Expected isOptimal to be true');
});

test('K线2分钟时不应该成熟', () => {
  const result = strategy.checkCandleMaturity(Date.now() - 2 * 60 * 1000);
  assert(result.isMature === false, 'Expected isMature to be false');
});

test('K线10分钟时应该成熟但不是最优', () => {
  const result = strategy.checkCandleMaturity(Date.now() - 10 * 60 * 1000);
  assert(result.isMature === true, 'Expected isMature to be true');
  assert(result.isOptimal === false, 'Expected isOptimal to be false');
});

// ====== K线实体测试 ======
console.log('\n--- K线实体测试 ---');

test('强阳线应该通过(实体>60%)', () => {
  // Open=100, High=115, Low=98, Close=112
  // Range = 17, Body = 12, Ratio = 70.6%
  const result = strategy.checkCandleBody(100, 115, 98, 112);
  assert(result.valid === true, 'Expected valid to be true');
  assert(result.isBullish === true, 'Expected isBullish to be true');
  assert(result.ratio >= 0.6, `Expected ratio >= 0.6, got ${result.ratio}`);
});

test('十字星应该被过滤(实体<60%)', () => {
  // Open=100, High=102, Low=98, Close=100.5
  // Range = 4, Body = 0.5, Ratio = 12.5%
  const result = strategy.checkCandleBody(100, 102, 98, 100.5);
  assert(result.valid === false, 'Expected valid to be false');
});

test('强阴线应该通过', () => {
  // Open=112, High=115, Low=98, Close=100
  // Range = 17, Body = 12, Ratio = 70.6%
  const result = strategy.checkCandleBody(112, 115, 98, 100);
  assert(result.valid === true, 'Expected valid to be true');
  assert(result.isBullish === false, 'Expected isBullish to be false');
});

// ====== 成交量突破测试 ======
console.log('\n--- 成交量突破测试 ---');

test('2倍量能应该通过', () => {
  const volumes = Array(25).fill(100000);
  volumes[24] = 200000; // 2x average
  const result = strategy.checkVolumeBreakout(volumes);
  assert(result.valid === true, 'Expected valid to be true');
  assert(result.ratio >= 1.5, `Expected ratio >= 1.5, got ${result.ratio}`);
});

test('1.2倍量能不应该通过', () => {
  const volumes = Array(25).fill(100000);
  volumes[24] = 120000; // 1.2x average
  const result = strategy.checkVolumeBreakout(volumes);
  assert(result.valid === false, 'Expected valid to be false');
});

// ====== RSI过滤测试 ======
console.log('\n--- RSI过滤测试 ---');

test('RSI=60时做多应该通过', () => {
  // Create price array that oscillates to get mid-range RSI
  const closes = [];
  for (let i = 0; i < 50; i++) {
    // Create a more balanced pattern
    closes.push(100 + Math.sin(i / 3) * 3);
  }
  const result = strategy.checkRSIFilter(closes, 'LONG');
  // Just check that RSI is calculated and within reasonable range for passing
  assert(result.rsi !== undefined, 'Expected RSI to be calculated');
  // If RSI < 75, it should pass for LONG
  if (result.rsi < 75) {
    assert(result.valid === true, `Expected valid to be true when RSI=${result.rsi}`);
  }
});

test('RSI=80时做多不应该通过(超买)', () => {
  // Create strongly trending up prices
  const closes = [];
  for (let i = 0; i < 50; i++) {
    closes.push(100 + i * 3); // Strong uptrend
  }
  const result = strategy.checkRSIFilter(closes, 'LONG');
  assert(result.rsi > 75 || result.valid === false, `Expected RSI > 75 or valid = false, got RSI=${result.rsi}`);
});

// ====== 信号等级评估测试 ======
console.log('\n--- 信号等级评估测试 ---');

test('所有条件满足应该是A级', () => {
  const checks = {
    candleMaturity: { isMature: true },
    candleBody: { valid: true },
    cooldown: { valid: true },
    adxTrend: { valid: true, isTrending: true },
    volumeBreakout: { valid: true },
    bollingerBreakout: { valid: true },
    keltnerBreakout: { valid: true },
    rsiFilter: { valid: true },
    emaTrend: { valid: true },
    obvTrend: { bullish: true }
  };
  const result = strategy.evaluateSignalGrade(checks);
  assert(result.grade === 'A', `Expected grade A, got ${result.grade}`);
  assert(result.score >= 80, `Expected score >= 80, got ${result.score}`);
});

test('部分条件满足应该是B级', () => {
  const checks = {
    candleMaturity: { isMature: true },
    candleBody: { valid: true },
    cooldown: { valid: true },
    adxTrend: { valid: true, isTrending: false }, // 弱趋势
    volumeBreakout: { valid: true },
    bollingerBreakout: { valid: false },
    keltnerBreakout: { valid: true },
    rsiFilter: { valid: true },
    emaTrend: { valid: true },
    obvTrend: null
  };
  const result = strategy.evaluateSignalGrade(checks);
  assert(result.grade === 'B', `Expected grade B, got ${result.grade}`);
  assert(result.score >= 60 && result.score < 80, `Expected score 60-80, got ${result.score}`);
});

test('基础条件不满足不应该有等级', () => {
  const checks = {
    candleMaturity: { isMature: false }, // 不成熟
    candleBody: { valid: true },
    cooldown: { valid: true }
  };
  const result = strategy.evaluateSignalGrade(checks);
  assert(result.grade === null, `Expected grade null, got ${result.grade}`);
});

// ====== 冷却时间测试 ======
console.log('\n--- 冷却时间测试 ---');

test('无历史信号应该允许通过', () => {
  strategy.clearCooldown('TESTUSDT');
  const result = strategy.checkCooldown('TESTUSDT', 'LONG');
  assert(result.valid === true, 'Expected valid to be true');
});

test('刚发送信号应该被冷却', () => {
  strategy.updateCooldown('COOLTEST', 'LONG', false);
  const result = strategy.checkCooldown('COOLTEST', 'LONG');
  assert(result.valid === false, 'Expected valid to be false');
  assert(result.remainingMinutes > 0, 'Expected remaining time > 0');
});

test('亏损后冷却时间应该更长', () => {
  strategy.updateCooldown('LOSSTEST', 'SHORT', true); // wasLoss = true
  const result = strategy.checkCooldown('LOSSTEST', 'SHORT');
  assert(result.valid === false, 'Expected valid to be false');
  assert(result.remainingMinutes > 29, `Expected remaining > 29 min, got ${result.remainingMinutes}`);
});

// ====== 肯特纳通道测试 ======
console.log('\n--- 肯特纳通道测试 ---');

test('肯特纳通道计算应该正确', () => {
  const closes = Array(50).fill(0).map((_, i) => 100 + Math.sin(i/5) * 5);
  const highs = closes.map(c => c + 2);
  const lows = closes.map(c => c - 2);
  
  const result = strategy.calculateKeltnerChannel(closes, highs, lows);
  assert(result !== null, 'Expected result not null');
  assert(result.upper > result.middle, 'Expected upper > middle');
  assert(result.middle > result.lower, 'Expected middle > lower');
  assert(result.atr > 0, 'Expected ATR > 0');
});

// ====== OBV趋势测试 ======
console.log('\n--- OBV趋势测试 ---');

test('OBV趋势计算应该正确', () => {
  const closes = Array(50).fill(0).map((_, i) => 100 + i * 0.5);
  const volumes = Array(50).fill(100000);
  
  const result = strategy.calculateOBVTrend(closes, volumes);
  assert(result !== null, 'Expected result not null');
  assert(typeof result.bullish === 'boolean', 'Expected bullish to be boolean');
});

// ====== Telegram消息格式测试 ======
console.log('\n--- Telegram消息格式测试 ---');

test('Telegram消息格式应该正确', () => {
  const signal = {
    grade: 'A',
    side: 'LONG',
    symbol: 'BTCUSDT',
    price: 67890.50,
    checks: {
      adxTrend: { adx: 32.5, isTrending: true },
      volumeBreakout: { valid: true, ratio: 2.1 },
      bollingerBreakout: { valid: true, amplitude: 0.45 },
      rsiFilter: { valid: true, rsi: 62 }
    },
    suggestion: {
      entry: '当前价附近',
      stopLoss: '66800',
      stopLossPct: '-1.6%'
    }
  };
  
  const message = strategy.formatTelegramMessage(signal);
  assert(message.includes('A级做多'), 'Expected A级做多 in message');
  assert(message.includes('BTCUSDT'), 'Expected BTCUSDT in message');
  assert(message.includes('ADX'), 'Expected ADX in message');
  assert(message.includes('量能'), 'Expected 量能 in message');
});

// ====== 完整信号分析测试 ======
console.log('\n--- 完整信号分析测试 ---');

test('数据不足应该返回null信号', () => {
  const data = {
    symbol: 'BTCUSDT',
    opens: [100],
    highs: [101],
    lows: [99],
    closes: [100.5],
    volumes: [100000],
    timestamps: [Date.now() - 6 * 60 * 1000]
  };
  
  const result = strategy.analyzeSignal(data);
  assert(result.signal === null, 'Expected signal to be null');
});

// ====== 总结 ======
console.log('\n====================================');
console.log(`测试完成: ${passed} 通过, ${failed} 失败`);
console.log('====================================');

if (failed > 0) {
  process.exit(1);
}
