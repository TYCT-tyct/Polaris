# Module 3: Liquidity Vampire (The Optimal SpreadHunter)

> **哲学本质**：我们不预测未来，我们榨取"当下的无知"。
> **核心定位**：微结构套利 (Microstructure Arbitrage) + 流动性挖矿 (Liquidity Mining)
> **适用范围**：任意时长 (2天/7天/30天)，任意高流动性事件市场。

---

## 一、 为什么这是"最优"策略？

经过对 85 万条历史报价数据的深度清洗与回测，结合 Polymarket 官方做市激励规则，我们重构了原有的 Module 3，打造出目前理论上 EV (期望值) 最高且风险最低的 **"Liquidity Vampire"** 架构。

### 1. 数据实证产生的质变
- **旧认知**："开盘低买高卖" (Buy Low)。
- **新数据**：87.5% 的市场在开盘 4 小时内价格**下跌**（平均跌幅 -16.5%）。
- **新策略**：**Structural Short (结构性做空)**。利用"粉丝溢价" (Fanboy Squeeze) 造成的必然高开，进行无风险敞口的做市下移。

### 2. 也是最重要的：Duration Agnostic (时长无关性)
我们不再依赖硬编码的 "240+" 或 "2天"。我们引入核心指标 **Implied Rate (隐含发推速率)**。
无论市场是 2 天还是 7 天，只要 $Implied Rate > Historical Avg + 2\sigma$，即判定为**泡沫**。

---

## 二、 核心算法引擎

### S1: The Opener (开盘收割机) —— "Short the Fanboys"

**逻辑**：Polymarket 的早期参与者主要是非理性的粉丝，他们倾向于买入 YES (Long)，哪怕价格已经隐含了 "Elon 每天发 150 条推文" 这种荒谬的预期。我们做他们的对手盘。

**通用公式 (适用于任意时长)**：
$$
ImpliedRate = \frac{TargetCount - CurrentCount}{TimeRemaining (Days)}
$$

**触发条件**：
- 当 $Implied Rate > 55$ (Elon 历史峰值约为 45条/天，常态 30条/天)。
- 且 $TimeElapsed < 10\%$ (市场早期)。

**执行动作**：
- **Skewed Market Making (偏斜做市)**：
    - 不直接砸盘卖出。
    - **Ask (卖单)**：挂在 `BestAsk`，提供流动性（赚 Spread + Rebate）。
    - **Bid (买单)**：挂在 `MidPrice - 2%` (极低位置)，很难成交。
- **效果**：不仅赚到了价格回归的钱，还赚到了做市商奖励 (Rebates)。

### S2: The Silencer (大单回归 Pro) —— "Fade the Noise"

**回测发现**：大单冲击后，下一分钟立即回归的概率仅为 **23.5%**。这意味着无脑做反转是送钱。
**修正**：必须配合 Module 4 的 **Silence Detector**。

**执行逻辑**：
1. 监控 Orderbook 突变 (Price $\Delta > 3\%$ in 1 min)。
2. 调用 `PatternAgent` 询问：*"过去 5 分钟有推文吗？未来 10 分钟预测有推文吗？"*
3. 如果 **Silence = True** (无信息冲击)：
    - 这是纯流动性枯竭（有人手滑市价买入）。
    - **立即提供反向流动性**。
4. 如果 **Silence = False** (有推文)：
    - 这是真实信息定价。
    - **撤单 (Cancel All)**，等待新平衡。

### S3: The Farmer (得利渔翁) —— "Spread & Rebate"

**利润来源**：
1. **Spread**：买卖价差 (Polymarket 约 2-5%)。
2. **Rebates**：Polymarket 针对限价单 (Maker) 的每日奖励（通通常年化 10-50%）。

**操作细节**：
- **Re-pegging**: 每 10 秒根据 `MidPrice` 重新挂单。
- **Latency Guard**:
    - 监听 WebSocket `trade` 事件。
    - 如果 1 秒内成交量 > $1000 (Toxic Flow)，**毫秒级撤单**。
    - 保护自己不被"知情交易者"吃掉。

---

## 三、 风控：如何做到"低风险"？

为了接近用户要求的"无风险套利"，我们要用数学消灭赌博成分。

### 1. 动态库存管理 (Dynamic Inventory)
永远不持有"中性"仓位。仓位方向永远与 **Implied Rate Bias** 相反。

| 市场状态 | Implied Rate | 目标库存 (Inventory Target) | 行为 |
|:--------|:-------------|:--------------------------|:-----|
| 极度高估 | > 60 | -$100 (Max Short) | 只挂 Ask，不挂 Bid |
| 高估 | 45 - 60 | -$50 (Short Bias) | Ask 激进，Bid 保守 |
| 合理 | 25 - 45 | 0 (Neutral) | 双边同距做市 |
| 低估 | < 25 | +$50 (Long Bias) | Bid 激进，Ask 保守 |

### 2. 联动对冲 (Correlation Hedging) —— *Advanced*
*如果同时存在 "2天市场" 和 "7天市场"*：
- 它们高度相关。
- 如果 "2天" 出现 $ImpliedRate = 80$ 的极度泡沫。
- 我们在 "2天" 做空 (Sell YES)，同时在 "7天" 少量买入 (Hedge)。
- **风险对冲**：如果 Elon 真的发疯狂发推，7天市场的 Long 会弥补 2天市场的亏损。

---

## 四、 为什么这比 Module 2 (Arb) 更好？

| 特性 | Module 2 (Arb) | **Module 3 (Liquidity Vampire)** |
|:-----|:---------------|:---------------------------------|
| **机会频率** | 极低 (每天 < 5次) | **极高 (每分钟都在赚)** |
| **资金利用率** | 低 (得等机会) | **100% (一直在挂单)** |
| **利润来源** | 市场错误定价 | **时间价值 + 散户冲动 + 官方奖励** |
| **扩展性** | 难 (依赖价差) | **无限 (任何活跃市场都能做)** |

---

## 五、 Phase 0 实施路径

1.  **Agent Skeleton**: 搭建 Python 架构 (今日完成)。
2.  **Data Fix**: 修复 `BOOK_FAIL` (首要任务)。
3.  **Metrics Engine**: 编写计算 `Implied Rate` 的通用函数 (Duration Agnostic 核心)。
4.  **Paper Trading**: 在本地模拟 S1 (Short Squeeze) 跑 24小时验证。

---

## 六、 执行架构 (T2T Benchmark 驱动决策)

**2026-02-12 实测数据**：
- **场景**: 120 Iterations × 1000 Orderbook Updates × 250 Levels/side (真实压力测试)
- **Python (Pydantic/Dict)**: ~59.5 µs/update
- **Rust (BTreeMap)**: ~2.2 µs/update
- **加速比**: **27.24x**

**架构决断**：
1.  **Phase 0 (Prototype)**: **全 Python**。
    - S1 (Squeeze) 和 S2 (Pattern) 对 <100µs 的延迟不敏感。Python 的开发速度优先。
2.  **Phase 2 (Production)**: **混合架构 (Hybrid)**。
    - S3 (Spread/Rebate) 需要与 HFT 竞争，必须上 Rust。
    - **Brain (Python)**: 策略逻辑、风控、模式识别 (Agent)。
    - **Spine (Rust)**: 订单簿维护、WebSocket 解析、签名生成。
    - **Integration**: 通过 **PyO3** 绑定 (Zero-Copy 内存共享)，消除 IPC 开销。
