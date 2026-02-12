# Module 2 Part 2: Strategies E-H 详细设计

> 接续 `module2_arbitragebot_design.md`（Part 1: Strategies A-D）

---

## 六、Strategy E: 语义关联/组合套利

### 原理

不同市场问的是**逻辑等价或蕴含关系**的问题，但价格不一致。

```
市场 A: "Will Democrats lose the Senate?"        YES = $0.55
市场 B: "Will Republicans control the Senate?"   YES = $0.48

逻辑上 A-YES ≈ B-YES（语义等价）
价差 = $0.07 → 买 B-YES + 买 A-NO($0.45)
总成本 = $0.48 + $0.45 = $0.93
如果语义真的等价，保证一边赢 $1.00
净利润 = 0.98 × $0.07 = $0.069/份
```

### 三种语义关系类型

| 类型 | 例子 | 检测难度 |
|---|---|---|
| 等价 | "X wins?" vs "X elected?" | 中（LLM） |
| 蕴含 | "GDP > 3%?" 蕴含 "GDP > 2%?" | 高 |
| 互补 | "Biden wins?" + "Trump wins?" in 2-person race | 低 |

### 检测方法

```python
class SemanticArbScanner:
    """用 LLM 判断市场间的逻辑关系"""

    async def find_related_markets(self, markets: list[Market]) -> list[MarketPair]:
        """Step 1: 用 embedding 找候选对"""
        embeddings = await self.embed_questions([m.question for m in markets])
        pairs = []
        for i in range(len(markets)):
            for j in range(i + 1, len(markets)):
                sim = cosine_similarity(embeddings[i], embeddings[j])
                if sim > 0.85:  # 高相似度
                    pairs.append(MarketPair(markets[i], markets[j], sim))
        return pairs

    async def classify_relationship(self, pair: MarketPair) -> str:
        """Step 2: 用 LLM 精确分类关系"""
        prompt = f"""
        Market A: "{pair.a.question}"
        Market B: "{pair.b.question}"
        Classify: EQUIVALENT / IMPLIES_AB / IMPLIES_BA / COMPLEMENTARY / UNRELATED
        """
        return await self.llm.classify(prompt)

    async def check_arb(self, pair: MarketPair, relation: str) -> ArbOpportunity | None:
        """Step 3: 基于关系类型检查套利"""
        if relation == "EQUIVALENT":
            # A-YES 应该 ≈ B-YES，检查价差
            a_yes = await self.get_best_ask(pair.a, "YES")
            b_yes = await self.get_best_ask(pair.b, "YES")
            if abs(a_yes - b_yes) > 0.05:
                # 买便宜的 YES + 买贵的 NO
                cheap, expensive = (pair.a, pair.b) if a_yes < b_yes else (pair.b, pair.a)
                cheap_yes = min(a_yes, b_yes)
                expensive_no = 1.0 - max(a_yes, b_yes)  # 近似
                # ... 构建套利
```

### 可行性评估：⚠️ 可行但风险较高

| 因素 | 评估 |
|---|---|
| 技术实现 | ✅ embedding + LLM 分类 |
| 核心风险 | ❌ **语义不完全等价**导致两边都亏 |
| 例子 | "X wins election" vs "X inaugurated" → 赢了不一定就职 |
| Flashbots 研究 | 组合套利利润 < 单市场套利（执行难度大） |
| 建议 | **Phase 3 实施，需要人工审核配对** |

---

## 七、Strategy F: 高概率债券策略 ⭐

### 原理

买入 $0.95-$0.99 的"几乎确定"outcome，等结算赚 1-5% 的无聊利润。像买短期国债。

```
市场: "Will the sun rise tomorrow?"  YES = $0.99
买入 1000 份 × $0.99 = $990
结算收到 $1000
毛利 = $10，扣 2% winner fee 后净利 = $9.80
年化收益 = 如果 1 天结算 → 9.80/990 × 365 = 361% 年化
```

### 关键：时间价值的利用

```python
@dataclass(frozen=True)
class BondOpportunity:
    market_id: str
    question: str
    yes_price: float          # 0.95 - 0.99
    resolution_date: datetime
    days_to_resolve: float
    gross_yield: float        # (1.0 - price) / price
    net_yield: float          # gross × 0.98
    annualized_yield: float   # net_yield × (365 / days_to_resolve)
    confidence_score: float   # 我们对"确定性"的评估

class BondScanner:
    MIN_PRICE = 0.93           # 最低价格（太低说明不确定）
    MAX_PRICE = 0.995          # 太高没利润
    MIN_ANNUALIZED = 0.10      # 最低年化 10%
    MAX_DAYS = 30              # 最长 30 天

    async def scan(self, markets: list[Market]) -> list[BondOpportunity]:
        opps = []
        for market in markets:
            if market.closed or not market.end_date:
                continue
            book = await self.clob.get_book(market.yes_token_id)
            best_ask = min((l.price for l in book.asks), default=None)
            if not best_ask or best_ask < self.MIN_PRICE or best_ask > self.MAX_PRICE:
                continue

            days = (market.end_date - now()).total_seconds() / 86400
            if days > self.MAX_DAYS or days < 0.1:
                continue

            gross = (1.0 - best_ask) / best_ask
            net = gross * 0.98
            annual = net * (365 / days)
            if annual < self.MIN_ANNUALIZED:
                continue

            opps.append(BondOpportunity(
                market_id=market.market_id,
                question=market.question,
                yes_price=best_ask,
                resolution_date=market.end_date,
                days_to_resolve=days,
                gross_yield=gross,
                net_yield=net,
                annualized_yield=annual,
                confidence_score=best_ask,  # 价格本身就是置信度
            ))
        return sorted(opps, key=lambda o: o.annualized_yield, reverse=True)
```

### 风险分析

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| 黑天鹅事件 | 极低 | 亏 $0.95/份 | 分散到多个不相关事件 |
| 结算延迟 | 中 | 资金锁定时间延长 | 只选有明确 end_date 的市场 |
| 价格下跌 | 低 | 如果提前卖出有损失 | 持有到结算，不提前退出 |

### 实际案例

```
美联储利率市场：
"Fed 维持利率不变 3月？" YES = $0.96，结算 2 天后
年化 = (0.04/0.96) × 0.98 × (365/2) = 743%

但这么高的年化意味着：
1. 资金只锁定 2 天
2. 绝对利润很小（4% × $100 = $4）
3. 需要高资金量才有意义
```

### 可行性评估：✅ 完全可行，低风险稳定

| 因素 | 评估 |
|---|---|
| 实现难度 | 低（只需读盘口 + 买入 + 等待） |
| 风险 | 低（$0.95+ 的事件几乎确定发生） |
| 利润特征 | 小而稳定，类似固收 |
| 竞争 | 低（利润太小，大资金看不上） |
| 资金需求 | 高（需要 $5000+ 才有意义） |

---

## 八、Strategy G: 尾部收敛交易 ⭐

### 原理

事件即将结算时，价格收敛到真实概率。在结算前 2-4 小时买入"已成定局"的 outcome。

```
时间线：
T-24h: "Elon 发了 80 条推" → 65-89 bin YES = $0.70
T-4h:  实际已有 82 条    → 65-89 bin YES = $0.85
T-2h:  已有 85 条        → 65-89 bin YES = $0.92
T-0:   结算 86 条        → 65-89 bin YES = $1.00

如果在 T-4h 买入，利润 = $0.15/份
如果在 T-2h 买入，利润 = $0.08/份
```

### 关键洞察：用时间换确定性

Polymarket 市场精度在结算前急剧提高：
- T-24h: 88.2% 准确率
- T-4h:  95.4% 准确率
- T-1h:  98%+ 准确率

### 检测算法（利用 Module 1 的实时数据）

```python
class TailEndScanner:
    """结合 Module 1 的推文实时数据做尾部收敛"""

    async def scan(self, event: Event) -> TailEndOpportunity | None:
        # Step 1: 检查市场是否接近结算
        hours_left = (event.end_date - now()).total_seconds() / 3600
        if hours_left > 6 or hours_left < 0.5:
            return None

        # Step 2: 获取实时推文计数（来自 Module 1 DataHarvester）
        current_count = await self.db.get_latest_tweet_count(event.user_id)

        # Step 3: 判断哪个 bin 最可能赢
        likely_bin = self._determine_bin(current_count, hours_left, event.bins)

        # Step 4: 获取该 bin 的当前价格
        book = await self.clob.get_book(likely_bin.yes_token_id)
        current_price = min((l.price for l in book.asks), default=None)
        if not current_price:
            return None

        # Step 5: 利润是否足够
        expected_profit = (1.0 - current_price) * 0.98
        if expected_profit < 0.03:  # 至少 3% 利润
            return None

        return TailEndOpportunity(
            bin=likely_bin,
            current_count=current_count,
            hours_left=hours_left,
            buy_price=current_price,
            expected_profit=expected_profit,
            confidence=self._calc_confidence(current_count, hours_left, likely_bin),
        )

    def _determine_bin(self, count: int, hours_left: float, bins: list[Bin]) -> Bin:
        """基于当前计数和剩余时间预测最终 bin"""
        # 用 Module 1 的历史数据估算 Elon 的发推速率
        avg_rate = self.db.get_avg_tweet_rate_per_hour(hours=24)
        projected_total = count + avg_rate * hours_left
        for b in bins:
            if b.lower <= projected_total < b.upper:
                return b
        return bins[-1]
```

### 与 Module 1 的协同

这是 **ArbitrageBot 和 DataHarvester 唯一的深度协同点**：

```
Module 1 提供 → 实时推文计数 + 发推速率
Module 2 利用 → 预测最终 bin → 在结算前买入
```

### 可行性评估：✅ 可行，你有独特信息优势

| 因素 | 评估 |
|---|---|
| 实现难度 | 低-中（核心是预测逻辑） |
| 信息优势 | ✅ Module 1 的实时推文数据是独特优势 |
| 风险 | 低（接近结算时不确定性很小） |
| 利润 | 中等（每次 3-15%，取决于入场时机） |
| 竞争 | 中（其他人也能看推文但可能没有你的自动化） |

---

## 九、Strategy H: 做市商价差捕获

### 原理

在 bid 和 ask 两侧同时挂单，当两边都成交时赚取价差。

```
当前盘口：bid $0.48 / ask $0.52（spread = $0.04）
你挂：bid $0.49 / ask $0.51（spread = $0.02）
如果两边都成交：利润 = $0.02/份
```

### Avellaneda-Stoikov 做市模型（简化版）

```python
class MarketMaker:
    def __init__(self, gamma: float = 0.1, sigma: float = 0.05):
        self.gamma = gamma    # 风险厌恶系数
        self.sigma = sigma    # 波动率估计

    def optimal_spread(self, inventory: int, time_left: float) -> tuple[float, float]:
        """计算最优 bid/ask 报价"""
        mid_price = self.current_mid()
        # Avellaneda-Stoikov 公式
        reservation_price = mid_price - inventory * self.gamma * self.sigma ** 2 * time_left
        optimal_spread = self.gamma * self.sigma ** 2 * time_left + (2 / self.gamma) * log(1 + self.gamma / self.k)
        bid = reservation_price - optimal_spread / 2
        ask = reservation_price + optimal_spread / 2
        return max(0.01, bid), min(0.99, ask)

    async def quote_cycle(self, market: Market):
        """一个报价周期"""
        # 1. 取消所有现有挂单
        await self.cancel_all_orders(market)
        # 2. 计算最优报价
        bid, ask = self.optimal_spread(self.inventory, self.time_remaining)
        # 3. 下限价单
        await self.place_order(market, "BUY", bid, self.quote_size)
        await self.place_order(market, "SELL", ask, self.quote_size)
        # 4. 监控成交
        await self.monitor_fills()
```

### 库存管理（核心风险）

```python
class InventoryManager:
    MAX_INVENTORY = 500        # 最大净头寸
    SKEW_FACTOR = 0.005        # 每持有 1 份偏移价格 0.5%

    def skewed_quotes(self, mid: float, inventory: int) -> tuple[float, float]:
        """库存偏斜报价：持仓多时降低 bid、提高 ask"""
        skew = inventory * self.SKEW_FACTOR
        bid = mid - self.half_spread - skew
        ask = mid + self.half_spread - skew
        return bid, ask

    def should_pause(self, inventory: int) -> bool:
        """库存超限时停止报价"""
        return abs(inventory) > self.MAX_INVENTORY
```

### 做市商奖励

Polymarket 对限价单提供 **maker reward**（部分市场），进一步提升做市利润。

### 可行性评估：⚠️ 可行但风险最高

| 因素 | 评估 |
|---|---|
| 利润潜力 | 高（持续赚价差 + maker reward） |
| 核心风险 | ❌ **事件风险**：二元结果导致持仓归零 |
| 库存风险 | 价格剧烈波动时 inventory 可能巨亏 |
| 技术要求 | 高（持续报价、库存管理、风控） |
| 竞争 | 极高（专业做市商 + 机构 quant） |
| 建议 | **Phase 3+，仅在低波动市场试水** |

---

## 十、费用体系与盈亏平衡

### Polymarket 费用结构

| 费用类型 | 金额 | 适用范围 |
|---|---|---|
| 交易费 | **0%** | 大部分市场 |
| 赢家费 | **2%** | 对盈利部分收取 |
| Gas 费 | ~$0.01 | Polygon 链上操作 |
| 动态 Taker 费 | 0-3.15% | **仅限** 15 分钟 crypto 市场 |
| 入金手续费 | 1-2% | 第三方 on-ramp (MoonPay) |
| 出金费用 | Gas only | Polygon → 外部钱包 |

### Kalshi 费用（跨平台套利需考虑）

| 费用类型 | 金额 |
|---|---|
| 交易费 | 合约价格相关（40-60¢最高） |
| ACH 出入金 | 免费 |
| 借记卡入金 | 2% |

### 各策略盈亏平衡点

```
Strategy A (NegRisk):     Σ(ask) < $0.97 → 净利 > 0
Strategy B (Binary):      YES+NO < $0.97 → 净利 > 0
Strategy C (NO Convert):  NO_ask < Σ(complement_YES) - $0.03
Strategy D (Cross-plat):  price_diff > 3% + Kalshi fees
Strategy F (Bond):        annualized > 10% 才值得锁资金
Strategy G (Tail-end):    buy_price < $0.97 且 confidence > 90%
Strategy H (Market Make): avg_spread > 2% 且 fill_rate > 50%
```
