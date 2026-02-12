# Module 2: ArbitrageBot 套利引擎 — 完整方案设计 V2

> **设计哲学**：套利不赌方向，赌市场结构性定价错误会被修正。
> $40M 套利利润在 2024-2025 年间从 Polymarket 被提取——证明机会真实存在。
> 本文档覆盖 **8 种可实现策略**，每种给出可行性评估和实现方案。

---

## 目录

1. [策略总览与优先级矩阵](#一策略总览与优先级矩阵)
2. [Strategy A: NegRisk 概率加总套利](#二strategy-a-negrisk-概率加总套利-核心)
3. [Strategy B: Binary YES+NO 套利](#三strategy-b-binary-yesno-套利)
4. [Strategy C: NegRisk NO→YES 转换套利](#四strategy-c-negrisk-noyes-转换套利)
5. [Strategy D: 跨平台 Polymarket-Kalshi 套利](#五strategy-d-跨平台-polymarket-kalshi-套利)
6. [Strategy E: 语义关联/组合套利](#六strategy-e-语义关联组合套利)
7. [Strategy F: 高概率债券策略](#七strategy-f-高概率债券策略)
8. [Strategy G: 尾部收敛交易](#八strategy-g-尾部收敛交易)
9. [Strategy H: 做市商价差捕获](#九strategy-h-做市商价差捕获)
10. [费用体系与盈亏平衡](#十费用体系与盈亏平衡)
11. [执行引擎与风控体系](#十一执行引擎与风控体系)
12. [系统架构与集成设计](#十二系统架构与集成设计)
13. [开发路线图](#十三开发路线图)

---

## 一、策略总览与优先级矩阵

| # | 策略 | 风险等级 | 实现难度 | 预期月利润 | 资金需求 | 优先级 |
|---|------|---------|---------|-----------|---------|--------|
| A | NegRisk 概率加总 | **无风险** | 中 | $300-800 | $1500 | ⭐⭐⭐ P0 |
| B | Binary YES+NO | **无风险** | 低 | $0-50 | $500 | P2 监控 |
| C | NO→YES 转换 | **无风险** | 高 | $200-600 | $2000 | ⭐⭐ P1 |
| D | 跨平台 Poly-Kalshi | **无风险** | 高 | $100-400 | $3000 | ⭐ P1 |
| E | 语义关联组合 | **低风险** | 极高 | $50-200 | $2000 | P3 |
| F | 高概率债券 | **低风险** | 低 | $50-150 | $5000 | ⭐ P1 |
| G | 尾部收敛 | **低风险** | 低 | $80-250 | $3000 | ⭐ P1 |
| H | 做市价差捕获 | **中风险** | 高 | $200-1000 | $5000 | P2 |

**核心原则**：先做无风险的（A/B/C/D），再做低风险的（F/G），最后做需要判断力的（E/H）。

---

## 二、Strategy A: NegRisk 概率加总套利 ⭐⭐⭐ 核心

### 原理

N 个互斥 outcome 的市场（negRisk=true），买入所有 YES，保证恰好 1 个结算到 $1.00。

```
市场："Elon 本周推文数量"
Outcome     YES_ask
<40         $0.05
40-64       $0.12
65-89       $0.25
90-114      $0.30
115-139     $0.15
140+        $0.08
─────────────────
Σ(ask)    = $0.95 → 折价 5%
净利润    = 0.98 × $0.05 = $0.049/份（扣 2% winner fee）
```

### 为什么这是最佳策略

1. **散户系统性偏差**：散户高估热门 bin、低估冷门 bin，但不做加总校验
2. **做市商覆盖不完全**：6+ outcome 的市场，做市商难以同时维护所有盘口
3. **利润占比 73%**：研究表明 negRisk rebalancing 占所有 Polymarket 套利利润的 73%
4. **资本效率 29 倍**：相比 binary 套利，negRisk 的资本利用率是 29 倍

### 检测算法

```python
@dataclass(frozen=True)
class ArbLeg:
    market_id: str
    token_id: str
    side: str          # "BUY"
    price: float
    available_size: int

@dataclass(frozen=True)
class ArbOpportunity:
    event_id: str
    arb_type: str             # "A" | "B" | "C" | ...
    total_ask: float
    discount: float           # 1.0 - total_ask
    net_profit_per_share: float
    max_shares: int
    total_profit: float
    legs: list[ArbLeg]
    detected_at: datetime

def check_neg_risk_sum(
    event: Event, markets: list[Market], books: list[ClobBook],
) -> ArbOpportunity | None:
    legs, total_ask, min_depth = [], 0.0, float("inf")

    for market, book in zip(markets, books):
        best_ask = min((lvl.price for lvl in book.asks), default=None)
        if best_ask is None:
            return None
        depth = sum(l.size for l in book.asks if l.price == best_ask)
        total_ask += best_ask
        min_depth = min(min_depth, depth)
        legs.append(ArbLeg(market.market_id, market.yes_token_id, "BUY", best_ask, depth))

    discount = 1.0 - total_ask
    if discount < 0.03:        # 最小 3% 折价门槛
        return None
    net = 0.98 * discount      # 扣 2% winner fee
    shares = min(int(min_depth), 500)
    if shares < 50:
        return None

    return ArbOpportunity("A", event.id, total_ask, discount, net, shares, net * shares, legs, now())
```

### 多层盘口真实利润计算

```python
def real_profit(books: list[ClobBook], target: int) -> tuple[float, int]:
    """模拟在每个 outcome 上买入 target 份的真实成本"""
    total_cost, actual = 0.0, target
    for book in books:
        asks = sorted(book.asks, key=lambda x: x.price)
        cost, filled = 0.0, 0
        for level in asks:
            take = min(level.size, target - filled)
            cost += take * level.price
            filled += take
            if filled >= target: break
        actual = min(actual, filled)
        total_cost += cost
    # 按实际可执行量重算（木桶原理）
    if actual < target:
        total_cost = sum(
            sum(min(l.size, actual - sum(min(l2.size, actual) for l2 in sorted(b.asks, key=lambda x: x.price)[:i])) * l.price
                for i, l in enumerate(sorted(b.asks, key=lambda x: x.price)))
            for b in books
        )  # 简化：实际实现用循环
    return (actual * 1.0 - total_cost) * 0.98, actual
```

### 可行性评估：✅ 完全可行

| 因素 | 评估 |
|---|---|
| API 支持 | ✅ CLOB API + Gamma API 完全支持 |
| 出现频率 | 每天 3-8 次（Elon tweet 市场） |
| 存活时间 | 10-120 秒（足够执行） |
| 你的延迟 | 70-90ms RTT，可接受 |
| 竞争强度 | 中等（需理解 negRisk 机制） |

---

## 三、Strategy B: Binary YES+NO 套利

### 原理

单个二元市场：YES_ask + NO_ask < $1.00 → 买两边锁定利润。

```
YES ask = $0.45, NO ask = $0.52
总成本 = $0.97, 结算 = $1.00
净利润 = 0.98 × $0.03 = $0.029/份
```

### 可行性评估：⚠️ 可实现但利润极低

| 因素 | 评估 |
|---|---|
| 出现频率 | 极低（做市商 24/7 盯着） |
| 存活时间 | < 3 秒 |
| 竞争 | 极高（HFT bot 10-20ms 内吃掉） |
| 你的延迟 | 70ms 太慢 |
| 建议 | **作为健康检查指标**，不作为利润来源 |

### 实现（作为 Scanner 的一部分）

```python
def check_binary_arb(market: Market, yes_book: ClobBook, no_book: ClobBook) -> ArbOpportunity | None:
    yes_ask = min((l.price for l in yes_book.asks), default=None)
    no_ask = min((l.price for l in no_book.asks), default=None)
    if not yes_ask or not no_ask:
        return None
    total = yes_ask + no_ask
    if total >= 0.97:  # 需要 3% 折价
        return None
    # ... 构建 ArbOpportunity
```

---

## 四、Strategy C: NegRisk NO→YES 转换套利

### 原理

利用 NegRiskAdapter 合约的 `convertPositions()`：1 个 outcome-X 的 NO = 所有其他 outcome 的 YES 组合。

```
市场 6 个 outcome，Outcome X 的 NO ask = $0.60
买入 X-NO → 用 convertPositions() 转为 5 个其他 YES
结算时恰好 1 个 YES = $1.00
利润 = $1.00 - $0.60 - 2% × $0.40 = $0.392/份（39.2%！）
```

### 为什么利润最高

- 散户几乎不理解 NO share 的隐含价值
- NO 盘口流动性差 → 更大价格偏离
- 转换操作是链上原子的 → 无执行风险

### 检测逻辑

```python
def check_neg_risk_no_conversion(event: Event, markets: list[Market], books: list[ClobBook]) -> list[ArbOpportunity]:
    """检查每个 outcome 的 NO 价格是否低于其互补 YES 价格之和"""
    opps = []
    n = len(markets)
    for i, (market, book) in enumerate(zip(markets, books)):
        no_book = books_no[i]  # 需要额外拉取 NO 盘口
        no_ask = min((l.price for l in no_book.asks), default=None)
        if not no_ask:
            continue
        # 互补的 YES 总价 = Σ(其他 outcome 的 YES ask)
        complement_yes = sum(
            min((l.price for l in books[j].asks), default=1.0)
            for j in range(n) if j != i
        )
        # 如果 NO 价格 < 互补 YES 总价，转换更便宜
        if no_ask < complement_yes and (complement_yes - no_ask) > 0.03:
            # 通过 NO 买入 + convert 比直接买所有 YES 更便宜
            saving = complement_yes - no_ask
            opps.append(ArbOpportunity(arb_type="C", ...))
    return opps
```

### 可行性评估：✅ 可行但需链上交互

| 因素 | 评估 |
|---|---|
| 技术要求 | 需要 web3.py + NegRiskAdapter ABI |
| 链上 Gas | Polygon ~$0.01/tx，可忽略 |
| 出现频率 | 中等（NO 盘口定价偏差更频繁） |
| 实现阶段 | Phase 2（先做 Strategy A） |

---

## 五、Strategy D: 跨平台 Polymarket-Kalshi 套利

### 原理

同一事件在 Polymarket 和 Kalshi 上价格不同 → 低买高卖。

```
事件："Fed 3月降息？"
Polymarket YES = $0.62
Kalshi YES     = $0.68
→ 买 Polymarket YES + 买 Kalshi NO($0.32)
总成本 = $0.62 + $0.32 = $0.94
保证有一边结算 $1.00
净利润 = $0.06/份
```

### 实现要求

```python
class CrossPlatformScanner:
    """跨平台套利扫描器"""
    def __init__(self, poly_client: ClobClient, kalshi_client: KalshiClient):
        self.poly = poly_client
        self.kalshi = kalshi_client
        self.event_pairs: list[EventPair] = []  # 手动/自动配对

    async def scan(self) -> list[CrossPlatformOpp]:
        opps = []
        for pair in self.event_pairs:
            poly_book = await self.poly.get_book(pair.poly_token_id)
            kalshi_book = await self.kalshi.get_orderbook(pair.kalshi_ticker)
            # 双向检查
            poly_yes = min_ask(poly_book)
            kalshi_no = min_ask(kalshi_book, side="NO")
            if poly_yes + kalshi_no < 0.97:
                opps.append(CrossPlatformOpp(...))
            # 反向
            kalshi_yes = min_ask(kalshi_book, side="YES")
            poly_no = min_ask(poly_no_book)
            if kalshi_yes + poly_no < 0.97:
                opps.append(CrossPlatformOpp(...))
        return opps
```

### Kalshi API 集成

```python
class KalshiClient:
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
    DEMO_URL = "https://demo-api.kalshi.co/trade-api/v2"

    async def get_orderbook(self, ticker: str) -> KalshiBook:
        resp = await self._client.get(f"/markets/{ticker}/orderbook")
        return KalshiBook.model_validate(resp.json())

    async def get_markets(self, series: str) -> list[KalshiMarket]:
        resp = await self._client.get("/markets", params={"series_ticker": series})
        return [KalshiMarket(**m) for m in resp.json()["markets"]]
```

### 可行性评估：⚠️ 可行但有实际障碍

| 因素 | 评估 |
|---|---|
| Kalshi API | ✅ 免费 REST API，有 demo 环境 |
| 事件匹配 | ⚠️ 需手动或 LLM 辅助配对同义事件 |
| 资金转移 | ❌ Poly=USDC, Kalshi=USD，跨链耗时 |
| 结算差异 | ⚠️ 同一事件可能定义不同导致结果不同 |
| 延迟 | 两个平台各 70-100ms，总计 150-200ms |
| Kalshi 费用 | ⚠️ Kalshi 有交易费（40-60¢合约费最高） |
| 建议 | **先用 demo 环境验证**，Phase 2 实施 |
