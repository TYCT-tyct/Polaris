# Module 2 Part 3: 执行引擎、风控、架构与路线图

> 接续 Part 1 (Strategies A-D) 和 Part 2 (Strategies E-H)

---

## 十一、执行引擎与风控体系

### 11.1 三阶段开发路径

| Phase | 行为 | 策略 | 持续时间 |
|---|---|---|---|
| **Phase 1: Monitor** | 只检测 + 记录 | A, B, F, G | 2-4 周 |
| **Phase 2: Paper** | 模拟下单 + 虚拟 PnL | A, C, D, F, G | 2-4 周 |
| **Phase 3: Live** | 真实下单，最小仓位 | 全部 | 持续 |

### 11.2 下单签名流程 (py-clob-client)

```python
from py_clob_client.client import ClobClient as PolyClient
from py_clob_client.clob_types import OrderArgs, OrderType

class OrderExecutor:
    def __init__(self, private_key: str, chain_id: int = 137):
        self.client = PolyClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=chain_id,    # 137 = Polygon mainnet
        )
        # L2 认证：API Key 从私钥派生
        self.client.set_api_creds(self.client.create_or_derive_api_creds())

    async def place_limit_order(
        self, token_id: str, side: str, price: float, size: int
    ) -> str:
        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=side,             # "BUY" | "SELL"
        )
        signed = self.client.create_order(order_args)
        resp = self.client.post_order(signed, OrderType.GTC)
        return resp["orderID"]

    async def cancel_order(self, order_id: str) -> bool:
        return self.client.cancel(order_id)

    async def cancel_all(self) -> bool:
        return self.client.cancel_all()
```

### 11.3 套利执行流程

```python
async def execute_strategy_a(self, opp: ArbOpportunity) -> ExecutionResult:
    """Strategy A: NegRisk 概率加总"""

    # ── Pre-flight ──────────────────────────
    # 1. 重新拉取盘口确认机会仍存在
    fresh_books = await gather(*(self.clob.get_book(l.token_id) for l in opp.legs))
    fresh = check_neg_risk_sum(opp.event, opp.markets, fresh_books)
    if not fresh or fresh.discount < 0.03:
        return ExecutionResult(status="STALE")

    # 2. 余额检查
    required = sum(l.price * fresh.max_shares for l in fresh.legs)
    if required > self.balance * 0.30:
        return ExecutionResult(status="SKIP_CAPITAL")

    # ── 下单：按深度从浅到深排序 ──────────
    sorted_legs = sorted(fresh.legs, key=lambda l: l.available_size)
    order_ids = []

    try:
        for leg in sorted_legs:
            oid = await self.executor.place_limit_order(
                token_id=leg.token_id,
                side="BUY",
                price=leg.price,
                size=fresh.max_shares,
            )
            order_ids.append(oid)

        # ── 等待填充（最多 30s）──────────
        filled = await self._wait_fills(order_ids, timeout=30)

        if all(f.fully_filled for f in filled):
            return ExecutionResult(status="FILLED", profit=fresh.total_profit)

        # ── 部分填充应急 ──────────────────
        return await self._handle_partial(order_ids, filled, fresh)

    except Exception as e:
        await self.executor.cancel_all()
        return ExecutionResult(status="ERROR", error=str(e))
```

### 11.4 部分填充应急方案

```python
async def _handle_partial(self, order_ids, filled, opp):
    """部分填充是套利唯一真正风险"""
    await self._cancel_unfilled(order_ids, filled)

    filled_count = sum(1 for f in filled if f.fully_filled)
    total_legs = len(opp.legs)
    fill_ratio = filled_count / total_legs

    # 方案 1：填充率 > 80% → 用市价单补完
    if fill_ratio > 0.8:
        for i, f in enumerate(filled):
            if not f.fully_filled:
                await self.executor.place_limit_order(
                    token_id=opp.legs[i].token_id,
                    side="BUY",
                    price=opp.legs[i].price * 1.05,  # 接受 5% 滑点
                    size=filled[0].filled_size,
                )
        return ExecutionResult(status="PATCHED")

    # 方案 2：填充率 < 50% → 平仓止损
    if fill_ratio < 0.5:
        for f in filled:
            if f.fully_filled:
                await self.executor.place_limit_order(
                    token_id=f.token_id,
                    side="SELL",
                    price=f.buy_price * 0.95,  # 接受 5% 损失
                    size=f.filled_size,
                )
        return ExecutionResult(status="UNWOUND")

    # 方案 3：50-80% → 持有等待后续机会补完
    return ExecutionResult(status="PARTIAL_HOLD")
```

### 11.5 四层风控体系

```
┌──────────────────────────────────────────────────┐
│  Layer 4: 系统熔断                                │
│  连续 3 次执行失败 → 暂停 1 小时                    │
│  日亏损 > $50 → 停止当日交易                       │
│  API 延迟 > 500ms → 降级为 Monitor                 │
├──────────────────────────────────────────────────┤
│  Layer 3: 资金分配                                 │
│  单次最大投入 ≤ 总资金 30%                          │
│  单事件最大持仓 ≤ 50%                              │
│  总未结算持仓 ≤ 80%                                │
├──────────────────────────────────────────────────┤
│  Layer 2: 机会质量过滤                              │
│  最小折价 ≥ 3%（Strategy A/B/C/D）                 │
│  最小年化 ≥ 10%（Strategy F/G）                    │
│  最小可执行量 ≥ 50 份                              │
│  盘口更新 < 5 秒（防止过期数据）                     │
├──────────────────────────────────────────────────┤
│  Layer 1: 数据完整性                               │
│  所有 outcome 都有 ask                             │
│  negRisk 标志确认                                  │
│  市场未 closed/archived                            │
│  anti-rug: 不参与流动性 < $1000 的市场              │
└──────────────────────────────────────────────────┘
```

---

## 十二、系统架构与集成设计

### 12.1 架构图

```
┌─────────────────────────────────────────────────────┐
│                    ArbitrageBot                       │
│                                                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│  │  Gamma     │  │  CLOB      │  │  Kalshi    │    │
│  │  Client    │  │  Client    │  │  Client    │    │
│  │  (复用M1)  │  │  (复用M1)  │  │  (新增)    │    │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘    │
│        ▼               ▼               ▼            │
│  ┌──────────────────────────────────────────────┐   │
│  │         Multi-Strategy Scanner                │   │
│  │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐  │   │
│  │  │  A  │ │  B  │ │  C  │ │  D  │ │ F/G │  │   │
│  │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘  │   │
│  └─────┼───────┼───────┼───────┼───────┼──────┘   │
│        └───────┴───────┴───────┴───────┘           │
│                        ▼                            │
│  ┌──────────────────────────────────────────────┐   │
│  │        Profitability Filter + Risk Gate       │   │
│  └─────────────────────┬────────────────────────┘   │
│                        ▼                            │
│  ┌──────────────────────────────────────────────┐   │
│  │        Order Executor (py-clob-client)        │   │
│  │        Phase 1: Log    Phase 3: Live          │   │
│  └─────────────────────┬────────────────────────┘   │
│                        ▼                            │
│  ┌──────────────────────────────────────────────┐   │
│  │        PostgreSQL (fact_arb_*)                │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### 12.2 文件结构

```
polaris/
├── arb/                              # Module 2
│   ├── __init__.py
│   ├── scanner.py                    # 多策略扫描器
│   │   ├── ArbScanner                # 主编排器
│   │   ├── NegRiskSumChecker         # Strategy A
│   │   ├── BinaryArbChecker          # Strategy B
│   │   ├── NoConvertChecker          # Strategy C
│   │   ├── BondScanner               # Strategy F
│   │   └── TailEndScanner            # Strategy G
│   ├── cross_platform.py             # Strategy D
│   │   ├── KalshiClient
│   │   └── CrossPlatformScanner
│   ├── semantic.py                   # Strategy E (Phase 3)
│   │   └── SemanticArbScanner
│   ├── market_maker.py               # Strategy H (Phase 3+)
│   │   └── MarketMaker
│   ├── executor.py                   # 下单执行
│   │   ├── OrderExecutor
│   │   └── PartialFillHandler
│   ├── models.py                     # 数据结构
│   │   ├── ArbOpportunity
│   │   ├── ArbLeg
│   │   ├── BondOpportunity
│   │   ├── TailEndOpportunity
│   │   └── ExecutionResult
│   ├── risk.py                       # 风控
│   │   ├── RiskGate
│   │   └── CircuitBreaker
│   ├── config.py                     # 配置
│   │   └── ArbConfig
│   └── cli.py                        # 命令行
│       ├── arb scan                  # 手动扫描
│       ├── arb status                # 运行状态
│       ├── arb history               # 历史记录
│       └── arb bonds                 # 债券机会
├── sources/                          # 复用 Module 1
│   ├── clob_client.py
│   ├── gamma_client.py
│   └── models.py
```

### 12.3 数据库扩展

```sql
-- ===== 套利扫描日志 =====
CREATE TABLE fact_arb_scan (
    scan_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scanned_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    strategy        TEXT NOT NULL,       -- 'A','B','C','D','E','F','G','H'
    event_id        TEXT,
    market_ids      TEXT[],
    total_ask       NUMERIC(10,6),
    discount_pct    NUMERIC(10,6),
    net_profit_pct  NUMERIC(10,6),
    max_shares      INT,
    est_profit_usd  NUMERIC(12,4),
    annualized_pct  NUMERIC(10,4),      -- Strategy F/G
    legs            JSONB NOT NULL,
    book_snapshot   JSONB,
    executed        BOOLEAN DEFAULT FALSE
);

-- ===== 执行记录 =====
CREATE TABLE fact_arb_execution (
    exec_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    scan_id         UUID REFERENCES fact_arb_scan,
    strategy        TEXT NOT NULL,
    status          TEXT NOT NULL,       -- FILLED/PARTIAL/STALE/ERROR
    shares_target   INT,
    shares_filled   INT DEFAULT 0,
    total_cost      NUMERIC(12,4),
    realized_pnl    NUMERIC(12,4),
    legs_result     JSONB,
    error_msg       TEXT
);

-- ===== 债券持仓 =====
CREATE TABLE fact_arb_bond_position (
    position_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    market_id       TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    shares          INT NOT NULL,
    buy_price       NUMERIC(10,6),
    expected_resolve TIMESTAMPTZ,
    status          TEXT DEFAULT 'OPEN', -- OPEN/SETTLED/SOLD
    settle_pnl      NUMERIC(12,4)
);
```

### 12.4 配置参数

```python
@dataclass(frozen=True)
class ArbConfig:
    # ── 策略 A: NegRisk ──
    neg_risk_min_discount: float = 0.03
    neg_risk_min_shares: int = 50
    neg_risk_max_shares: int = 500

    # ── 策略 F: Bond ──
    bond_min_price: float = 0.93
    bond_max_price: float = 0.995
    bond_min_annualized: float = 0.10
    bond_max_days: int = 30

    # ── 策略 G: Tail-end ──
    tail_max_hours: float = 6.0
    tail_min_confidence: float = 0.90
    tail_min_profit_pct: float = 0.03

    # ── 通用风控 ──
    max_capital_pct: float = 0.30       # 单次最大资金
    max_daily_loss: float = 50.0
    max_consecutive_fail: int = 3
    scan_interval_normal: int = 30      # 秒
    scan_interval_active: int = 5
```

---

## 十三、开发路线图

### Phase 1: Monitor Only（第 1-4 周）

```
[x] 完成设计文档
[ ] 实现 ArbScanner 框架 + NegRiskSumChecker (Strategy A)
[ ] 实现 BinaryArbChecker (Strategy B)
[ ] 实现 BondScanner (Strategy F)
[ ] 实现 TailEndScanner (Strategy G)
[ ] 创建 fact_arb_scan 表
[ ] 接入 scheduler，30s 扫描循环
[ ] CLI: arb scan / arb status / arb history
[ ] 运行 2 周收集数据

验证指标：
- Strategy A 机会 ≥ 3 次/天
- Strategy F/G 机会 ≥ 5 次/天
- 检测延迟 < 200ms
```

### Phase 2: Paper Trading（第 5-8 周）

```
[ ] 实现 Paper Executor（虚拟下单 + PnL 跟踪）
[ ] 实现 NoConvertChecker (Strategy C) 检测逻辑
[ ] 实现 KalshiClient + CrossPlatformScanner (Strategy D)
[ ] 虚拟 PnL 统计 > 0
[ ] 部分填充处理逻辑验证
```

### Phase 3: Live Execution（第 9 周+）

```
[ ] 集成 py-clob-client 签名下单
[ ] 实现 OrderExecutor + PartialFillHandler
[ ] 最小仓位开始（50 份/次）
[ ] Kalshi 账户开通 + 跨平台测试
[ ] 逐步扩大仓位
```

### Phase 4: 高级策略（可选）

```
[ ] Strategy E: 语义关联套利（LLM 配对）
[ ] Strategy H: 做市商模式（Avellaneda-Stoikov）
[ ] Strategy C: NegRiskAdapter 链上交互
[ ] WebSocket 实时盘口推送
```

---

## 十四、盈利性综合预测

### 保守场景（仅 Strategy A + F + G）

```
Strategy A: 3 次/天 × 200份 × 4%折价 × 0.98 × 70%成功 = $16.5/天
Strategy F: $3000 × 15% 年化 / 365 = $1.23/天
Strategy G: 1 次/天 × 300份 × 5%利润 × 0.98 × 80%成功 = $11.8/天

日利润合计: ~$29.5/天
月利润: ~$885/月
启动资金: $5000
运营成本: $2-5/月 (复用 Module 1 基础设施)
```

### 乐观场景（全策略）

```
Strategy A: $16.5/天
Strategy C: $8/天 (Phase 2+)
Strategy D: $5/天 (跨平台)
Strategy F: $1.2/天
Strategy G: $11.8/天
Strategy H: $10/天 (做市)

日利润合计: ~$52.5/天
月利润: ~$1575/月
```

> [!CAUTION]
> 实际利润高度依赖市场活跃度。选举季利润可能 3-5 倍于淡季。
> 竞争加剧会压缩利润——2025 年 spreads 已从 4-6% 压缩至 1-2%。

---

## 十五、与其他模块的关系

```
Module 1 (DataHarvester) ──→ 市场数据 + 盘口 + 推文计数
                              │
                              ▼
Module 2 (ArbitrageBot)    独立运行，8 种策略并行扫描
                              │
                              │ 无直接依赖
                              ▼
Module 3 (SpreadHunter)    独立运行
Module 4 (ClawBot)         独立运行
```

**唯一协同点**：Strategy G (尾部收敛) 利用 Module 1 的实时推文计数来预测最终 bin。
