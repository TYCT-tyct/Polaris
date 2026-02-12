# ClawBot Agentic Architecture Design

> **核心变革**：从"依赖特定预测模型"转向"自主 Agent 集群"。
> **目标**：即使 OpenClaw（外部高级模型）挂了，本地 Agent 依然能通过基础 LLM + 统计模型稳定赚钱。

---

## 一、为什么转为 Agent 架构？

原设计依赖一个中心化的"预测引擎"（OpenClaw）。风险在于：
1. **单点故障**：如果预测模型 API 挂了，整个策略停摆。
2. **黑盒风险**：我们不知道模型为什么给出这个数字。
3. **扩展性差**：很难加入新的观察角度（如新闻、链上数据）。

**Agent 架构优势**：
- **去中心化**：多个小 Agent 各司其职（一个看图，一个算数，一个读推文）。
- **容错性**：某个 Agent 挂了，其他 Agent 还能投票得出结论。
- **可解释性**：每个 Agent 都会输出自己的推理过程。
- **成本低**：日常监控用便宜模型（GPT-4o-mini/Claude Haiku），关键时刻才用大模型。

---

## 二、Agent 集群角色定义

我们设计 5 个轻量级 Agent，模拟一个"交易室"：

### 1. Data Agent (数据员)
- **职责**：只负责事实，不负责观点。
- **任务**：
  - 实时更新 XTracker 数据。
  - 计算日均速率、环比变化。
  - 维护 `fact_tweet_metric_daily` 准确性。
- **输出**："当前窗口已过 45%，累计 102 条，日均 22.6 条，且周末发帖率通常只有工作日的 80%。"

### 2. Pattern Agent (分析师)
- **职责**：识别模式和政权。
- **任务**：
  - 用 `fact_tweet_post` 分析最近 50 条推文内容。
  - 判断当前是"政治狂热期"、"SpaceX 任务期"还是"度假潜水期"。
  - 对比历史相似周期。
- **输出**："当前内容 60% 是转发，且没有任何政治关键词，属于低活跃的'潜水期'，类似 2025 年 6 月。"

### 3. Market Agent (盯盘手)
- **职责**：监控盘口和资金流。
- **任务**：
  - 监控 CLOB 订单簿。
  - 识别大单冲击（S2）和开盘错价（S1）。
  - 计算隐含概率分布。
- **输出**："Bin 200-220 价格异常偏高 (0.22)，且 ask 深度只有 $300，存在大单冲击痕迹。"

### 4. Risk Agent (风控官)
- **职责**：只说"不"。
- **任务**：
  - 检查仓位限制、止损条件。
  - 否决一切高风险提议。
- **输出**："拒绝交易。虽然你们觉得 240+ 会赢，但当前已持有 $80 风险敞口，且该 bin 流动性过差。"

### 5. Strategy Agent (交易主管) — **核心决策者**
- **职责**：综合所有信息，下达最终指令。
- **任务**：
  - 听取 Data, Pattern, Market Agent 的报告。
  - 在 Risk Agent 允许的范围内决策。
  - 调用普通 LLM (GPT-4o-mini) 进行最终逻辑合成。
- **输出**："根据 Data Agent 的速率 (预计 210) 和 Pattern Agent 的定性 (低活跃)，Market Agent 发现的 240+ bin (0.15) 明显高估。决定卖出 240+ YES，仓位 $30。"

---

## 三、OpenClaw 的新角色：监督者 (Supervisor)

OpenClaw 不再直接参与每一笔交易，而是作为**"董事会"**：

1. **事后复盘**：每天收盘后，分析 Strategy Agent 的每笔决策，给出评分和改进建议。
2. **宏观预警**：当发生重大事件（如 Elon 宣布收购新公司）时，覆盖 Strategy Agent 的默认逻辑，下达"全线暂停"或"方向性调整"指令。
3. **参数调优**：每周根据 PnL 调整 Risk Agent 的阈值。

---

## 四、技术实现：Antigravity Agent Framework

不需要复杂的 LangChain，我们用更轻量的状态机：

```python
class AgentContext:
    data_snapshot: dict
    market_snapshot: dict
    history: list

class BaseAgent:
    async def think(self, ctx: AgentContext) -> AgentOutput:
        pass

class StrategyAgent(BaseAgent):
    async def decide(self, inputs: list[AgentOutput]) -> TradeDecision:
        # 1. 组装 prompt
        prompt = f"""
        Data says: {inputs['data']}
        Pattern says: {inputs['pattern']}
        Market says: {inputs['market']}
        Risk constraints: {inputs['risk']}

        作为交易主管，请给出操作建议：BUY / SELL / HOLD，及理由。
        """
        # 2. 调用普通 LLM
        response = await llm_client.complete(prompt, model="gpt-4o-mini")
        # 3. 解析并执行
        return parse_decision(response)
```

---

## 五、盈利与可行性

- **成本**：GPT-4o-mini 极其便宜，全天候运行成本 < $5/月。
- **稳定性**：本地代码逻辑 (Rule-based) 处理 80% 的基础工作，LLM 只做最后 20% 的定性判断。即使断网，本地规则也能保命（Risk Agent 是硬代码）。
- **优化**：可以通过不断完善 Data 和 Pattern Agent 的规则（增加更多数据源），提高 Strategy Agent 的判断质量，而无需修改核心 LLM。

## 六、下一步

1. **Phase 0**: 搭建 Agent 框架基础代码 (`polaris/agents/`)。
2. **Phase 1**: 实现 Data 和 Market Agent（纯规则，无 LLM）。
3. **Phase 2**: 接入 Pattern Agent (LLM 分析推文) 和 Strategy Agent (LLM 决策)。
4. **Phase 3**: 实盘运行，OpenClaw 作为旁路监控。
