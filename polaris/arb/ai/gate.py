from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import AiGateDecision, ArbSignal, StrategyCode
from polaris.arb.ai.provider_anthropic import AnthropicProvider
from polaris.arb.ai.provider_google import GoogleProvider
from polaris.arb.ai.provider_minimax import MiniMaxProvider
from polaris.arb.ai.provider_openai import OpenAIProvider
from polaris.arb.ai.provider_zhipu import ZhipuProvider
from polaris.config import PolarisSettings


@dataclass(slots=True)
class AiGate:
    config: ArbConfig
    providers: dict[str, object]

    @classmethod
    def from_settings(cls, settings: PolarisSettings, config: ArbConfig) -> "AiGate":
        providers: dict[str, object] = {
            "google": GoogleProvider(settings.arb_google_api_key, settings.arb_google_model),
            "anthropic": AnthropicProvider(settings.arb_anthropic_api_key, settings.arb_anthropic_model),
            "openai": OpenAIProvider(settings.arb_openai_api_key, settings.arb_openai_model),
            "minimax": MiniMaxProvider(settings.arb_minimax_api_key, settings.arb_minimax_model),
            "zhipu": ZhipuProvider(settings.arb_zhipu_api_key, settings.arb_zhipu_model),
        }
        return cls(config=config, providers=providers)

    async def evaluate(self, signal: ArbSignal, context: dict) -> AiGateDecision:
        if not self.config.ai_enabled or signal.strategy_code != StrategyCode.G:
            return AiGateDecision(True, 1.0, "disabled", {}, "ai_gate_disabled")

        prompt = _build_prompt(signal, context)
        order = [name for name in self.config.ai_provider_order if name in self.providers]
        enabled = [name for name in order if getattr(self.providers[name], "is_configured")()]
        if not enabled:
            return AiGateDecision(True, 1.0, "disabled", {}, "no_provider_configured")

        if self.config.ai_mode == "single_model":
            model = self.config.ai_single_model.lower().strip()
            if model not in self.providers or not getattr(self.providers[model], "is_configured")():
                return AiGateDecision(True, 1.0, "single_model", {}, "configured_model_unavailable")
            allow, confidence, reason = await getattr(self.providers[model], "evaluate")(prompt)
            return AiGateDecision(bool(allow), float(confidence), "single_model", {model: bool(allow)}, reason)

        selected = enabled[:3]
        tasks = [getattr(self.providers[name], "evaluate")(prompt) for name in selected]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        votes: dict[str, bool] = {}
        confidences: list[float] = []
        reasons: list[str] = []
        for name, result in zip(selected, results):
            if isinstance(result, Exception):
                votes[name] = False
                reasons.append(f"{name}:{type(result).__name__}")
                continue
            allow, confidence, reason = result
            votes[name] = bool(allow)
            confidences.append(float(confidence))
            reasons.append(f"{name}:{reason}")

        yes = sum(1 for vote in votes.values() if vote)
        allow = yes >= max(1, self.config.ai_quorum)
        confidence = sum(confidences) / len(confidences) if confidences else 0.0
        return AiGateDecision(allow, confidence, "cascade_quorum", votes, "; ".join(reasons))


def _build_prompt(signal: ArbSignal, context: dict) -> str:
    return (
        "你是预测市场风控审查器。根据信号与市场上下文，只输出 JSON。\n"
        "JSON: {\"allow\":bool,\"confidence\":float,\"reason\":string}\n"
        f"signal={json.dumps(signal.features, ensure_ascii=False)}\n"
        f"meta={{\"strategy\":\"{signal.strategy_code.value}\",\"edge_pct\":{signal.edge_pct}}}\n"
        f"context={json.dumps(context, ensure_ascii=False)}\n"
        "规则：只有当该信号在短期内高概率收敛且流动性足够时 allow=true。"
    )
