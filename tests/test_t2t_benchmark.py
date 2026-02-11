from __future__ import annotations

from polaris.arb.bench.t2t import generate_t2t_payload, run_python_t2t_benchmark


def test_t2t_payload_generation_shape() -> None:
    payload = generate_t2t_payload(levels_per_side=50, updates=200, iterations=10, seed=7)
    assert int(payload["iterations"]) == 10
    assert len(payload["initial_bids"]) == 50
    assert len(payload["initial_asks"]) == 50
    assert len(payload["updates"]) == 200


def test_python_t2t_benchmark_outputs_metrics() -> None:
    payload = generate_t2t_payload(levels_per_side=30, updates=120, iterations=4, seed=11)
    report = run_python_t2t_benchmark(payload)
    assert int(report["total_updates"]) == 480
    assert float(report["avg_update_us"]) > 0
    assert float(report["p95_update_us"]) >= float(report["p50_update_us"])
    assert float(report["total_ms"]) >= float(report["decode_ms"])
