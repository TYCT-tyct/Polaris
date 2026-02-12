from __future__ import annotations

from polaris.sources.clob_client import _parse_prices_response


def test_parse_prices_response_dict_payload() -> None:
    payload = {
        "tok-a": {"BUY": "0.41", "SELL": "0.39"},
        "tok-b": {"BUY": 0.55, "SELL": 0.53},
    }
    parsed = _parse_prices_response(payload, ["tok-a", "tok-b"])
    assert parsed["tok-a"] == (0.39, 0.41)
    assert parsed["tok-b"] == (0.53, 0.55)


def test_parse_prices_response_list_payload_with_fallback_token_id() -> None:
    payload = [
        {"BUY": "0.22", "SELL": "0.20"},
        {"token_id": "tok-b", "BUY": "0.62", "SELL": "0.60"},
    ]
    parsed = _parse_prices_response(payload, ["tok-a", "tok-b"])
    assert parsed["tok-a"] == (0.20, 0.22)
    assert parsed["tok-b"] == (0.60, 0.62)
