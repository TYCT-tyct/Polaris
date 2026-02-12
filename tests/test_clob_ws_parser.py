from __future__ import annotations

import json

from polaris.sources.clob_client import _extract_ws_books


def test_extract_ws_books_from_market_payload() -> None:
    payload = {
        "asset_id": "token-a",
        "market": "market-1",
        "bids": [{"price": "0.42", "size": "10"}],
        "asks": [{"price": "0.44", "size": "9"}],
        "timestamp": "1739300000000",
    }

    books = _extract_ws_books(json.dumps(payload))

    assert len(books) == 1
    assert books[0].asset_id == "token-a"
    assert books[0].bids[0].price == 0.42
    assert books[0].asks[0].size == 9.0


def test_extract_ws_books_from_nested_payload_with_buys_sells() -> None:
    payload = {
        "type": "market",
        "data": {
            "asset_id": "token-b",
            "buys": [["0.51", "8"], ["0.50", "11"]],
            "sells": [{"p": "0.53", "s": "7"}],
        },
    }

    books = _extract_ws_books(payload)

    assert len(books) == 1
    assert books[0].asset_id == "token-b"
    assert [level.price for level in books[0].bids] == [0.51, 0.5]
    assert [level.price for level in books[0].asks] == [0.53]


def test_extract_ws_books_ignores_non_book_messages() -> None:
    payload = {"event_type": "pong", "message": "ok"}

    books = _extract_ws_books(payload)

    assert books == []
