import pytest

from polaris.harvest.discovery import discover_target_markets, is_elon_tweet_market, is_market_in_state, is_open_market


def test_market_filter_accepts_target() -> None:
    assert is_elon_tweet_market(
        "Will Elon Musk post 240+ tweets from February 2 to February 4, 2026?",
        "elon-musk-of-tweets-february-2-february-4-240plus",
    )


def test_market_filter_rejects_non_target() -> None:
    assert not is_elon_tweet_market(
        "Will BTC be above 120k by Friday?",
        "btc-above-120k-by-friday",
    )


def test_open_market_filter() -> None:
    assert is_open_market({"active": True, "closed": False, "archived": False})
    assert not is_open_market({"active": False, "closed": False, "archived": False})


def test_market_state_filter() -> None:
    row = {"active": False, "closed": True, "archived": False}
    assert is_market_in_state(row, "all")
    assert not is_market_in_state(row, "open")
    assert not is_market_in_state({"active": True, "closed": False, "archived": True}, "open")


class _FakeGammaClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def iter_markets(self, page_size: int = 500, max_pages: int | None = None):
        self.calls.append((page_size, max_pages))
        return self.rows


@pytest.mark.asyncio
async def test_discover_target_markets_scope_and_state() -> None:
    rows = [
        {
            "question": "Will Elon Musk post 200+ tweets?",
            "slug": "elon-tweet-200",
            "active": True,
            "closed": False,
            "archived": False,
        },
        {
            "question": "Will Elon Musk post 300+ tweets?",
            "slug": "elon-tweet-300",
            "active": False,
            "closed": True,
            "archived": False,
        },
        {
            "question": "Will BTC be above 120k?",
            "slug": "btc-120k",
            "active": True,
            "closed": False,
            "archived": False,
        },
    ]
    client = _FakeGammaClient(rows)

    open_elon = await discover_target_markets(
        client,
        scope="elon_tweet",
        state="open",
        page_size=100,
        max_pages=10,
    )
    assert len(open_elon) == 1
    assert open_elon[0]["slug"] == "elon-tweet-200"

    all_elon = await discover_target_markets(client, scope="elon_tweet", state="all")
    assert len(all_elon) == 2

    all_scope_all_state = await discover_target_markets(client, scope="all", state="all")
    assert len(all_scope_all_state) == 3
    assert client.calls[0] == (100, 10)
