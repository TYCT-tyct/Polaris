from polaris.harvest.discovery import is_elon_tweet_market, is_open_market


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
