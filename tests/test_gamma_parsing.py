from polaris.sources.gamma_client import GammaClient


def test_token_descriptor_parsing() -> None:
    market = {
        "id": 123,
        "outcomes": '["Yes","No"]',
        "clobTokenIds": '["111","222"]',
    }
    tokens = GammaClient.token_descriptors(market)
    assert len(tokens) == 2
    assert tokens[0].token_id == "111"
    assert tokens[0].outcome_label == "Yes"
    assert tokens[0].outcome_side == "YES"
    assert tokens[1].outcome_side == "NO"

