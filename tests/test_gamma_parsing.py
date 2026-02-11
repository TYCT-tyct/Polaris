import pytest
import httpx

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


class _FakeGammaClient(GammaClient):
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    async def fetch_markets_page(self, limit: int, offset: int):
        self.calls.append((limit, offset))
        page_index = offset // limit
        if page_index >= len(self.pages):
            return []
        return self.pages[page_index]


@pytest.mark.asyncio
async def test_iter_markets_without_fixed_page_cap() -> None:
    client = _FakeGammaClient(
        pages=[
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
            [{"id": 5}],
        ]
    )
    rows = await client.iter_markets(page_size=2, max_pages=None)
    assert [row["id"] for row in rows] == [1, 2, 3, 4, 5]
    assert client.calls == [(2, 0), (2, 2), (2, 4)]


class _FlakyGammaClient(GammaClient):
    def __init__(self):
        self.calls = []
        self._raised = False
        self._rows = {
            0: [{"id": 1}, {"id": 2}],
            2: [{"id": 3}],
        }

    async def fetch_markets_page(self, limit: int, offset: int):
        self.calls.append((limit, offset))
        if offset == 0 and limit >= 4 and not self._raised:
            self._raised = True
            raise httpx.RemoteProtocolError("incomplete chunked read")
        return self._rows.get(offset, [])


@pytest.mark.asyncio
async def test_iter_markets_reduces_page_size_on_chunked_read() -> None:
    client = _FlakyGammaClient()
    rows = await client.iter_markets(page_size=20, max_pages=None)
    assert [row["id"] for row in rows] == [1, 2]
    assert client.calls[0] == (20, 0)
    assert client.calls[1] == (10, 0)
