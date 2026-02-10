from datetime import UTC, datetime

from polaris.harvest.collector_tweets import _collect_new_posts, to_post_row
from polaris.sources.models import XTrackerPost


def _post(pid: str, content: str) -> XTrackerPost:
    return XTrackerPost.model_validate(
        {
            "id": f"src-{pid}",
            "userId": "account-1",
            "platformId": pid,
            "content": content,
            "createdAt": datetime.now(tz=UTC).isoformat(),
            "importedAt": datetime.now(tz=UTC).isoformat(),
        }
    )


def test_collect_new_posts_until_last_seen() -> None:
    posts = [_post("300", "a"), _post("200", "b"), _post("100", "c")]
    fresh = _collect_new_posts(posts, 200)
    assert [p.platform_post_id for p in fresh] == ["300"]


def test_to_post_row_derivation() -> None:
    post = _post("400", "RT hello https://t.co/x")
    row = to_post_row(post, "account-1")
    assert row[0] == "400"
    assert row[6] is False
    assert row[7] is True
    assert row[8] is True
    assert row[10] > 0

