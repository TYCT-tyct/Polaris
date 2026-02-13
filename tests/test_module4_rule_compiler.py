from __future__ import annotations

from polaris.core.module4.rule_compiler import compile_rule_policy


def test_compile_rule_policy_parses_common_constraints() -> None:
    description = """
    Resolution source: XTracker Post Counter.
    Main feed posts, quote posts and reposts count.
    Replies do not count.
    Deleted posts count if captured by the tracker.
    """
    policy = compile_rule_policy(description)
    assert policy.resolution_source == "xtracker"
    assert policy.include_main is True
    assert policy.include_quote is True
    assert policy.include_repost is True
    assert policy.include_reply is False
    assert policy.count_deleted_if_captured is True

