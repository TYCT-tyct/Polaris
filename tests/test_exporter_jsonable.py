from __future__ import annotations

from uuid import uuid4

from polaris.ops.exporter import _to_jsonable


def test_to_jsonable_supports_uuid_values() -> None:
    value = uuid4()
    assert _to_jsonable(value) == str(value)
