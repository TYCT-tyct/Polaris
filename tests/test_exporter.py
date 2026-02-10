from __future__ import annotations

import json

import pytest

from polaris.ops.exporter import export_table, list_exportable_tables


@pytest.mark.asyncio
async def test_export_table_csv_and_json(db, tmp_path) -> None:
    account_id = "test_export_account"
    handle = "test_export_handle"
    await db.execute(
        """
        insert into dim_account(account_id, handle, created_at, updated_at)
        values (%s, %s, now(), now())
        on conflict (account_id) do update
        set handle = excluded.handle,
            updated_at = now()
        """,
        (account_id, handle),
    )

    json_path = tmp_path / "dim_account.json"
    csv_path = tmp_path / "dim_account.csv"

    json_result = await export_table(
        db=db,
        table="dim_account",
        export_format="json",
        output_path=json_path,
        limit=1000,
    )
    csv_result = await export_table(
        db=db,
        table="dim_account",
        export_format="csv",
        output_path=csv_path,
        limit=1000,
    )

    assert json_result.rows >= 1
    assert csv_result.rows >= 1
    assert json_path.exists()
    assert csv_path.exists()

    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert any(row["account_id"] == account_id for row in data)

    csv_text = csv_path.read_text(encoding="utf-8-sig")
    assert "account_id" in csv_text.splitlines()[0]
    assert account_id in csv_text


@pytest.mark.asyncio
async def test_export_table_invalid_table(db, tmp_path) -> None:
    with pytest.raises(ValueError):
        await export_table(
            db=db,
            table="not_a_table",
            export_format="json",
            output_path=tmp_path / "x.json",
        )


def test_exportable_tables_include_latest_view() -> None:
    tables = list_exportable_tables()
    assert "view_quote_latest" in tables
