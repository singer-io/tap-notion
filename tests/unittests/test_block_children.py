"""
Unit tests for BlockChildren stream recursive sync behavior.
"""

import pytest
from unittest.mock import MagicMock, patch, call
from tap_notion.streams.block_children import BlockChildren


@pytest.fixture
def mock_catalog():
    catalog = MagicMock()
    catalog.schema.to_dict.return_value = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "block_id": {"type": "string"},
            "has_children": {"type": "boolean"},
        }
    }
    catalog.metadata = []
    return catalog


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.base_url = "https://api.notion.com/v1"
    client.config = {"start_date": "2024-01-01T00:00:00Z"}
    return client


@patch("tap_notion.streams.block_children.metrics.record_counter")
@patch("tap_notion.streams.block_children.write_record")
def test_sync_no_children(mock_write_record, mock_counter, mock_catalog, mock_client):
    """Blocks with has_children=False are emitted but not recursed into."""
    mock_client.get.return_value = {
        "results": [{"id": "block-1", "has_children": False}],
        "next_cursor": None,
    }
    counter_inst = MagicMock()
    counter_inst.__enter__.return_value = counter_inst
    counter_inst.__exit__.return_value = False
    counter_inst.value = 1
    mock_counter.return_value = counter_inst

    stream = BlockChildren(client=mock_client, catalog=mock_catalog)
    stream.is_selected = MagicMock(return_value=True)
    transformer = MagicMock()
    transformer.transform.side_effect = lambda r, s, m: r

    stream.sync(state={}, transformer=transformer, parent_obj={"id": "parent-block"})

    assert mock_write_record.call_count == 1
    # Only one GET request — no recursion
    assert mock_client.get.call_count == 1


@patch("tap_notion.streams.block_children.metrics.record_counter")
@patch("tap_notion.streams.block_children.write_record")
def test_sync_recurses_into_nested_children(mock_write_record, mock_counter, mock_catalog, mock_client):
    """Blocks with has_children=True trigger a recursive sync call."""
    # First call returns a child block that itself has children
    # Second call (recursive) returns a grandchild with no further children
    mock_client.get.side_effect = [
        {"results": [{"id": "child-1", "has_children": True}], "next_cursor": None},
        {"results": [{"id": "grandchild-1", "has_children": False}], "next_cursor": None},
    ]

    counter_inst = MagicMock()
    counter_inst.__enter__.return_value = counter_inst
    counter_inst.__exit__.return_value = False
    counter_inst.value = 1
    mock_counter.return_value = counter_inst

    stream = BlockChildren(client=mock_client, catalog=mock_catalog)
    stream.is_selected = MagicMock(return_value=True)
    transformer = MagicMock()
    transformer.transform.side_effect = lambda r, s, m: r

    stream.sync(state={}, transformer=transformer, parent_obj={"id": "parent-block"})

    # Both child and grandchild should be written
    assert mock_write_record.call_count == 2
    # Two GET requests: one for the child level, one for the grandchild level
    assert mock_client.get.call_count == 2


@patch("tap_notion.streams.block_children.metrics.record_counter")
@patch("tap_notion.streams.block_children.write_record")
def test_sync_grandchild_has_correct_block_id(mock_write_record, mock_counter, mock_catalog, mock_client):
    """Each record's block_id should reference its direct parent, not the top-level parent."""
    mock_client.get.side_effect = [
        {"results": [{"id": "child-1", "has_children": True}], "next_cursor": None},
        {"results": [{"id": "grandchild-1", "has_children": False}], "next_cursor": None},
    ]

    counter_inst = MagicMock()
    counter_inst.__enter__.return_value = counter_inst
    counter_inst.__exit__.return_value = False
    counter_inst.value = 1
    mock_counter.return_value = counter_inst

    stream = BlockChildren(client=mock_client, catalog=mock_catalog)
    stream.is_selected = MagicMock(return_value=True)
    transformer = MagicMock()
    transformer.transform.side_effect = lambda r, s, m: r

    stream.sync(state={}, transformer=transformer, parent_obj={"id": "parent-block"})

    written_records = [c.args[1] for c in mock_write_record.call_args_list]
    child_record = next(r for r in written_records if r["id"] == "child-1")
    grandchild_record = next(r for r in written_records if r["id"] == "grandchild-1")

    assert child_record["block_id"] == "parent-block"
    assert grandchild_record["block_id"] == "child-1"


@patch("tap_notion.streams.block_children.metrics.record_counter")
@patch("tap_notion.streams.block_children.write_record")
def test_sync_not_selected_skips_write_but_still_recurses(mock_write_record, mock_counter, mock_catalog, mock_client):
    """Even when the stream is not selected, recursion should still occur (matching base class behavior)."""
    mock_client.get.side_effect = [
        {"results": [{"id": "child-1", "has_children": True}], "next_cursor": None},
        {"results": [{"id": "grandchild-1", "has_children": False}], "next_cursor": None},
    ]

    counter_inst = MagicMock()
    counter_inst.__enter__.return_value = counter_inst
    counter_inst.__exit__.return_value = False
    counter_inst.value = 0
    mock_counter.return_value = counter_inst

    stream = BlockChildren(client=mock_client, catalog=mock_catalog)
    stream.is_selected = MagicMock(return_value=False)
    transformer = MagicMock()
    transformer.transform.side_effect = lambda r, s, m: r

    stream.sync(state={}, transformer=transformer, parent_obj={"id": "parent-block"})

    mock_write_record.assert_not_called()
    assert mock_client.get.call_count == 2
