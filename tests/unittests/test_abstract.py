"""
Unit tests for validating IncrementalStream and FullTableStream abstract base classes.
Includes tests for:
- Bookmark handling
- URL formatting
- Sync process and record writing
"""

import pytest
from tap_notion.streams.user import User
from unittest.mock import MagicMock, patch
from tap_notion.streams.abstracts import IncrementalStream, FullTableStream


class DummyIncrementalStream(IncrementalStream):
    tap_stream_id = "dummy_incremental"
    replication_keys = ["updatedAt"]
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    path = "pages"
    data_key = "results"


class DummyFullTableStream(FullTableStream):
    tap_stream_id = "dummy_full"
    replication_keys = []
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "blocks"
    data_key = "results"


@pytest.fixture
def dummy_catalog():
    mock_catalog = MagicMock()
    mock_catalog.schema.to_dict.return_value = {
        "type": "object",
        "properties": {"id": {"type": "string"}}
    }
    mock_catalog.metadata = []
    return mock_catalog


@pytest.fixture
def dummy_client():
    client = MagicMock()
    client.base_url = "https://api.notion.com/v1"
    client.get.return_value = {
        "results": [{"id": "1", "updatedAt": "2025-01-01T00:00:00Z"}],
        "next_cursor": None
    }
    client.config = {"start_date": "2024-01-01T00:00:00Z"}
    return client


def test_get_bookmark_returns_state_bookmark(dummy_catalog, dummy_client):
    stream = DummyIncrementalStream(client=dummy_client, catalog=dummy_catalog)
    state = {"bookmarks": {"dummy_incremental": {"updatedAt": "2025-01-01T00:00:00Z"}}}
    result = stream.get_bookmark(state, "dummy_incremental")
    assert result == "2025-01-01T00:00:00Z"


def test_get_bookmark_uses_config_start_date(dummy_catalog, dummy_client):
    stream = DummyIncrementalStream(client=dummy_client, catalog=dummy_catalog)
    state = {}  # no bookmarks
    result = stream.get_bookmark(state, "dummy_incremental")
    assert result == "2024-01-01T00:00:00Z"


def test_get_url_endpoint(dummy_catalog, dummy_client):
    """
    Test retrieval of the URL endpoint.
    This test ensures that the function responsible for getting the URL 
    endpoint works as expected.
    """
    stream = DummyFullTableStream(client=dummy_client, catalog=dummy_catalog)
    expected = "https://api.notion.com/v1/blocks"
    result = stream.get_url_endpoint()
    assert result == expected


def test_get_url_endpoint_static_path(dummy_catalog, dummy_client):
    """
    Test: get_url_endpoint should return correct full URL when path is static.
    """
    class BlocksStream(DummyFullTableStream):
        path = "blocks"

    stream = BlocksStream(client=dummy_client, catalog=dummy_catalog)
    expected = "https://api.notion.com/v1/blocks"
    result = stream.get_url_endpoint()
    assert result == expected


@patch("tap_notion.streams.abstracts.metrics.record_counter")
@patch("tap_notion.streams.abstracts.write_record")
@patch("tap_notion.streams.abstracts.Transformer")
def test_full_table_stream_sync(mock_transformer, mock_write_record, mock_counter, dummy_catalog, dummy_client):
    """
    Test full table stream synchronization.
    This test verifies the behavior of the full table stream sync process.
    """
    dummy_client.get.return_value = {
        "results": [{"id": "1"}, {"id": "2"}],
        "next_cursor": None
    }

    # Set up the mocked counter
    mock_counter_inst = MagicMock()
    mock_counter_inst.__enter__.return_value = mock_counter_inst
    mock_counter_inst.__exit__.return_value = False
    mock_counter_inst.value = 2
    mock_counter.return_value = mock_counter_inst

    # Set up the stream and transformer
    stream = User(client=dummy_client, catalog=dummy_catalog)
    stream.data_key = "results"
    stream.is_selected = MagicMock(return_value=True)

    transformer = MagicMock()
    transformer.transform.side_effect = lambda record, schema, metadata: record
    mock_transformer.return_value = transformer

    # Provide parent_obj with an 'id' to avoid KeyError
    parent_obj = {"id": "user_123"}

    # Run sync and assert
    state = {}
    count = stream.sync(state=state, transformer=transformer, parent_obj=parent_obj)
    assert count == 2
    assert mock_write_record.call_count == 2

@patch("tap_notion.streams.abstracts.metrics.record_counter")
@patch("tap_notion.streams.abstracts.write_bookmark")
@patch("tap_notion.streams.abstracts.write_record")
@patch("tap_notion.streams.abstracts.Transformer")
def test_incremental_stream_sync(mock_transformer, mock_write_record, mock_write_bookmark, mock_counter, dummy_catalog, dummy_client):
    """
    Test: Verifies that DummyIncrementalStream.sync correctly writes records and updates bookmarks.
    Ensures that:
    - Only selected streams are processed.
    - Records are written using the provided transformer.
    - Bookmarks are updated after syncing.
    - The record counter is used to track the number of records processed.
    """
    stream = DummyIncrementalStream(client=dummy_client, catalog=dummy_catalog)
    stream.is_selected = MagicMock(return_value=True)

    mock_counter_inst = MagicMock()
    mock_counter_inst.__enter__.return_value = mock_counter_inst
    mock_counter_inst.__exit__.return_value = False
    mock_counter_inst.value = 1
    mock_counter.return_value = mock_counter_inst

    dummy_client.get.return_value = {
        "results": [{"id": "1", "updatedAt": "2025-01-01T00:00:00Z"}],
        "next_cursor": None
    }

    transformer = MagicMock()
    transformer.transform.side_effect = lambda r, s, m: r
    mock_transformer.return_value = transformer

    state = {}
    count = stream.sync(state=state, transformer=transformer)
    assert count == 1
    assert mock_write_record.call_count == 1
    mock_write_bookmark.assert_called_once()
