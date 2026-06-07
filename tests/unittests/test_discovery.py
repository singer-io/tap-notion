"""
Unit tests for discovery access checks.
Validates:
- All streams accessible → full catalog returned
- Some streams return 403 → excluded from catalog
- All parent streams return 403 → raises NotionForbiddenError
- Child streams excluded when parent is inaccessible
"""

import pytest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_notion.discover import discover, _apply_access_checks, _prune_inaccessible_children
from tap_notion.exceptions import NotionForbiddenError
from tap_notion.streams import STREAMS


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.base_url = "https://api.notion.com/v1"
    client.config = {"auth_token": "test_token", "start_date": "2023-01-01T00:00:00Z"}
    client.headers = {
        "Authorization": "Bearer test_token",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }
    client.get.return_value = {"results": [], "next_cursor": None}
    client.post.return_value = {"results": [], "has_more": False}
    return client


@pytest.fixture
def mock_schemas():
    """Return a minimal schemas dict with all streams."""
    return {name: {"type": "object", "properties": {}} for name in STREAMS}


@pytest.fixture
def mock_field_metadata():
    """Return minimal field_metadata dict with all streams."""
    return {name: [] for name in STREAMS}


class TestApplyAccessChecks:
    """Tests for _apply_access_checks function."""

    def test_all_streams_accessible(self, mock_client, mock_schemas, mock_field_metadata):
        """When all streams are accessible, none are removed."""
        original_streams = set(mock_schemas.keys())
        _apply_access_checks(mock_client, mock_schemas, mock_field_metadata)
        assert set(mock_schemas.keys()) == original_streams

    def test_single_parent_stream_forbidden(self, mock_client, mock_schemas, mock_field_metadata):
        """When one parent stream returns 403, it's excluded from schemas."""
        def side_effect_get(url, params, headers, path=None):
            if "file_uploads" in (url or path or ""):
                raise NotionForbiddenError("403 Forbidden")
            return {"results": [], "next_cursor": None}

        mock_client.get.side_effect = side_effect_get

        _apply_access_checks(mock_client, mock_schemas, mock_field_metadata)

        assert "file_upload" not in mock_schemas
        assert "file_upload" not in mock_field_metadata

    def test_all_parent_streams_forbidden_raises(self, mock_client, mock_schemas, mock_field_metadata):
        """When ALL parent streams return 403, raise NotionForbiddenError."""
        mock_client.get.side_effect = NotionForbiddenError("403 Forbidden")
        mock_client.post.side_effect = NotionForbiddenError("403 Forbidden")

        with pytest.raises(NotionForbiddenError):
            _apply_access_checks(mock_client, mock_schemas, mock_field_metadata)

    def test_child_streams_excluded_when_parent_inaccessible(self, mock_client, mock_schemas, mock_field_metadata):
        """Child streams are removed when their parent is excluded."""
        # Make pages inaccessible via POST (pages uses /search)
        def side_effect_post(url, headers, body, params=None, path=None):
            if "search" in (url or ""):
                raise NotionForbiddenError("403 Forbidden")
            return {"results": [], "has_more": False}

        mock_client.post.side_effect = side_effect_post

        _apply_access_checks(mock_client, mock_schemas, mock_field_metadata)

        # pages excluded
        assert "pages" not in mock_schemas
        # data_sources also uses /search, so it's excluded too
        assert "data_sources" not in mock_schemas
        # children of pages should be excluded
        assert "blocks" not in mock_schemas
        assert "page_property" not in mock_schemas
        # grandchildren of pages (children of blocks) should also be excluded
        assert "block_children" not in mock_schemas
        assert "comments" not in mock_schemas


class TestPruneInaccessibleChildren:
    """Tests for _prune_inaccessible_children function."""

    def test_children_pruned_when_parent_missing(self, mock_schemas, mock_field_metadata):
        """Children are removed if parent is not in schemas."""
        # Remove parent "pages"
        mock_schemas.pop("pages")
        mock_field_metadata.pop("pages")

        _prune_inaccessible_children(mock_schemas, mock_field_metadata)

        # blocks has parent="pages", so it should be removed
        assert "blocks" not in mock_schemas
        # page_property has parent="pages"
        assert "page_property" not in mock_schemas

    def test_grandchildren_pruned(self, mock_schemas, mock_field_metadata):
        """Grandchildren excluded when parent is removed (cascading)."""
        # Remove "blocks" (parent of block_children and comments)
        mock_schemas.pop("blocks")
        mock_field_metadata.pop("blocks")

        _prune_inaccessible_children(mock_schemas, mock_field_metadata)

        assert "block_children" not in mock_schemas
        assert "comments" not in mock_schemas

    def test_no_pruning_when_all_parents_present(self, mock_schemas, mock_field_metadata):
        """No children removed when all parents are present."""
        original = set(mock_schemas.keys())
        _prune_inaccessible_children(mock_schemas, mock_field_metadata)
        assert set(mock_schemas.keys()) == original


class TestDiscover:
    """Tests for the discover() function end-to-end."""

    @patch("tap_notion.discover.get_schemas")
    def test_discover_returns_catalog(self, mock_get_schemas, mock_client):
        """discover() returns a valid Catalog object."""
        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {"id": {"type": "string"}}} for name in STREAMS},
            {name: [{"breadcrumb": (), "metadata": {"table-key-properties": ["id"]}}] for name in STREAMS},
        )

        catalog = discover(mock_client)
        assert isinstance(catalog, Catalog)
        assert len(catalog.streams) > 0

    @patch("tap_notion.discover.get_schemas")
    def test_discover_excludes_forbidden_streams(self, mock_get_schemas, mock_client):
        """discover() excludes streams that return 403."""
        mock_get_schemas.return_value = (
            {name: {"type": "object", "properties": {"id": {"type": "string"}}} for name in STREAMS},
            {name: [{"breadcrumb": (), "metadata": {"table-key-properties": ["id"]}}] for name in STREAMS},
        )

        def side_effect_get(url, params, headers, path=None):
            if "file_uploads" in (url or path or ""):
                raise NotionForbiddenError("403 Forbidden")
            return {"results": [], "next_cursor": None}

        mock_client.get.side_effect = side_effect_get

        catalog = discover(mock_client)
        stream_names = [s.tap_stream_id for s in catalog.streams]
        assert "file_upload" not in stream_names


class TestCheckAccess:
    """Tests for BaseStream.check_access() method."""

    def test_child_stream_always_returns_true(self, mock_client):
        """Child streams always return True without making API calls."""
        from tap_notion.streams.blocks import Blocks
        stream = Blocks(client=mock_client)
        assert stream.check_access() is True
        # No API call should be made
        mock_client.get.assert_not_called()
        mock_client.post.assert_not_called()

    def test_parent_stream_returns_true_on_success(self, mock_client):
        """Parent stream returns True when API call succeeds."""
        from tap_notion.streams.users import Users
        stream = Users(client=mock_client)
        assert stream.check_access() is True

    def test_parent_stream_returns_false_on_403(self, mock_client):
        """Parent stream returns False when API returns 403."""
        from tap_notion.streams.users import Users
        mock_client.get.side_effect = NotionForbiddenError("403 Forbidden")
        stream = Users(client=mock_client)
        assert stream.check_access() is False
