import os

import requests

from base import NotionBaseTest
from tap_tester import connections, runner

NOTION_API_BASE = "https://api.notion.com/v1"
# Keep this local so the test does not depend on importing tap_notion package.
NOTION_VERSION = os.getenv("TAP_NOTION_VERSION", "2025-09-03")


class NotionNestedHierarchyTest(NotionBaseTest):
    """
        Dynamically creates a branched 3-level nested page hierarchy in Notion, runs a
    sync, validates that all levels are replicated, then archives the test
    data.

    Hierarchy created:
                page
                    └─ text_1
                             ├─ text_1_child1
                             └─ text_1_child2
                                        ├─ text1_child2_grandchild1
                                        └─ text1_child2_grandchild2

    This lives in tap-notion for now because nested grandchild coverage is
    needed only for taps that expose this hierarchy pattern.
    """

    STREAMS_TO_SYNC = {"pages", "blocks", "block_children"}
    PARENT_PAGE_ID = "38a0dbd4-4275-80c5-a531-e55f1eaba109"

    @staticmethod
    def name():
        return "tap_tester_notion_nested_hierarchy_test"

    @staticmethod
    def streams_to_selected_fields():
        return {}

    @staticmethod
    def _normalize_id(value):
        """Normalize Notion ids so dashed/undashed forms compare equally."""
        if not value:
            return ""
        return str(value).replace("-", "").lower()

    # Notion API helpers (used only for test data management)

    def _notion_headers(self):
        token = os.getenv("TAP_NOTION_AUTH_TOKEN")
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _bulleted_list_item_block(self, text):
        """Return a minimal bulleted list item block payload."""
        return {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text}}]
            },
        }



    def _append_block(self, parent_block_id, text):
        """Append one bulleted list item block to parent_block_id and return its id."""
        resp = requests.patch(
            f"{NOTION_API_BASE}/blocks/{parent_block_id}/children",
            headers=self._notion_headers(),
            json={"children": [self._bulleted_list_item_block(text)]},
        )
        resp.raise_for_status()
        return resp.json()["results"][0]["id"]

    def _archive_block(self, block_id):
        """Archive (soft-delete) a block and all its nested content."""
        resp = requests.patch(
            f"{NOTION_API_BASE}/blocks/{block_id}",
            headers=self._notion_headers(),
            json={"archived": True},
        )
        resp.raise_for_status()

    # Validating the nested hierarchy replication
    def test_parent_child_grandchild_replication(self):
        """
        1. Dynamically create a 3-level branched Notion hierarchy under a fixed parent page.
        2. Sync pages / blocks / block_children.
        3. Assert every level is present in the target output with correct
           primary keys and parent-child linkage.
        4. Archive the top-level test block (cleanup runs even on failure).
        """
        # --- set up connection first ------------------------------------
        # Establish connection and bookmark BEFORE creating test data so the
        # sync captures all newly created records in its window.
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [
            c for c in found_catalogs if c.get("stream_name") in self.STREAMS_TO_SYNC
        ]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        # --- create test data -------------------------------------------
        # Use fixed pre-indexed parent page; create all blocks under it.
        # No wait needed because parent is already indexed and blocks are fetched by ID.
        page_id = self.PARENT_PAGE_ID

        text_1_id = self._append_block(page_id, "text_1")
        self.addCleanup(self._archive_block, text_1_id)

        text_1_child1_id = self._append_block(text_1_id, "text_1_child1")
        text_1_child2_id = self._append_block(text_1_id, "text_1_child2")

        # Create third-level grandchildren.
        text_1_child2_grandchild1_id = self._append_block(
            text_1_child2_id,
            "text1_child2_grandchild1",
        )
        text_1_child2_grandchild2_id = self._append_block(
            text_1_child2_id,
            "text1_child2_grandchild2",
        )

        # --- tap-tester sync (no wait; parent is pre-indexed, blocks fetched by ID) --------------------------------------------
        # --- extract upsert payloads ------------------------------------
        def upserts(synced_records, stream_name):
            messages = synced_records.get(stream_name, {}).get("messages", [])
            upsert_messages = [
                msg for msg in messages
                if isinstance(msg, dict) and msg.get("action") == "upsert"
            ]
            invalid_upserts = [
                msg for msg in upsert_messages if not isinstance(msg.get("data"), dict)
            ]
            self.assertEqual(
                invalid_upserts,
                [],
                f"Expected all upsert messages in {stream_name} to have dict 'data' payload.",
            )
            return [msg["data"] for msg in upsert_messages]

        self.run_and_verify_sync_mode(conn_id)
        synced_records = runner.get_records_from_target_output()

        pages = upserts(synced_records, "pages")
        blocks = upserts(synced_records, "blocks")
        block_children = upserts(synced_records, "block_children")

        # --- assert primary keys present -------
        pages_missing_pk = [r for r in pages if not r.get("id")]
        self.assertEqual(pages_missing_pk, [], "Every page record must have 'id'.")

        blocks_missing_pk = [r for r in blocks if not r.get("id")]
        self.assertEqual(blocks_missing_pk, [], "Every block record must have 'id'.")

        block_children_missing_pk = [
            r for r in block_children if not r.get("id") or not r.get("block_id")
        ]
        self.assertEqual(
            block_children_missing_pk,
            [],
            "Every block_children record must have 'id' and 'block_id'.",
        )

        # --- assert parent-child linkage at each level -------
        # Level 1: text_1 is a direct child of the page → lands in blocks stream.
        l1_blocks = [
            r for r in blocks
            if self._normalize_id(r.get("id")) == self._normalize_id(text_1_id)
        ]
        self.assertTrue(l1_blocks, f"Top-level block {text_1_id} not found in blocks stream.")
        self.assertEqual(
            self._normalize_id(l1_blocks[0].get("page_id")),
            self._normalize_id(page_id),
            f"Top-level block must have page_id={page_id}.",
        )

        # Level 2: both child blocks are in block_children linked to text_1.
        level_2_records = {
            self._normalize_id(r.get("id")): r
            for r in block_children
            if self._normalize_id(r.get("id")) in {
                self._normalize_id(text_1_child1_id),
                self._normalize_id(text_1_child2_id),
            }
        }
        self.assertEqual(
            set(level_2_records.keys()),
            {
                self._normalize_id(text_1_child1_id),
                self._normalize_id(text_1_child2_id),
            },
            "Expected both second-level child blocks to be replicated in block_children.",
        )
        self.assertEqual(
            self._normalize_id(
                level_2_records[self._normalize_id(text_1_child1_id)].get("block_id")
            ),
            self._normalize_id(text_1_id),
            f"text_1_child1 must have block_id={text_1_id}.",
        )
        self.assertEqual(
            self._normalize_id(
                level_2_records[self._normalize_id(text_1_child2_id)].get("block_id")
            ),
            self._normalize_id(text_1_id),
            f"text_1_child2 must have block_id={text_1_id}.",
        )

        # Level 3: both grandchildren are in block_children linked to text_1_child2.
        level_3_records = {
            self._normalize_id(r.get("id")): r
            for r in block_children
            if self._normalize_id(r.get("id")) in {
                self._normalize_id(text_1_child2_grandchild1_id),
                self._normalize_id(text_1_child2_grandchild2_id),
            }
        }
        self.assertEqual(
            set(level_3_records.keys()),
            {
                self._normalize_id(text_1_child2_grandchild1_id),
                self._normalize_id(text_1_child2_grandchild2_id),
            },
            "Expected both third-level grandchild blocks to be replicated in block_children.",
        )
        self.assertEqual(
            self._normalize_id(
                level_3_records[self._normalize_id(text_1_child2_grandchild1_id)].get("block_id")
            ),
            self._normalize_id(text_1_child2_id),
            f"text1_child2_grandchild1 must have block_id={text_1_child2_id}.",
        )
        self.assertEqual(
            self._normalize_id(
                level_3_records[self._normalize_id(text_1_child2_grandchild2_id)].get("block_id")
            ),
            self._normalize_id(text_1_child2_id),
            f"text1_child2_grandchild2 must have block_id={text_1_child2_id}.",
        )