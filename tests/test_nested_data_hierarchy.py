from base import NotionBaseTest
from tap_tester import connections, runner


class NotionNestedHierarchyTest(NotionBaseTest):
    """
    Validate parent-child-grandchild replication integrity for Notion.

    This lives in tap-notion for now because nested grandchild coverage is
    currently needed only for taps that expose this hierarchy pattern.
    """

    @staticmethod
    def name():
        return "tap_tester_notion_nested_hierarchy_test"

    @staticmethod
    def streams_to_selected_fields():
        # Empty mapping means select all fields for selected streams.
        return {}

    def test_parent_child_grandchild_replication(self):
        """
        Verify hierarchical replication with this model:
        pages -> blocks (parents) -> block_children (descendants).

        In this tap, block_children contains child and grandchild-level records.

        Scenario expectation (simple):
        if some parent blocks have nested descendants, descendant records should
        be present in target output. If a child has no grandchildren, this test
        should still pass.
        """
        
        # Checking for relevant streams
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        streams_to_sync = {"pages", "blocks", "block_children"}
        test_catalogs = [catalog for catalog in found_catalogs
            if catalog.get("stream_name") in streams_to_sync
        ]

        # Verify we have expected streams in catalog & sync
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)
        self.run_and_verify_sync_mode(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Helper function to extract upsert messages and validate their structure.
        def upserts(stream_name):
            messages = synced_records.get(stream_name, {}).get("messages", [])
            upsert_messages = [
                msg for msg in messages if isinstance(msg, dict) and msg.get("action") == "upsert"
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

        pages = upserts("pages")
        blocks = upserts("blocks")
        block_children = upserts("block_children")

        self.assertGreater(len(pages), 0, "Expected at least one page record.")
        self.assertGreater(len(blocks), 0, "Expected at least one block record.")
        self.assertGreater(
            len(block_children),
            0,
            "Expected at least one block_children record.",
        )

        # Validating pages have primary keys and linked blocks
        pages_missing_primary_key = [record for record in pages if not record.get("id")]
        self.assertEqual(
            pages_missing_primary_key,
            [],
            "Expected every page record to include mandatory primary key field "
            "'id' ",
        )

        page_ids = {record["id"] for record in pages}
        page_blocks = [
            record for record in blocks if record.get("page_id") in page_ids
        ]

        self.assertGreater(
            len(page_blocks),0,"Expected at least one block linked to a replicated page.",
        )

        # Validating blocks have primary keys and linked block_children
        blocks_missing_primary_key = [
            record for record in page_blocks if not record.get("id")
        ]
        self.assertEqual(
            blocks_missing_primary_key,
            [],
            "Expected every parent block record to include mandatory primary key "
            "field 'id' ",
        )

        # Validating block_children have primary keys and linked parent blocks
        block_children_missing_primary_keys = [
            record
            for record in block_children
            if not record.get("id") or not record.get("block_id")
        ]
        self.assertEqual(
            block_children_missing_primary_keys,
            [],
            "Expected every block_children record to include mandatory key fields "
            "'id' and 'block_id' ",
        )

        # Building a mapping of parent blocks to their child blocks
        children_by_parent = {}
        for record in block_children:
            parent_id = record.get("block_id")
            child_id = record.get("id")
            children_by_parent.setdefault(parent_id, set()).add(child_id)

        parent_block_ids = {record["id"] for record in page_blocks}
        direct_child_ids = set()
        for parent_id in parent_block_ids:
            direct_child_ids.update(children_by_parent.get(parent_id, set()))

        self.assertGreater(
            len(direct_child_ids),
            0,
            "Expected at least one child block under replicated parent blocks.",
        )

        grandchild_ids = set()
        for child_id in direct_child_ids:
            grandchild_ids.update(children_by_parent.get(child_id, set()))

        self.assertGreater(
            len(grandchild_ids),
            0,
            "Expected at least one grandchild block under replicated child blocks.",
        )