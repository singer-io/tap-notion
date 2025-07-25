
from base import NotionBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class NotionInterruptedSyncTest(NotionBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_notion_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()


    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
                "pages": { "last_edited_time" : "2020-01-01T00:00:00Z"},
                "blocks": { "last_edited_time" : "2020-01-01T00:00:00Z"},
        }
    }