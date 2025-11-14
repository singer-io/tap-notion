
from base import NotionBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class NotionInterruptedSyncTest(InterruptedSyncTest, NotionBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_notion_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {'block_children', 'bot_user', 'page_property', 'users'}
        return self.expected_stream_names().difference(streams_to_exclude)


    def manipulate_state(self):
        return {
            "currently_syncing": "pages",
            "bookmarks": {
                "pages": { "last_edited_time" : "2020-01-01T00:00:00Z"},
                "blocks": { "last_edited_time" : "2020-01-01T00:00:00Z"},
                "data_sources": { "last_edited_time" : "2020-01-01T00:00:00Z"},
                "comments": { "last_edited_time" : "2020-01-01T00:00:00Z"},
                "file_upload": { "last_edited_time" : "2020-01-01T00:00:00Z"},
        }
    }