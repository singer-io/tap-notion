from base import NotionBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class NotionBookMarkTest(BookmarkTest, NotionBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "pages": { "last_edited_time" : "2025-01-01T00:00:00Z"},
            "blocks": { "last_edited_time" : "2025-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_notion_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {'page_property', 'block_children', 'bot_user', 'comments', 'data_sources', 'file_upload', 'users'}
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "pages": { "last_edited_time" : "2025-11-05T00:00:00Z"},
            "blocks": { "last_edited_time" : "2025-11-05T00:00:00Z"},

        }

        return new_bookmarks