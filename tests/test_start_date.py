from base import NotionBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest



class NotionStartDateTest(StartDateTest, NotionBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_notion_start_date_test"

    def streams_to_test(self):
        # excluded streams having less data for this test
        streams_to_exclude = {'page_property', 'block_children', 'bot_user', 
                              'comments', 'data_sources', 'file_upload', 'users'}
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2025-03-25T00:00:00Z"
    @property
    def start_date_2(self):
        return "2025-11-13T00:00:00Z"