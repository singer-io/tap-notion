from typing import Dict, List
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()


class PagesProperty(FullTableStream):
    tap_stream_id = "pages_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "pages/{page_id}/properties/{property_id}"

    def get_records(self) -> List:
        self.params["start_cursor"] = self.page_size
        return super().get_records()

    @property
    def replication_keys(self):
        return []
