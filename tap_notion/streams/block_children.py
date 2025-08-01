from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class BlockChildren(FullTableStream):
    tap_stream_id = "block_children"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    path = "blocks/{block_id}/children"

    def get_records(self) -> List:
        self.params["start_cursor"] = self.page_size
        return super().get_records()
