from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Blocks(IncrementalStream):
    tap_stream_id = "blocks"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_edited_time"]
    data_key = "results"
    path = "blocks/{page_id}/children"
    parent = "pages"
    children = ['block_children','comments']
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
