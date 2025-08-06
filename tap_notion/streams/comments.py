from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

from typing import Dict
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Comments(FullTableStream):
    tap_stream_id = "comments"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent = "blocks"
    path = "comments"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Return the comments endpoint with block_id as a query param."""
        block_id = parent_obj.get("id")
        if not block_id:
            raise ValueError("Missing 'id' in parent object for Comments stream.")

        return f"{self.client.base_url}/{self.path}?block_id={block_id}"

