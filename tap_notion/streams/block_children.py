from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class BlockChildren(FullTableStream):
    tap_stream_id = "block_children"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent = "blocks"
    path = "blocks/{block_id}/children"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Build the full URL endpoint for a specific block's children.
        """
        if not parent_obj:
            raise ValueError("Parent object required to build BlockChildren URL")

        block_id = parent_obj.get("block_id") or parent_obj.get("id")
        if not block_id:
            raise ValueError("Missing 'id' in parent object for BlockChildren.")

        return f"{self.client.base_url}/{self.path.format(block_id=block_id)}"
