from typing import Dict
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
        """Prepare URL using the block id from parent."""
        block_id = parent_obj.get("id")
        if not block_id:
            raise ValueError("Missing 'id' in parent object for BlockChildren.")
        return f"{self.client.base_url}/blocks/{block_id}/children"

