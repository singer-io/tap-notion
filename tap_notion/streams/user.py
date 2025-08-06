from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class User(FullTableStream):
    tap_stream_id = "user"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent = "users"
    path = "users/{id}"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Prepare URL endpoint for child streams."""
        return f"{self.client.base_url}/{self.path.format(id=parent_obj['id'])}"
