from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream, IncrementalStream

LOGGER = get_logger()

class Comments(IncrementalStream):
    tap_stream_id = "comments"
    key_properties = ["id", "block_id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"
    parent = "blocks"
    path = "comments"
    data_key = "results"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Constructs the API endpoint URL for fetching comments
        using the parent block's id dynamically.
        """
        if parent_obj is None or "id" not in parent_obj:
            raise ValueError("Parent object must have an 'id' key")

        block_id = parent_obj["id"]
        return f"{self.client.base_url}/{self.path}?block_id={block_id}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
            """
            Modify the record before writing to the stream
            """
            if parent_record:
                record["block_id"] = parent_record.get("id")
            return record
