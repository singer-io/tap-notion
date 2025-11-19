from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Blocks(IncrementalStream):
    tap_stream_id = "blocks"
    key_properties = ["id", "page_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_edited_time"]
    data_key = "results"
    path = "blocks/{page_id}/children"
    parent = "pages"
    children = ['block_children','comments']
    bookmark_value = None
    
    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        kwargs ={}
        self.params.update(kwargs)

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
    
    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        if not parent_obj:
            raise ValueError("Parent object required to build blocks URL")

        page_id = parent_obj.get("page_id") or parent_obj.get("id")

        return f"{self.client.base_url}/{self.path.format(page_id=page_id)}"


    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        if parent_record:
            record["page_id"] = parent_record.get("id")
        return record
