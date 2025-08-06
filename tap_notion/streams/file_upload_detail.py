from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class FileUpload(FullTableStream):
    tap_stream_id = "file_upload_detail"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    parent = "file_upload_detail"
    path = "file_upload_detail/{id}"

    def get_url_endpoint(self, context: Dict) -> str:
        file_id = context["id"]
        return f"file_uploads/{file_id}"
