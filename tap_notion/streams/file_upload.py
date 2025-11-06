from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class FileUpload(IncrementalStream):
    tap_stream_id = "file_upload"
    key_properties = ["id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"
    path = "file_uploads"

    def parse_response(self, response):
        yield from response.json().get("results", [])
