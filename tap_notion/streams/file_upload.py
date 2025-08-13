from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class FileUpload(FullTableStream):
    tap_stream_id = "file_upload"
    key_properties = ["id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"
    path = "files"

    def parse_response(self, response):
        yield from response.json().get("results", [])
