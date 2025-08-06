from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

LOGGER = get_logger()

class FileUploads(FullTableStream):
    tap_stream_id = "file_uploads"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    path = "file_uploads"
