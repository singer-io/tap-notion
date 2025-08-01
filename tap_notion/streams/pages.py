from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Pages(IncrementalStream):
    tap_stream_id = "pages"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_edited_time"]
    path = "pages/23b0dbd4427580f0a858dc5bc096b964"