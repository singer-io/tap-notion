from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class PagesProperty(IncrementalStream):
    tap_stream_id = "pages_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    path = "/v1/pages/{page_id}/properties/{property_id}"
