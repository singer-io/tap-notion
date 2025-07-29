from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Databases(FullTableStream):
    tap_stream_id = "databases"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    path = "databases"
