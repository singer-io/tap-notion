from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Users(FullTableStream):
    @property
    def tap_stream_id(self):
        return "users"

    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_method(self):
        return "FULL_TABLE"

    @property
    def replication_keys(self):
        return []

    @property
    def data_key(self):
        return "results"

    @property
    def path(self):
        return "users"
