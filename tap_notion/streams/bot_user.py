from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class BotUser(FullTableStream):
    tap_stream_id = "bot_user"
    key_properties = ["id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    path = "users/me"
