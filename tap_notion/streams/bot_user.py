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

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:

        prop_url = self.get_url_endpoint()
        prop_data = self.client.get(prop_url, params={}, headers=self.client.headers)

        yield prop_data
