from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream, IncrementalStream

LOGGER = get_logger()

class Databases(IncrementalStream):
    tap_stream_id = "databases"
    key_properties = ["id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"

    def build_payload(self, next_cursor=None):
        payload = {
            "filter": {"property": "object", "value": "database"},
            "page_size": self.page_size,
        }
        if next_cursor:
            payload["start_cursor"] = next_cursor
        return payload

    def get_records(self, parent_obj=None):
        url = f"{self.client.base_url}/search"
        has_more, next_cursor = True, None

        while has_more:
            response = self.post_records(url, self.headers, next_cursor)
            results = response.get("results", [])
            yield from results

            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")



