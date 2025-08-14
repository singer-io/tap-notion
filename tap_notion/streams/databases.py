from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class Databases(FullTableStream):
    tap_stream_id = "databases"
    key_properties = ["id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        LOGGER.info("Fetching databases using Notion search API.")
        url = f"{self.client.base_url}/search"
        payload = {
            "filter": {"property": "object", "value": "database"},
            "page_size": self.page_size
        }

        next_cursor = None
        while True:
            if next_cursor:
                payload["start_cursor"] = next_cursor

            response = self.client.post(
                url,
                params={},
                headers=self.headers,
                body=payload
            )

            results = response.get("results", [])
            yield from results

            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break

