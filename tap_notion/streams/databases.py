from typing import Dict, Iterator, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream, IncrementalStream

LOGGER = get_logger()

class Databases(IncrementalStream):
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

        has_more = True
        next_cursor = None

        while has_more:
            if next_cursor:
                payload["start_cursor"] = next_cursor
            else:
                payload.pop("start_cursor", None)  # Ensure clean payload if no cursor yet

            response = self.client.post(
                url,
                params={},
                headers=self.headers,
                body=payload
            )

            results = response.get("results", [])
            yield from results

            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")

