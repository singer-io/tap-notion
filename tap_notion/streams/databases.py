from typing import Dict, Iterator, Optional
from singer import get_logger
from dateutil import parser
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Databases(IncrementalStream):
    tap_stream_id = "databases"
    key_properties = ["id"]
    replication_keys = ["last_edited_time"]
    replication_method = "INCREMENTAL"

    def build_payload(self, next_cursor: Optional[str] = None) -> dict:
        """Build request payload for Notion /search API to fetch databases."""
        payload = {
            "filter": {"property": "object", "value": "database"},
            "page_size": self.page_size,
        }
        if next_cursor:
            payload["start_cursor"] = next_cursor
        return payload

    def get_records(self, parent_obj: Dict = None, state: Dict = None) -> Iterator[Dict]:
        """Fetch database records incrementally using /search API."""
        url = f"{self.client.base_url}/search"
        has_more, next_cursor = True, None

        # Ensure state is not None
        state = state or {}

        bookmark = self.get_bookmark(state, self.tap_stream_id)
        bookmark_dt = parser.isoparse(bookmark) if bookmark else None

        while has_more:
            response = self.post_records(url, self.client.headers, next_cursor)
            results = response.get("results", [])

            for record in results:
                last_edited_time = record.get("last_edited_time")
                if last_edited_time:
                    record_dt = parser.isoparse(last_edited_time)
                    if not bookmark_dt or record_dt > bookmark_dt:
                        yield record

            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")
