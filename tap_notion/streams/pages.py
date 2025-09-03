from typing import Dict, Any, Iterator, Optional
from singer import get_logger
from tap_notion.streams.abstracts import IncrementalStream
from .page_property import PageProperty

LOGGER = get_logger()

class Pages(IncrementalStream):
    tap_stream_id = "pages"
    key_properties = ["id"]
    parent = None
    children = ["page_property"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_edited_time"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def build_payload(self, next_cursor: Optional[str] = None) -> dict:
        payload = {
            "filter": {"property": "object", "value": "page"},
            "page_size": self.page_size,
            "sort": {
                "direction": "ascending",
                "timestamp": "last_edited_time"
            },
        }
        if next_cursor:
            payload["start_cursor"] = next_cursor
        return payload

    def get_records(self, parent_obj: Dict[str, Any] = None, state: Optional[dict] = None) -> Iterator[Dict[str, Any]]:
        LOGGER.info(f"START Syncing: {self.tap_stream_id}")
        url = f"{self.client.base_url}/search"
        bookmark_date = self.get_bookmark(state or {}, self.tap_stream_id)

        has_more = True
        next_cursor = None

        while has_more:
            response = self.post_records(url, self.client.headers, next_cursor)
            results = response.get("results", [])

            for page in results:
                if page.get("last_edited_time") and page["last_edited_time"] >= bookmark_date:
                    yield page

            has_more = response.get("has_more", False)
            next_cursor = response.get("next_cursor")

        LOGGER.info(f"FINISHED Syncing: {self.tap_stream_id}")
