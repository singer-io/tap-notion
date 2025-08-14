from typing import Dict, Any, Iterator
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
        self.child_to_sync = [PageProperty(self.client)]

    def get_records(self) -> Iterator[Dict[str, Any]]:
        LOGGER.info(f"START Syncing: {self.tap_stream_id}")

        url = f"{self.client.base_url}/search"
        bookmark_date = self.get_bookmark(self.client.config, self.tap_stream_id)

        payload = {
            "filter": {
                "property": "object",
                "value": "page"
            },
            "page_size": self.page_size,
            "sort": {
                "direction": "ascending",
                "timestamp": "last_edited_time"
            }
        }

        next_cursor = None
        while True:
            if next_cursor:
                payload["start_cursor"] = next_cursor

            response = self.client.post(
                url,
                params={},  # No `updated_since` here
                headers=self.headers,
                body=payload
            )

            results = response.get("results", [])
            for page in results:
                if page.get("last_edited_time") and page["last_edited_time"] >= bookmark_date:
                    yield page

            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break

        LOGGER.info(f"FINISHED Syncing: {self.tap_stream_id}")

