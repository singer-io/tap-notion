from typing import Dict, Any, List
from singer import get_bookmark, get_logger
from tap_notion.streams.abstracts import IncrementalStream, FullTableStream

LOGGER = get_logger()


class PageProperty(FullTableStream):
    tap_stream_id = "pages_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def get_records(self) -> List:
        page_id = "23b0dbd44275800e922fddb26fd2de21"

        headers, params = self.client.authenticate(self.headers, self.params)

        # Fetch the page to get property IDs
        page_data = self.client.get(f"{self.client.base_url}/pages/{page_id}", params=params, headers=headers)

        for prop_name, prop_info in page_data.get("properties", {}).items():
            prop_id = prop_info.get("id")
            if not prop_id:
                continue

            next_cursor = None
            while True:
                url = f"{self.client.base_url}/pages/{page_id}/properties/{prop_id}"
                if next_cursor:
                    params["start_cursor"] = next_cursor
                else:
                    params.pop("start_cursor", None)

                resp = self.client.get(url, params=params, headers=headers)
                yield resp

                next_cursor = resp.get("next_cursor")
                if not next_cursor:
                    break


