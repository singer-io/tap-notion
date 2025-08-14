from typing import Dict, Iterator
from tap_notion.streams.abstracts import FullTableStream
from singer import get_logger

LOGGER = get_logger()

class PageProperty(FullTableStream):
    tap_stream_id = "page_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    parent = "pages"
    children = []
    path = "pages/{page_id}/properties/{property_id}"  # relative path, no /v1

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        """
        Fetches all properties for a given Notion page and yields them.
        """
        if not parent_obj:
            LOGGER.info(f"Skipping {self.tap_stream_id} because no parent object was provided.")
            return []

        page_id = parent_obj["id"]
        LOGGER.info(f"START Fetching properties for page {page_id}")

        page_url = f"{self.client.base_url}/pages/{page_id}"
        page_data = self.client.get(page_url, params=self.params, headers=self.headers)

        total_properties = 0
        for prop_name, prop_info in page_data.get("properties", {}).items():
            prop_id = prop_info.get("id")
            if not prop_id:
                continue

            prop_url = f"{self.client.base_url}/{self.path.format(page_id=page_id, property_id=prop_id)}"
            prop_data = self.client.get(prop_url, params=self.params, headers=self.headers)

            yield {
                "page_id": page_id,
                "property_name": prop_name,
                "property_id": prop_id,
                "property_data": prop_data
            }

            total_properties += 1

        LOGGER.info(f"FINISHED Fetching properties for page {page_id}, total_properties: {total_properties}")
