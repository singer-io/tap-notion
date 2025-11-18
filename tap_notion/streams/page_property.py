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
    path = "pages/{page_id}/properties/{property_id}"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        if not parent_obj:
            raise ValueError("Parent object required to build PageProperty URL")

        page_id = parent_obj.get("page_id") or parent_obj.get("id")
        property_id = parent_obj.get("property_id")

        if not property_id:
            return f"{self.client.base_url}/pages/{page_id}"

        return f"{self.client.base_url}/{self.path.format(page_id=page_id, property_id=property_id)}"

    def fetch_property_items(self, first_url: str) -> Iterator[Dict]:
        """
        Handles:
        - object = property_item (simple)
        - object = list (paginated)
        """

        url = first_url
        params = {}

        while True:
            response = self.client.get(url, params=params, headers=self.client.headers)

            if response.get("object") == "property_item":
                yield response
                return

            if response.get("object") == "list":
                for item in response.get("results", []):
                    yield item

                if not response.get("has_more"):
                    return

                next_url = response.get("next_url")
                next_cursor = response.get("next_cursor")

                if next_url:
                    url = next_url
                    params = {}
                else:
                    params = {"start_cursor": next_cursor}

            else:
                LOGGER.warning(f"Unknown Notion response type: {response}")
                return

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        if not parent_obj:
            raise ValueError("PageProperty must be run as a child of Pages stream")

        page_id = parent_obj["id"]
        LOGGER.info(f"START Fetching properties for page {page_id}")

        total_properties = 0

        for prop_name, prop_info in parent_obj.get("properties", {}).items():
            prop_id = prop_info.get("id")
            if not prop_id:
                continue

            url = self.get_url_endpoint({"page_id": page_id, "property_id": prop_id})

            for prop_item in self.fetch_property_items(url):
                prop_item["parent_id"] = page_id
                yield prop_item
                total_properties += 1

        LOGGER.info(
            f"FINISHED Fetching properties for page {page_id}, total_items_fetched: {total_properties}"
        )
