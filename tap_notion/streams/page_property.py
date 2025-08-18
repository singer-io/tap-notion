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
        """
        This method is called by the base sync(), but for PageProperty
        we can't build the URL until we know the property_id.
        So we return just the page endpoint if property_id isn't provided.
        """
        if not parent_obj:
            raise ValueError("Parent object required to build PageProperty URL")

        page_id = parent_obj.get("page_id") or parent_obj.get("id")
        property_id = parent_obj.get("property_id")

        if not property_id:
            # Just return the page URL; actual property URLs will be built in get_records()
            return f"{self.client.base_url}/pages/{page_id}"

        return f"{self.client.base_url}/{self.path.format(page_id=page_id, property_id=property_id)}"

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        if not parent_obj:
            raise ValueError("PageProperty must be run as a child of Pages stream")

        page_id = parent_obj["id"]
        LOGGER.debug(f"START Fetching properties for page {page_id}")

        page_url = f"{self.client.base_url}/pages/{page_id}"
        page_data = self.client.get(page_url, params=self.params, headers=self.headers)

        total_properties = 0
        for prop_name, prop_info in page_data.get("properties", {}).items():
            prop_id = prop_info.get("id")
            if not prop_id:
                continue

            prop_url = self.get_url_endpoint({"page_id": page_id, "property_id": prop_id})
            prop_data = self.client.get(prop_url, params=self.params, headers=self.headers)

            yield {
                "page_id": page_id,
                "property_name": prop_name,
                "property_id": prop_id,
                "property_data": prop_data
            }
            total_properties += 1

        LOGGER.debug(f"FINISHED Fetching properties for page {page_id}, total_properties: {total_properties}")
