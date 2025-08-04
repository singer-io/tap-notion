from typing import Dict, List
from singer import get_logger
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class PagesProperty(FullTableStream):
    tap_stream_id = "pages_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "pages/{page_id}/properties/{property_id}"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        if not parent_obj:
            raise ValueError("Missing parent_obj for PagesProperty")

        page_id = parent_obj.get("id")
        property_id = self._get_property_id_from_metadata()

        if not page_id or not property_id:
            raise ValueError("Missing page_id or property_id for PagesProperty")

        return f"{self.client.base_url}/pages/{page_id}/properties/{property_id}"

    def _get_property_id_from_metadata(self) -> str:
        try:
            return self.catalog.metadata[0].breadcrumb[-1]
        except (AttributeError, IndexError, KeyError):
            LOGGER.error("Failed to extract property_id from metadata.")
            return None

    def get_records(self) -> List[Dict]:
        self.params["page_size"] = self.page_size
        self.url_endpoint = self.get_url_endpoint(self.parent_obj)

        next_cursor = None
        while True:
            if next_cursor:
                self.params["start_cursor"] = next_cursor

            response = self.client.get(
                self.url_endpoint,
                self.params,
                self.headers
            )

            results = response.get("results", [])
            yield from results

            next_cursor = response.get("next_cursor")
            if not next_cursor:
                break

    @property
    def replication_keys(self):
        return []
