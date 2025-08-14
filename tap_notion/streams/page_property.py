from typing import Dict, Any, List
from tap_notion.streams.abstracts import FullTableStream
from singer import get_logger, write_record, metrics

LOGGER = get_logger()

class PageProperty(FullTableStream):
    tap_stream_id = "page_property"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    parent = "pages"
    children = []
    path = "pages/{page_id}/properties/{property_id}"  # relative path, no /v1

    def get_records(self, parent_obj: Dict = None) -> List:
        if not parent_obj:
            raise ValueError("PageProperty must be run as a child of Pages stream")

        page_id = parent_obj["id"]
        headers, params = self.client.authenticate(self.headers, self.params)

        # Fetch page details to get property IDs
        page_url = f"{self.client.base_url}/pages/{page_id}"
        LOGGER.info(f"Fetching properties for page {page_id}")
        page_data = self.client.get(page_url, params=params, headers=headers)

        total_properties = 0
        for prop_name, prop_info in page_data.get("properties", {}).items():
            prop_id = prop_info.get("id")
            if not prop_id:
                continue

            prop_url = f"{self.client.base_url}/{self.path.format(page_id=page_id, property_id=prop_id)}"
            response = self.client.get(prop_url, params=params, headers=headers)

            # Log metrics for each property
            LOGGER.info(
                f'METRIC: {{"type": "timer", "metric": "http_request_duration", "value": {response.elapsed.total_seconds()}, "tags": {{"endpoint": "{prop_url}", "status": "succeeded"}}}}'
            )
            total_properties += 1

        LOGGER.info(f"FINISHED Fetching properties for page {page_id}, total_properties: {total_properties}")

    def sync(self, state: Dict, transformer, parent_obj: Dict = None) -> Dict:
        """
        Transform and write property records.
        """
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_obj=parent_obj):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(record, self.schema, self.metadata)

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

            return counter.value
