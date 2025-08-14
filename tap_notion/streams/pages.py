from typing import Dict, Any, List
from typing import Iterator
from singer import get_logger, write_record, metrics
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
        # Attach PageProperty as a child stream
        self.child_to_sync = [PageProperty(self.client)]

    def get_records(self) -> Iterator[Dict[str, Any]]:
        LOGGER.info("START Syncing: pages")
        total_records = 0

        url = f"{self.client.base_url}/pages"
        payload = {}  # Remove invalid fields

        next_cursor = None
        while True:
            if next_cursor:
                payload["start_cursor"] = next_cursor

            response = self.client.post(url, params={}, headers=self.headers, body=payload)
            results = response.get("results", [])
            total_records += len(results)

            # Log metrics for the batch
            LOGGER.info(
                f'METRIC: {{"type": "counter", "metric": "record_count", "value": {len(results)}, "tags": {{"endpoint": "pages"}}}}'
            )
            LOGGER.info(
                f'METRIC: {{"type": "timer", "metric": "http_request_duration", "value": {response.elapsed.total_seconds()}, "tags": {{"endpoint": "{url}", "status": "succeeded"}}}}'
            )

            yield from results

            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break

        LOGGER.info(f"FINISHED Syncing: pages, total_records: {total_records}")

    def sync(self, state: Dict, transformer, parent_obj: Dict = None) -> Dict:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(record, self.schema, self.metadata)

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                # Pass each page to PageProperty as parent_obj
                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
