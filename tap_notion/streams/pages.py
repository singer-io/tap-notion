from typing import Dict, Any, List
from singer import get_logger, write_record, metrics
from tap_notion.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Pages(IncrementalStream):
    tap_stream_id = "pages"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["last_edited_time"]

    def get_records(self) -> List[Dict[str, Any]]:
        headers, params = self.client.authenticate(self.headers, self.params)
        search_endpoint = f"{self.client.base_url}/search"

        body = {
            "filter": {"property": "object", "value": "page"},
            "sort": {"direction": "ascending", "timestamp": "last_edited_time"},
            "page_size": self.page_size
        }

        while True:
            response = self.client.post(search_endpoint, params, headers, body)
            results = response.get("results", [])
            yield from results

            next_cursor = response.get("next_cursor")
            if not next_cursor:
                break
            body["start_cursor"] = next_cursor

    def sync(self, state: Dict, transformer, parent_obj: Dict = None) -> Dict:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(record, self.schema, self.metadata)


                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
