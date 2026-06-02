from typing import Dict
from singer import Transformer, get_logger, metrics, write_record
from tap_notion.streams.abstracts import FullTableStream

LOGGER = get_logger()

class BlockChildren(FullTableStream):
    tap_stream_id = "block_children"
    key_properties = ["id", "block_id"]
    replication_keys = []
    replication_method = "FULL_TABLE"
    data_key = "results"
    parent = "blocks"
    path = "blocks/{block_id}/children"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """
        Build the full URL endpoint for a specific block's children.
        """
        if not parent_obj:
            raise ValueError("Parent object required to build BlockChildren URL")

        block_id = parent_obj.get("id")
        if not block_id:
            raise ValueError("Missing 'id' in parent object for BlockChildren.")

        return f"{self.client.base_url}/{self.path.format(block_id=block_id)}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        if parent_record:
            record["block_id"] = parent_record.get("id")
        else:
            record.pop("block_id", None)
        return record

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Dict = None) -> int:
        """
        Sync block children. Recursively fetches children for any nested block
        (parent.type == "block_id"). Blocks whose parent is a page (parent.type == "page_id")
        are top-level blocks already covered by the Blocks stream and are not recursed into,
        which prevents duplicate records.
        """
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(parent_obj=parent_obj):
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(record, self.schema, self.metadata)
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                is_nested = record.get("parent", {}).get("type") == "block_id"
                if record.get("has_children") and is_nested:
                    self.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
