from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pymilvus import DataType, MilvusClient
from pymilvus.exceptions import MilvusException


def probe() -> dict[str, object]:
    with TemporaryDirectory(prefix="vectorlake-milvus-") as directory:
        client = MilvusClient(str(Path(directory) / "probe.db"))
        try:
            schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=32)
            schema.add_field("scope_id", DataType.VARCHAR, max_length=32)
            schema.add_field("hash", DataType.BINARY_VECTOR, dim=16)
            indexes = MilvusClient.prepare_index_params()
            indexes.add_index(field_name="hash", index_type="BIN_FLAT", metric_type="HAMMING")
            client.create_collection("pdq_probe", schema=schema, index_params=indexes)
            client.insert(
                "pdq_probe",
                [
                    {"id": "same", "scope_id": "scope-a", "hash": b"\x00\x00"},
                    {"id": "near", "scope_id": "scope-a", "hash": b"\x00\x01"},
                    {"id": "other", "scope_id": "scope-b", "hash": b"\x00\x00"},
                ],
            )
            hits = client.search(
                "pdq_probe",
                data=[b"\x00\x00"],
                anns_field="hash",
                filter='scope_id == "scope-a"',
                limit=3,
                output_fields=["scope_id"],
            )[0]
            return {
                "status": "supported",
                "index": client.describe_index("pdq_probe", "hash"),
                "hits": [{"id": hit["id"], "distance": hit["distance"]} for hit in hits],
            }
        except MilvusException as exc:
            if "BinaryVector which MilvusLite does not support" not in str(exc):
                raise
            return {"status": "unsupported", "error_type": type(exc).__name__, "error": str(exc)}
        finally:
            client.close()


if __name__ == "__main__":
    print(json.dumps(probe(), indent=2, default=str))
