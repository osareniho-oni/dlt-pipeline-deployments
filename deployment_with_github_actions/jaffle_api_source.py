from typing import Any
import dlt
from dlt.sources.rest_api import rest_api_source, rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.common.typing import TDataItem

import os

os.environ["EXTRACT__WORKERS"] = "5"
os.environ["NORMALIZE__WORKERS"] = "2"
os.environ['NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = '100000'
os.environ["LOAD__WORKERS"] = "3"

def keep_large_orders(record: TDataItem) -> bool:
    """Keep only orders with total > 500"""
    return record.get("order_total", 0) > 500


@dlt.source(name="jaffle_shop")
def jaffle_shop_source() -> Any:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
            "paginator": {
                "type": "header_link",
            },
        },
        "resources": [
            {
                "name": "orders",
                "parallelized": True,
                "processing_steps": [
                    {"filter": keep_large_orders}
                ],
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "page": 1,
                        "page_size": 100,
                        # incremental parameter binding
                        "from": "{incremental.start_value}",
                    },
                    "incremental": {
                        "cursor_path": "ordered_at",
                        "initial_value": "2017-08-01T00:00:00",
                    },
                },
            },
            {
                "name": "customers",
                "parallelized": True,
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "customers",
                    "params": {
                        "page": 1,
                        "page_size": 100,
                    },
                },
            },
            {
                "name": "products",
                "parallelized": True,
                "primary_key": "sku",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "products",
                    "params": {
                        "page": 1,
                        "page_size": 100,
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)

def load_items() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_jaffle_shop",
        destination="duckdb",
        dataset_name="rest_api_data",
        progress="log"
    )

    load_info = pipeline.run(
        jaffle_shop_source()
    )

    print(load_info)

if __name__ == "__main__":
    load_items()
