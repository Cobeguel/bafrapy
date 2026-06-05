import dagster as dg

import flows.defs.markets.definitions as markets_definitions
import flows.defs.ohlcv.definitions as ohlcv_definitions
from flows.resources.backoffice import BackofficeResource
from flows.resources.datawarehouse import DatawarehouseResource

defs = dg.Definitions.merge(
    markets_definitions.definitions,
    ohlcv_definitions.definitions,
    dg.Definitions(
        resources={
            "backoffice": BackofficeResource(),
            "datawarehouse": DatawarehouseResource(),
        },
    ),
)
