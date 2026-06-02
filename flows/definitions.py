import dagster as dg

from flows.defs.exchanges.assets import exchanges, sync_exchange_markets
from flows.resources.backoffice import BackofficeResource

defs = dg.Definitions(
    assets=[sync_exchange_markets],
    resources={"backoffice": BackofficeResource()},
    jobs=[
        dg.define_asset_job(
            name="sync_exchange_markets_job",
            selection=dg.AssetSelection.assets(sync_exchange_markets),
            partitions_def=exchanges,
        ),
    ],
)
