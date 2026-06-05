import dagster as dg

from flows.defs.markets.assets import exchanges, sync_exchange_markets

definitions = dg.Definitions(
    assets=[sync_exchange_markets],
    jobs=[
        dg.define_asset_job(
            name="sync_exchange_markets_job",
            selection=dg.AssetSelection.assets(sync_exchange_markets),
            partitions_def=exchanges,
        ),
    ],
)
