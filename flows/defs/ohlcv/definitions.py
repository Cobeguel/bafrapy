import dagster as dg

from flows.defs.ohlcv.assets import sync_market_ohlcv

definitions = dg.Definitions(
    assets=[sync_market_ohlcv],
    jobs=[
        dg.define_asset_job(
            name="sync_market_ohlcv_job",
            selection=dg.AssetSelection.assets(sync_market_ohlcv),
        ),
    ],
)
