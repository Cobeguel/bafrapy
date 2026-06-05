from datetime import datetime, timedelta, timezone

import dagster as dg

from bafrapy.backoffice.models import MarketAvailability, SyncStatus
from bafrapy.exchanges import (
    ExchangeClientResolution,
    ExchangeProvider,
    ExchangeSpotClientFactory,
)
from flows.resources.backoffice import BackofficeResource
from flows.resources.datawarehouse import DatawarehouseResource


class SyncMarketOhlcvConfig(dg.Config):
    exchange: str
    symbol: str
    min_chunk_size: int = 50000


@dg.asset
def sync_market_ohlcv(
    context: dg.AssetExecutionContext,
    config: SyncMarketOhlcvConfig,
    backoffice: BackofficeResource,
    datawarehouse: DatawarehouseResource,
) -> None:
    exchange_id = config.exchange
    symbol = config.symbol

    try:
        client = ExchangeSpotClientFactory().create_exchange_client(ExchangeProvider(exchange_id))
    except ValueError as exc:
        raise dg.Failure(f"Cannot create exchange client for exchange key: {exchange_id}") from exc

    repository = backoffice.get_repository()
    with repository.start_session() as repo:
        exchange = repo.exchanges.get(exchange_id)
        if exchange is None:
            raise dg.Failure(f"Exchange '{exchange_id}' not found")

        market = repo.markets.get_by_exchange_and_symbol(exchange_id, symbol)
        if market is None:
            raise dg.Failure(f"Market '{symbol}' not found for exchange '{exchange_id}'")

        if not exchange.resolutions:
            raise dg.Failure(f"Exchange '{exchange_id}' has no resolutions configured")

        resolutions = sorted(exchange.resolutions, key=lambda r: r.seconds)
        base = market.base
        quote = market.quote
        market_id = market.id

    warehouse = datawarehouse.get_repository()

    for resolution in resolutions:
        historical_range = warehouse.market_historical_range(exchange_id, symbol, resolution.seconds)
        if historical_range is None:
            start = client.get_first_market_date(base, quote)
        else:
            start = historical_range.end

        end = client.get_last_market_date(base, quote) - timedelta(days=1)

        if historical_range is None:
            context.log.info(
                f"No DuckLake history for {exchange_id}:{symbol} at {resolution.code}; "
                f"downloading full history from {start} to {end}"
            )
        else:
            context.log.info(
                f"DuckLake history for {exchange_id}:{symbol} at {resolution.code} spans "
                f"{historical_range.start} to {historical_range.end}; updating from {start} to {end}"
            )

        if start > end:
            context.log.info(f"No new {resolution.code} data available for {exchange_id}:{symbol}")
            continue

        client_resolution = ExchangeClientResolution(seconds=resolution.seconds, name=resolution.code)
        inserted_rows = 0
        for chunk in client.get_ohlcv(
            base,
            quote,
            client_resolution,
            start.date(),
            end.date(),
            min_chunk_size=config.min_chunk_size,
        ):
            warehouse.insert_ohlcv(chunk)
            inserted_rows += chunk.height

        with repository.start_session() as repo:
            availability = repo.market_availabilities.get(f"{market_id}-{resolution.seconds}")
            if availability is None:
                availability = MarketAvailability.create(market=market_id, resolution=resolution)

            if historical_range is None:
                availability.first_date = start
            elif availability.first_date is None:
                availability.first_date = historical_range.start

            availability.last_date = end
            repo.market_availabilities.save(availability)
            repo.commit()

        context.log.info(f"Stored {inserted_rows} {resolution.code} OHLCV rows for {exchange_id}:{symbol}")

    with repository.start_session() as repo:
        market = repo.markets.get(market_id)
        if market is None:
            raise dg.Failure(f"Market '{market_id}' not found while updating sync metadata")

        market.last_update = datetime.now(timezone.utc)
        market.sync_status = SyncStatus.SYNCED
        repo.markets.save(market)
        repo.commit()

    context.log.info(f"Finished OHLCV sync for {exchange_id}:{symbol}")
