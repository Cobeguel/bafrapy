import dagster as dg

from bafrapy.backoffice.models import Market, MarketStatus
from bafrapy.exchanges import ExchangeSpotClientFactory
from bafrapy.exchanges.client import ExchangeProvider
from bafrapy.exchanges.markets import MarketResponse
from flows.resources.backoffice import BackofficeResource

exchanges = dg.DynamicPartitionsDefinition(name="exchanges")


@dg.asset(partitions_def=exchanges)
def sync_exchange_markets(
    context: dg.AssetExecutionContext,
    backoffice: BackofficeResource,
) -> None:
    exchange_id = context.partition_key
    try:
        client = ExchangeSpotClientFactory().create_exchange_client(ExchangeProvider(exchange_id))
    except ValueError as exc:
        raise dg.Failure(f"Unknown exchange partition: {exchange_id}") from exc

    client_markets = client.get_markets()
    repository = backoffice.get_repository()
    with repository.start_session() as repo:
        if repo.exchanges.get(exchange_id) is None:
            raise dg.Failure(f"Exchange '{exchange_id}' not found")
        context.log.info(f"Syncing {len(client_markets)} markets for {exchange_id}")

        markets = repo.markets.list_by_exchange(exchange_id)
        markets_by_symbol = {market.symbol: market for market in markets}
        seen_symbols: set[str] = set()
        unlisted_count = 0

        to_insert: list[MarketResponse] = []
        to_update: list[tuple[Market, MarketResponse]] = []

        for client_market in client_markets:
            seen_symbols.add(client_market.symbol)
            existing = markets_by_symbol.get(client_market.symbol) or repo.markets.get(
                f"{exchange_id}-{client_market.symbol}"
            )
            if existing is None:
                to_insert.append(client_market)
            else:
                to_update.append((existing, client_market))

        for client_market in to_insert:
            repo.markets.save(
                Market(
                    symbol=client_market.symbol,
                    raw_symbol=client_market.raw_symbol,
                    base=client_market.base,
                    quote=client_market.quote,
                    exchange=exchange_id,
                    price_min=client_market.price_min,
                    price_max=client_market.price_max,
                    amount_min=client_market.amount_min,
                    amount_max=client_market.amount_max,
                    market_min=client_market.market_min,
                    market_max=client_market.market_max,
                    cost_min=client_market.cost_min,
                    cost_max=client_market.cost_max,
                    market_status=MarketStatus.LISTED if client_market.active else MarketStatus.UNLISTED,
                )
            )

        for existing, client_market in to_update:
            existing.price_min = client_market.price_min
            existing.price_max = client_market.price_max
            existing.amount_min = client_market.amount_min
            existing.amount_max = client_market.amount_max
            existing.market_min = client_market.market_min
            existing.market_max = client_market.market_max
            existing.cost_min = client_market.cost_min
            existing.cost_max = client_market.cost_max
            existing.market_status = MarketStatus.LISTED if client_market.active else MarketStatus.UNLISTED
            repo.markets.save(existing)

        for stored_market in markets:
            if stored_market.symbol in seen_symbols:
                continue
            if stored_market.market_status == MarketStatus.UNLISTED:
                continue
            stored_market.market_status = MarketStatus.UNLISTED
            repo.markets.save(stored_market)
            unlisted_count += 1

        repo.commit()

        context.log.info(f"Synced {len(client_markets)} markets for {exchange_id}; marked {unlisted_count} as unlisted")
