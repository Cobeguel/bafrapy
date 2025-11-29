import json
import os

from pathlib import Path

from cattrs import structure
from dotenv import load_dotenv
from ray import serve
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from bafrapy.datawarehouse.repository import OHLCVRepositoryBuilder
from bafrapy.providers.binance import BinanceConfig, BinanceFactory
from bafrapy.repositories.mainrepository import MainRepository, MainRepositoryBuilder
from workflows.deployments.gateway import ApiDeployment
from workflows.deployments.worker import Worker

load_dotenv()

if __name__ == "__main__":
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_PORT = os.getenv("DB_EXTERNAL_PORT", "3306")
    DB_HOST = os.getenv("DB_HOST", "localhost")

    url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"
    engine = create_engine(url)
    session = sessionmaker[Session](bind=engine)
    repository = MainRepository(session)
    repository_builder = MainRepositoryBuilder(dsn=url)

    CH_USER = os.getenv("CH_USER")
    CH_PASSWORD = os.getenv("CH_PASSWORD")
    CH_DATABASE = os.getenv("CH_DATABASE")
    CH_PORT = os.getenv("CH_EXTERNAL_PORT", "8123")
    CH_HOST = os.getenv("CH_HOST", "localhost")

    data_repository_builder = OHLCVRepositoryBuilder(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    provider_builders = []

    binance_config_file = json.loads(
        Path("config/providers/binance.json").read_text(encoding="utf-8")
    )
    binance_config = structure(binance_config_file, BinanceConfig)
    binance_builder = BinanceFactory(provider_name="BINANCE", config=binance_config)
    provider_builders.append(binance_builder)

    providers = {
        builder.provider_name: builder.create_provider()
        for builder in provider_builders
    }

    worker = Worker.bind(
        data_repository_builder=data_repository_builder,
        repository_builder=repository_builder,
        provider_builders=provider_builders,
    )

    api = ApiDeployment.bind(worker_handler=worker)

    serve.run(api, blocking=True)
