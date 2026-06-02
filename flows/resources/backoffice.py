import os

import dagster as dg
from dotenv import load_dotenv

from bafrapy.backoffice.db.main_repository import BackofficeRepository, BackofficeRepositoryBuilder

load_dotenv()


class BackofficeResource(dg.ConfigurableResource):
    def get_repository(self) -> BackofficeRepository:
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        host = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_EXTERNAL_PORT", "5432")
        database = os.environ["DB_DATABASE"]
        dsn = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        return BackofficeRepositoryBuilder(dsn=dsn).build()
