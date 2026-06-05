import os

import dagster as dg
from dotenv import load_dotenv

from bafrapy.datawarehouse.repository import DucklakeOHLCVRepository, DucklakeOHLCVRepositoryBuilder

load_dotenv()


class DatawarehouseResource(dg.ConfigurableResource):
    def get_repository(self) -> DucklakeOHLCVRepository:
        return DucklakeOHLCVRepositoryBuilder(
            host=os.environ.get("GIZMOSQL_HOST", "localhost"),
            port=int(os.environ.get("GIZMOSQL_EXTERNAL_PORT", "31337")),
            username=os.environ["GIZMOSQL_USERNAME"],
            password=os.environ["GIZMOSQL_PASSWORD"],
            database=os.environ.get("GIZMOSQL_DUCKLAKE_DATABASE", "ducklake"),
            schema=os.environ.get("GIZMOSQL_SCHEMA", "bafrapy"),
            tls=os.environ.get("GIZMOSQL_TLS", "false").lower() == "true",
            tls_skip_verify=os.environ.get("GIZMOSQL_TLS_SKIP_VERIFY", "false").lower() == "true",
        ).build()
