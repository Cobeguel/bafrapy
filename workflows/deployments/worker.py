from typing import Dict, List

from attrs import define, field
from ray import serve

from bafrapy.datawarehouse.repository import OHLCVRepositoryBuilder
from bafrapy.logger import LoguruLogger as log
from bafrapy.providers.base import ProviderClient, ProviderClientBuilder
from bafrapy.repositories.mainrepository import MainRepository, MainRepositoryBuilder
from workflows.tasks.data import DataTask
from workflows.tasks.symbols import SymbolTask


@serve.deployment(
    name="worker",
    num_replicas=1  
)
@define
class Worker:
    repository_builder: MainRepositoryBuilder = field(kw_only=True)
    data_repository_builder: OHLCVRepositoryBuilder = field(kw_only=True)
    provider_builders: List[ProviderClientBuilder] = field(kw_only=True)

    symbols_task: SymbolTask = field(init=False)
    data_task: DataTask = field(init=False)

    def __attrs_post_init__(self) -> None:
        repository: MainRepository = self.repository_builder.build()
        providers: Dict[str, ProviderClient] = {}
        for builder in self.provider_builders:
            provider = builder.create_provider()
            providers[builder.get_provider_name()] = provider

        data_repository = self.data_repository_builder.build()

        self.symbols_task = SymbolTask(providers, repository)
        self.data_task = DataTask(providers, repository, data_repository)
        log().info("Worker initialized")

    def run_symbols(self, provider: str) -> None:
            self.symbols_task.run(provider)

    def run_data(self, provider: str, symbol: str) -> None:
        self.data_task.run(provider, symbol)
