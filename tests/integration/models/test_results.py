from uuid import uuid4

from bafrapy.models import Result
from tests.integration.base import IntegrationTestDB


class TestResultIntegration(IntegrationTestDB):

    def test_create_result(self):
        result = Result(value="test")

        with self.main_repo.start_session() as uow:
            uow.results.save(result)
            uow.commit()

            result_saved = uow.results.get_by_id(result.id)

        assert result_saved is not None
        assert result_saved.value == "test"

    def test_get_results(self):
        result = Result(value="test")

        with self.main_repo.start_session() as uow:
            uow.results.save(result)
            for result in uow.results.list():
                print(result)
