from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bafrapy.models.base import BaseModel
from bafrapy.repositories import MainRepository


class IntegrationTestBase:
    def setup_method(self):
        self.engine = create_engine("sqlite:///:memory:")
        BaseModel.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.main_repo = MainRepository(session_factory=Session)

    def teardown_method(self):
        BaseModel.metadata.drop_all(self.engine)
        self.engine.dispose()