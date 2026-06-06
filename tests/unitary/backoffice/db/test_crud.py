from unittest.mock import MagicMock, patch

from bafrapy.backoffice.db.crud import CRUDRepository


class StubModel:
    id: str

    def __init__(self, id: str = "test-id") -> None:
        self.id = id


class TestCrud:
    def setup_method(self):
        self.session = MagicMock()
        self.repository = CRUDRepository(StubModel, self.session)

    def test_save_transient(self):
        instance = StubModel()

        with patch("bafrapy.backoffice.db.crud.inspect") as inspect_mock:
            inspect_mock.return_value.transient = True
            result = self.repository.save(instance)

        self.session.add.assert_called_once_with(instance)
        self.session.merge.assert_not_called()
        assert result is instance

    def test_save_persistent(self):
        instance = StubModel()
        merged = StubModel(id="merged")
        self.session.merge.return_value = merged

        with patch("bafrapy.backoffice.db.crud.inspect") as inspect_mock:
            inspect_mock.return_value.transient = False
            result = self.repository.save(instance)

        self.session.merge.assert_called_once_with(instance)
        self.session.add.assert_not_called()
        assert result is merged

    def test_get(self):
        instance = StubModel()
        self.session.get.return_value = instance

        result = self.repository.get("test-id")

        assert result is instance
        self.session.get.assert_called_once_with(StubModel, "test-id")

    def test_list(self):
        instance = StubModel()
        scalars = MagicMock()
        scalars.all.return_value = [instance]
        self.session.execute.return_value.scalars.return_value = scalars

        with patch("bafrapy.backoffice.db.crud.select", return_value=MagicMock()):
            result = self.repository.list()

        assert result == [instance]
        self.session.execute.assert_called_once()

    def test_remove_found(self):
        instance = StubModel()
        self.session.get.return_value = instance

        result = self.repository.remove("test-id")

        assert result is True
        self.session.delete.assert_called_once_with(instance)

    def test_remove_missing(self):
        self.session.get.return_value = None

        result = self.repository.remove("test-id")

        assert result is False
        self.session.delete.assert_not_called()

    def test_count(self):
        self.session.execute.return_value.scalar.return_value = 3

        with patch("bafrapy.backoffice.db.crud.select", return_value=MagicMock()):
            result = self.repository.count()

        assert result == 3
        self.session.execute.assert_called_once()
