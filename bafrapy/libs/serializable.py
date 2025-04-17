from typing import Protocol, runtime_checkable


@runtime_checkable
class Serializable(Protocol):
    def serialize(self) -> dict:
        pass

    def load(self, data: dict):
        pass
