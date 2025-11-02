from typing import Protocol, runtime_checkable


@runtime_checkable
class Serializable(Protocol):
    def serialize(self) -> str:
        pass

    def load(self, data: str):
        pass
