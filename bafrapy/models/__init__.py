from sqlmodel import SQLModel

from bafrapy.models.generated import Asset, Provider, Resolution

# Compatibility shim
BaseModel = SQLModel
metadata = SQLModel.metadata

__all__ = [
    "Asset",
    "Provider",
    "Resolution",
    "BaseModel",
    "metadata",
]
