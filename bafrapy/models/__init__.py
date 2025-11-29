from bafrapy.models.generated import Asset, Base, Provider, Resolution

# Compatibility shim
# BaseModel = SQLModel
BaseModel = Base

__all__ = [
    "Asset",
    "Provider",
    "Resolution",
    "BaseModel"
]
