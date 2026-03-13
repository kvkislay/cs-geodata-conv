from pydantic import BaseModel


class LayerConversionRequest(BaseModel):
    hierarchy: str
    location: str  # TODO: Build a mapping for this
    column_map: dict[str, dict] | None = None
    layer_names: list[str] | None = None


class IDConversionRequest(BaseModel):
    type: str
    id_list: list[str]
    layers: list[str]
