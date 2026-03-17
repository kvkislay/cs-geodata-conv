from pydantic import BaseModel


class LayerConversionRequest(BaseModel):
    unit: str
    filepath: str  # TODO: Build a mapping for this
    column_map: dict[str, str] | None = None
    layer_names: list[str] | None = None
    # resolution: str | None = None


class IDConversionRequest(BaseModel):
    type: str
    id_list: list[str]
    layers: list[str]
    # resolution: int | None = None
