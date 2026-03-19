from fastapi import APIRouter

from src.app.models import IDConversionRequest, LayerConversionRequest
from src.conversion.id import handle_id
from src.conversion.layers import handle_layers

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/layers")
async def create_layer(request: LayerConversionRequest) -> dict[str, str]:
    return handle_layers(request)


@router.post(path="/ids")
async def create_mws(request: IDConversionRequest) -> dict[str, str]:
    return handle_id(request)
