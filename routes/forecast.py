from pydantic import BaseModel
from fastapi import APIRouter


class ForecastRequest(BaseModel):
    organization_id: str

router = APIRouter()    

@router.post("/forecast")
async def forecast(data: ForecastRequest):
    
    
    pass
