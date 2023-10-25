from pydantic import BaseModel

class Data(BaseModel):
    password: str
    shop_url: str
    organization_id: str