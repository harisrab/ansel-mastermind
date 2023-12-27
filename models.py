from pydantic import BaseModel


class GetProducts(BaseModel):
    organization_id: str


class Data(BaseModel):
    password: str
    shop_url: str
    organization_id: str
