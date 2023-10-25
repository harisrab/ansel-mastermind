from fastapi import APIRouter
from pydantic import BaseModel

from forecast_helpers import generate_forecast_on_all
from helpers import fetch_data_from_db_for_forecasting


class ForecastRequest(BaseModel):
    organization_id: str


router = APIRouter()


@router.post("/forecast")
async def forecast(data: ForecastRequest):
    """
    This takes in direct data from PostgreSQL, and picks the order_item_quantity along with dates

    organization_id
    """

    # Organization ID
    organization_id = data.organization_id

    # Database connection details
    host = "ansel.postgres.database.azure.com"
    username = "harisrab"
    password = "44FantasticFox"
    database = "ansel"

    # Equivalent to data recieved from sales_order.csv directly downloaded from Shopify
    # Given the organization_id -> dataframe (name, title, quantity, variant_title, sku, day)
    df = fetch_data_from_db_for_forecasting(
        organization_id, host, username, password, database)

    # print(df)

    # Get forecast on all the products and saved.
    products = generate_forecast_on_all(df, bucketing_duration='weekly', organization_id=organization_id)

    print(products[0])

    # Construct a filtered dataframe (day, product_title, ordered_item_quantity)
    filtered_df = []

    pass
