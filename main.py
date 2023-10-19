import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyarrow import json
from pydantic import BaseModel

from airbyte_helpers import (create_connection, create_destination,
                             create_shopify_datasource, create_workspace)

load_dotenv()

AMZ_ACCESS_KEY = "AKIATXDG3RX4OBIE4EO3"
AMZ_SECRET_KEY = "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"
SHOPIFY_TOKEN = "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"

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Data(BaseModel):
    password: str
    shop_url: str
    organization_id: str


@app.post("/create_ds_shopify")
async def create_ds_shopify(data: Data):
    password = data.password
    shop_url = data.shop_url
    organization_id = data.organization_id

    print("API KEY for Airbyte", os.getenv('AIRBYTE_KEY'))

    # Create the workspace if it doesn't already exist in Airbyte
    workspaceId = create_workspace(organization_id)

    # 1. Create the data source
    sourceId = create_shopify_datasource(
        password=password,
        workspaceId=workspaceId,
        shop_url=shop_url
    )

    # 2. Create the destination.
    destinationId = create_destination(workspaceId, organization_id, sourceId)

    # Create a connection and move the data
    connectionId = create_connection(workspaceId, sourceId, destinationId)


# Shopify Store URL
# SHOP_NAME = "Dovetail Workwear"
# SHOP_URL = "quickstart-fbfc8d49.myshopify.com"
# SHOPIFY_API_KEY = "1912ec08649811b2b15083231c8e9410"
# SHOPIFY_TOKEN = "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"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
