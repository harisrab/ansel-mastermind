from routes import airbyte, forecast
from routes import dashboard
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyarrow import json
# from fastapi_cache.backends.inmemory import InMemoryBackend
# from fastapi_cache import FastAPICache

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.on_event("startup")
# async def startup():
#     # redis = aioredis.from_url("redis://localhost")
#     FastAPICache.init(InMemoryBackend())

@app.get("/")
def hello():
    return {"message": "Hello World, this just updated itself."}


# All the routes are registered here.
app.include_router(airbyte.router)
app.include_router(forecast.router)
app.include_router(dashboard.dashboard_router)


# Leze's API Token
# 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


# Shopify Store URL
# SHOP_NAME = "Dovetail Workwear"
# SHOP_URL = "quickstart-fbfc8d49.myshopify.com"
# SHOPIFY_API_KEY = "1912ec08649811b2b15083231c8e9410"
# SHOPIFY_TOKEN = "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"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
# SHOPIFY_API_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_TOKEN}@{SHOP_URL}/admin"
