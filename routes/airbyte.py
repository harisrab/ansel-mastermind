from fastapi import APIRouter
from models import Data
from airbyte_helpers import *

router = APIRouter()

@router.post("/create_ds_shopify")
async def create_ds_shopify(data: Data):
    password = data.password
    shop_url = data.shop_url
    organization_id = data.organization_id

    # # Create the workspace if it doesn't already exist in Airbyte
    workspaceId = create_workspace(organization_id)

    return

    # 1. Create the data source
    sourceId = create_shopify_datasource(
        password=password,
        workspaceId=workspaceId,
        shop_url=shop_url,
    )

    # 2. Create the destination.
    destinationId = create_destination(workspaceId, organization_id, sourceId)

    # Create a connection and move the data
    connectionId = create_connection(
        workspaceId, sourceId, destinationId, organization_id)

    print(f"Access admin panel → https://admin.airbyte.myansel.com/workspaces/{workspaceId}")

    # Run the job to sync the connection that has been created
    sync(connectionId)