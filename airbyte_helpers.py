import base64
import os

import requests

from helpers import create_database_for_customer

BASE_URL = "http://localhost:8006/v1"
BASE_URL = "https://api.airbyte.myansel.com/v1"


airbyte_username = "ansel"
airbyte_password = "44FantasticFox"
access_key = base64.b64encode(
    f"{airbyte_username}:{airbyte_password}".encode()).decode()
print("Access KEy: ", access_key)


def sync(connection_id):
    url = f"{BASE_URL}/jobs"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {access_key}"
    }

    payload = {
        "jobType": "sync",
        "connectionId": connection_id
    }

    response = requests.post(url, json=payload, headers=headers).json()
    
    return response.get('jobId', "")


import requests

from pprint import pprint

def get_streams(sourceId, destinationId):
    url = f"{BASE_URL}/streams?sourceId={sourceId}&destinationId={destinationId}&ignoreCache=true"
    headers = {
        "accept": "application/json", 
        "authorization": f"Basic {access_key}"      
    }

    response = requests.get(url, headers=headers)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
    selected_streams = [
        {"name": "orders", "syncMode": "full_refresh_overwrite"},
        {"name": "customers", "syncMode": "full_refresh_overwrite"},
        {"name": "inventory_items", "syncMode": "full_refresh_overwrite"},
        {"name": "inventory_levels", "syncMode": "full_refresh_overwrite"},
        {"name": "locations", "syncMode": "full_refresh_overwrite"},
        {"name": "products", "syncMode": "full_refresh_overwrite"}
    ]

    filtered_streams = [stream for stream in response.json() if stream['streamName'] in selected_streams]

    pprint(filtered_streams)

    return []


def create_connection(workspaceId, sourceId, destinationId, organization_id):

    print("\n")
    print("[+] Creating a connection")

    url = f'{BASE_URL}/connections?workspaceIds={workspaceId}&includeDeleted=false&limit=20&offset=0'
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {access_key}"
    }

    connections_list = requests.get(
        url, headers=headers).json().get('data', [])

    if len(connections_list) > 0:
        for each_connection in connections_list:
            existing_sourceId = each_connection['sourceId']
            existing_destinationId = each_connection['destinationId']

            if existing_destinationId == destinationId and existing_sourceId == sourceId:
                print(f"[+] Connection Already Exists")
                return each_connection['connectionId']

    # Check if the conenction already exists
    streams = get_streams(sourceId, destinationId, workspaceId)

    url = f"{BASE_URL}/connections"

    payload = {
        "schedule": {"scheduleType": "manual"},
        "dataResidency": "auto",
        "namespaceDefinition": "custom_format",
        "namespaceFormat": "${SOURCE_NAMESPACE}__" + organization_id,
        "nonBreakingSchemaUpdatesBehavior": "ignore",
        "sourceId": sourceId,
        "destinationId": destinationId,
        "configurations": {
            "streams": streams
        }
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {access_key}"
    }

    response = requests.post(url, json=payload, headers=headers)

    res = response.json().get("connectionId")

    print(res)

    return res


def create_destination(workspaceId, organization_id, sourceId):
    """
    This function creates a destination in Airbyte if it doesn't exist before.

    Args:
        workspaceId (str): The ID of the workspace in which the destination is to be created.
        organization_id (str): The ID of the organization for which the destination is to be created.
        sourceId (str): The ID of the source for which the destination is to be created.

    Returns:
        str: The destinationId of the existing or newly created destination.
    """
    print("\n")
    print("[+] Creating a PostgreSQL Destination")

    url = f"{BASE_URL}/destinations?workspaceIds={workspaceId}&includeDeleted=false&limit=20&offset=0"

    headers = {
        "accept": "application/json",
        "authorization": f"Basic {access_key}"
    }

    response = requests.get(url, headers=headers)

    if response.json() != {}:
        destinations = response.json().get('data')
        for eachDestination in destinations:
            if eachDestination.get('name') == f"{organization_id}/{sourceId}":
                print(
                    f"[+] The Destination Already Exists for {organization_id}/{sourceId}")
                destinationId = eachDestination.get('destinationId')


                print(f'[+] DestinationID: {destinationId}')
                return destinationId

    print("[+] Creating a destination ...")
    url = f"{BASE_URL}/destinations"

    db_host = "ansel.postgres.database.azure.com"
    db_port = 5432
    db_user = "harisrab"
    db_password = "44FantasticFox"

    db_name = f"{organization_id}__db"

    payload = {
        "configuration": {
            "credentials": {
                "credentials_title": "IAM Role"
            },
            "region": "",
            "lakeformation_governed_tables": False,
            "format": {
                "format_type": "JSONL",
                "compression_codec": "UNCOMPRESSED"
            },
            "partitioning": "NO PARTITIONING",
            "glue_catalog_float_as_decimal": False,
            "destinationType": "postgres",
            "schema": "public",
            "ssl_mode": {
                "mode": "allow"
            },
            "tunnel_method": {
                "tunnel_method": "NO_TUNNEL"
            },

            # Pass in the credentials
            "host": db_host,
            "port": db_port,
            "password": db_password,
            "username": db_user,
            "database": "ansel"
        },
        "name": f"{organization_id}/{sourceId}",
        "workspaceId": workspaceId
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {access_key}"
    }

    response = requests.post(url, json=payload, headers=headers)

    destinationId = response.json().get('destinationId')
    print(f'[+] DestinationID: {destinationId}')

    return destinationId


def create_shopify_datasource(password, workspaceId, shop_url):
    """
    This function creates a Shopify data source in Airbyte only if it doesn't exist before.

    Args:
        password (str): The API password for the Shopify store.
        workspaceId (str): The ID of the workspace in which the data source is to be created.
        shop_url (str): The URL of the Shopify store.

    Returns:
        None
    """
    print("\n")
    print(
        f"[+] Creating a Shopify Data Source for {shop_url} in workspaceId {workspaceId}")

    # Convert quickstart-fbfc8d49.myshopify.com → quickstart-fbfc8d49
    shop_name = shop_url.split('.')[0]

    # Get a list of sources and check if the data source for Shopify Exists or not with the same URL
    url = f"{BASE_URL}/sources?workspaceIds={workspaceId}&includeDeleted=false&limit=20&offset=0"

    headers = {
        "accept": "application/json",
        "authorization": f"Basic {access_key}"
    }

    response = requests.get(url, headers=headers)
    sources_list = response.json().get('data')

    # Check if the source 'shopify' already exists with the same shop_name
    if sources_list != None:
        for eachSource in sources_list:
            if eachSource.get('sourceType') == 'shopify' and eachSource.get('configuration').get('shop') == shop_name:
                print("[+] Shopify Source | EXISTS")
                sourceId = eachSource.get('sourceId')
                print(f'[+] SourceId: {sourceId}')


                return sourceId

    print("[+] Shopify Source | Creating ...")

    # Define the URL for the Airbyte sources API
    url = f"{BASE_URL}/sources"

    # Define the payload for the POST request
    payload = {
        "configuration": {
            "sourceType": "shopify",  # The type of the data source
            "credentials": {
                "auth_method": "api_password",  # The authentication method
                "api_password": password  # The API password
            },
            # The name of the Shopify store
            "shop": f"{shop_name}"
        },
        "name": "Shopify",  # The name of the data source
        "workspaceId": workspaceId  # The ID of the workspace
    }

    # Define the headers for the POST request
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        # The Airbyte API key
        "authorization": f"Basic {access_key}"
    }

    # Send the POST request to the Airbyte sources API
    response = requests.post(url, json=payload, headers=headers)
    # Print the response from the API

    print(response.json())

    # Get the sourceID
    sourceId = response.json().get('sourceId')
    print(f'[+] SourceId: {sourceId}')

    return sourceId


def create_workspace(organization_id):
    """
    This function creates a workspace in Airbyte if it doesn't already exist.
    It first checks if the workspace exists, if it does, it returns the workspaceId.
    If it doesn't, it creates a new workspace and returns the new workspaceId.

    Args:
        organization_id (str): The name of the organization for which the workspace is to be created.

    Returns:
        str: The workspaceId of the existing or newly created workspace.
    """
    print("[+] Creating a workspace")
    print(f"[+] Making a request to {BASE_URL}")

    url = f"{BASE_URL}/workspaces"
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {access_key}"
    }

    # Fetch existing workspaces
    response = requests.get(url, headers=headers)

    print("Successfully responsded: ", response)
    workspaces = response.json().get('data', [])

    # Check if workspace already exists
    for workspace in workspaces:
        if workspace['name'] == organization_id:
            print(
                f"[+] Workspace {organization_id} already exists with workspaceId as {workspace['workspaceId']}")
            return workspace['workspaceId']

    # If workspace does not exist, create a new one
    print(f"Workspace {organization_id} does not exist.")
    print(f"[+] Creating the workspace in Airbyte for {organization_id}")

    payload = {"name": organization_id}
    headers.update({"content-type": "application/json"})

    response = requests.post(url, json=payload, headers=headers)
    print("Response from creating: ", response.json())

    return response.json().get('workspaceId')
