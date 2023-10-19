import os

import requests
from dotenv import load_dotenv

load_dotenv()


def create_connection(workspaceId, sourceId, destinaionId):

    print("\n")
    print("[+] Creating a connection")
    print(f"Source ID: {sourceId}")
    print(f"Destination ID: {destinaionId}")

    url = "https://api.airbyte.com/v1/connections"

    payload = {
        "schedule": {"scheduleType": "manual"},
        "dataResidency": "auto",
        "namespaceDefinition": "destination",
        "namespaceFormat": None,
        "nonBreakingSchemaUpdatesBehavior": "ignore",
        "sourceId": sourceId,
        "destinationId": destinaionId
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    response = requests.post(url, json=payload, headers=headers)

    print(response.json())


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

    url = f"https://api.airbyte.com/v1/destinations?workspaceIds={workspaceId}&includeDeleted=false&limit=20&offset=0"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    response = requests.get(url, headers=headers)

    if response.json() != {}:
        destinations = response.json().get('data')
        for eachDestination in destinations:
            if eachDestination.get('name') == f"{organization_id}/{sourceId}":
                print(
                    f"[+] The Destination Already Exists for {organization_id}/{sourceId}")

                return eachDestination.get('destinationId')

    print("[+] Creating a destination")
    url = "https://api.airbyte.com/v1/destinations"

    payload = {
        "configuration": {
            "credentials": {"credentials_title": "IAM Role"},
            "region": "",
            "lakeformation_governed_tables": False,
            "format": {
                "format_type": "Parquet",
                "compression_codec": "UNCOMPRESSED"
            },
            "partitioning": "NO PARTITIONING",
            "glue_catalog_float_as_decimal": False,
            "destinationType": "s3",
            "s3_bucket_region": "us-east-1",
            "access_key_id": "AKIATXDG3RX4OBIE4EO3",
            "secret_access_key": "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",
            "s3_bucket_name": "ansel_source_of_truth",
            "s3_bucket_path": f"{organization_id}/{sourceId}/"
        },
        "name": f"{organization_id}/{sourceId}",
        "workspaceId": workspaceId
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    response = requests.post(url, json=payload, headers=headers)

    destinationId = response.json().get('destinationId')

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
    print(
        f"[+] Creating a Shopify Data Source for {shop_url} in workspaceId {workspaceId}")

    # Convert quickstart-fbfc8d49.myshopify.com → quickstart-fbfc8d49
    shop_name = shop_url.split('.')[0]

    # Get a list of sources and check if the data source for Shopify Exists or not with the same URL
    url = f"https://api.airbyte.com/v1/sources?workspaceIds={workspaceId}&includeDeleted=false&limit=20&offset=0"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    response = requests.get(url, headers=headers)
    sources_list = response.json().get('data')

    print(f"{response.json()=}")
    print(f"{sources_list=}")

    # Check if the source 'shopify' already exists with the same shop_name
    if sources_list != None:
        for eachSource in sources_list:
            if eachSource.get('sourceType') == 'shopify' and eachSource.get('configuration').get('shop') == shop_name:
                sourceId = eachSource.get('sourceId')

                return sourceId

    # Define the URL for the Airbyte sources API
    url = "https://api.airbyte.com/v1/sources"

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
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    # Send the POST request to the Airbyte sources API
    response = requests.post(url, json=payload, headers=headers)
    # Print the response from the API

    # Get the sourceID
    sourceId = response.json().get('sourceId')

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
    url = "https://api.airbyte.com/v1/workspaces"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.getenv('AIRBYTE_KEY')}"
    }

    # Fetch existing workspaces
    response = requests.get(url, headers=headers)
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
