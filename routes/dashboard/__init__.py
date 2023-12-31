from datetime import datetime, timedelta, timezone

import duckdb
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import Response

from models import GetProducts

dashboard_router = APIRouter()


@dashboard_router.post("/dashboard/get_products")
async def get_products(req: Request):
    import time
    print('[+] Fetching Products')

    body = await req.json()
    organization_id = body.get("organization_id")

    start_time = time.time()

    cmd = """
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_region='us-east-1';
    
    SET s3_access_key_id='AKIATXDG3RX4HC5MR2NQ';
    SET s3_secret_access_key='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';
    
    SELECT 
        product_id,
        COUNT(variant_id) AS variant_count,
        ANY_VALUE(title),
        ANY_VALUE(image),
        ANY_VALUE(ordered_item_quantity),
        ANY_VALUE(tags),
    FROM read_parquet('s3://ansel-assets/dovetail-workwear/clean_products.parquet') 
    GROUP BY product_id
    """

    import json

    products_list = duckdb.sql(cmd)

    products_list_formatted = (
        products_list.df()
        .rename(
            columns={
                "any_value(title)": "title",
                "any_value(image)": "image",
                "any_value(ordered_item_quantity)": "total_quantity",
                "any_value(tags)": "tags",
            }
        )
  
        .to_dict(orient="records")
    )

    products_list_formatted.sort(key=lambda x: x.get('total_quantity', 0), reverse=True)

    amended_products_list = [
        {**product, 'tags': product.get('tags', []) + ['all']} for product in products_list_formatted
    ]



    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")

    return amended_products_list

from datetime import datetime


class TimeSpan(BaseModel):
    start: str
    end:  str

    
class ChartData(BaseModel):
    bucketing: str
    selectedProduct: int
    forecast_period: str
    timespan: TimeSpan

def request_key_builder(
    func,
    namespace: str = "",
    args: list = None,
    request: Request = None,
    response: Response = None,
    **kwargs
):
    print("==> ", request)
    print("==> ", request.method)
    print("==> ", kwargs)
    print("===> ", args)

    chart_data = kwargs['kwargs']['chart_data']


    string_to_check = ":".join([
        namespace,
        request.method.lower(),
        request.url.path,
        str(chart_data.bucketing),
        str(chart_data.forecast_period),
        str(chart_data.selectedProduct),
    ])

    print("==> ", string_to_check)

    return string_to_check

@dashboard_router.post("/dashboard/get_chart_data")
# @cache(namespace="test", expire=120, key_builder=request_key_builder)
async def get_chart_data(chart_data: ChartData):
    print(f"[+] Fetching Chart Data for {int(chart_data.selectedProduct)}")
    import time
    start_time_total = time.time()

    bucketing = str(chart_data.bucketing).capitalize()
    forecast_period = int(chart_data.forecast_period)
    selectedProduct = int(chart_data.selectedProduct)
    timespan = chart_data.timespan

    start_time = datetime.fromisoformat(timespan.start)
    end_time = datetime.fromisoformat(timespan.end)
    current_time = datetime.now(timezone.utc)

    print('\n')
    print("Bucketing:", bucketing)
    print("Forecast Period:", forecast_period)
    print("Selected Product:", selectedProduct)
    print("Start Time:", start_time)
    print("End Time:", end_time)
    print("Current Time: ", current_time)
    print('\n')

    con = duckdb.connect('md:my_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiaGFyaXMubXlhbnNlbC5jb20iLCJlbWFpbCI6ImhhcmlzQG15YW5zZWwuY29tIiwidXNlcklkIjoiNmVjZTNjNjItNTZlMS00NjExLWExYjQtODA4OTMxZTU0Nzc3IiwiaWF0IjoxNzAzMjU3MDA0LCJleHAiOjE3MzQ4MTQ2MDR9.PDH_SYX8Y3jkKCfZ-9HLvS3vzd2wovjfN-f6aKdE7GM') 

    cmd = f"""
    -- INSTALL httpfs;
    -- LOAD httpfs;

    SET s3_region='us-east-1';
    
    SET s3_access_key_id='AKIATXDG3RX4HC5MR2NQ';
    SET s3_secret_access_key='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';
    
    SELECT 
        *
    FROM orders 
    WHERE product_id = {selectedProduct}
    AND frequency = '{bucketing}'
    """

    time_format = "%Y-%m-%d"

    if bucketing == 'M' and end_time >= current_time.replace(day=1, tzinfo=None):
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"

    elif bucketing == 'W' and (end_time.replace(tzinfo=None) >= (current_time - timedelta(days=current_time.weekday())).replace(tzinfo=None)):
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"

    else:
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"
        cmd += f"AND day <= '{end_time.strftime(time_format)}'\n"

    cmd += "ORDER BY day\n    """

    query_start_time = time.time()

    import pandas as pd
    table = con.sql(cmd).df()
    query_end_time = time.time()
    print(f"Query Execution Time: {query_end_time - query_start_time} seconds")

    print(table)

    demand_rows = table[table['type'] == 'Demand']
    forecast_rows = table[table['type'] == 'Forecast'].head(forecast_period)

    table = pd.concat([demand_rows, forecast_rows])

    # Format this table into a list of graph_data objects.
    

    import json

    # Flatten the 'source' column which contains JSON into separate columns
    attributed_marketing_sales = table.to_dict(orient='records')

    print(f"{attributed_marketing_sales=}")

    # attribution_sources = []

    # if (attributed_marketing_sales[0]['source'] != ""):
    #     attribution_sources = {key: 0 for key in json.loads(attributed_marketing_sales[0]['source'].replace("'", '"')).keys()}

    def mapping(x):
        # print(f"{x['day']=}")

        if len(x['day']) == 10:  # Date is in 'YYYY-MM-DD' format
            x['day'] = datetime.strptime(x['day'], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        else:
            x['day'] = datetime.strptime(x['day'], "%Y-%m-%d %H:%M:%S")

        if bucketing == 'M':
            formatted_date = x['day'].strftime('%b %y')
        else:
            formatted_date = x['day'].strftime(time_format)

        # Remove specified keys from the dictionary and return the rest
        keys_to_remove = ['ordered_item_quantity', 'day', 'index', 'title', 'order_id', 'type', 'frequency']
        source_data = {key: value for key, value in x.items() if key not in keys_to_remove}

        chart_data = {
            'date': formatted_date,
            'total_quantity': x['ordered_item_quantity'],
            'type': x['type'],
            **source_data
        }

        return chart_data

    mapping_start_time = time.time()

    data = list(map(mapping, attributed_marketing_sales))
    
    mapping_end_time = time.time()
    print(f"Mapping Execution Time: {mapping_end_time - mapping_start_time} seconds")

    for each in data:
        print('==> ', each)
        print('\n')

    keys_to_exclude = ['ordered_item_quantity', 'date', 'product_id', 'order_id', 'type', 'frequency']
    source_keys = [key for key in data[0].keys() if key not in keys_to_exclude]

    # print(source_keys)

    # end_time_total = time.time()
    # print(f"Total Execution Time: {end_time_total - start_time_total} seconds")

    return data, source_keys

import json
import re


@dashboard_router.post("/dashboard/get_campaign_breakdown")
async def get_campaign_data(chart_data: ChartData):
    import time
    start_time_total = time.time()

    bucketing = str(chart_data.bucketing).capitalize()
    forecast_period = int(chart_data.forecast_period)
    selectedProduct = int(chart_data.selectedProduct)
    timespan = chart_data.timespan

    start_time = datetime.fromisoformat(timespan.start)
    end_time = datetime.fromisoformat(timespan.end)
    current_time = datetime.now(timezone.utc)

    print('\n')
    print("Data for Campaign Breakdown")
    print('---------------------------')
    print("Bucketing:", bucketing)
    print("Forecast Period:", forecast_period)
    print("Selected Product:", selectedProduct)
    print("Start Time:", start_time)
    print("End Time:", end_time)
    print("Current Time: ", current_time)
    print('\n')


    con = duckdb.connect('md:my_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiaGFyaXMubXlhbnNlbC5jb20iLCJlbWFpbCI6ImhhcmlzQG15YW5zZWwuY29tIiwidXNlcklkIjoiNmVjZTNjNjItNTZlMS00NjExLWExYjQtODA4OTMxZTU0Nzc3IiwiaWF0IjoxNzAzMjU3MDA0LCJleHAiOjE3MzQ4MTQ2MDR9.PDH_SYX8Y3jkKCfZ-9HLvS3vzd2wovjfN-f6aKdE7GM') 

    cmd = f"""
    -- INSTALL httpfs;
    -- LOAD httpfs;

    SET s3_region='us-east-1';
    
    SET s3_access_key_id='AKIATXDG3RX4HC5MR2NQ';
    SET s3_secret_access_key='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';
    
    SELECT 
        *
    FROM campaign_breakdown 
    WHERE product_id = {selectedProduct}
    AND frequency = '{bucketing}'
    """

    time_format = "%Y-%m-%d"

    if bucketing == 'monthly' and end_time >= current_time.replace(day=1):
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"

    elif bucketing == 'weekly' and (end_time >= current_time - timedelta(days=current_time.weekday())):
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"

    else:
        cmd += f"AND day >= '{start_time.strftime(time_format)}'\n"
        cmd += f"AND day <= '{end_time.strftime(time_format)}'\n"

    cmd += "ORDER BY day\n    """

    import pandas as pd

    start_time_query = time.time()
    table = con.sql(cmd).df()
    print("Time taken for query: {:.2f} seconds".format(time.time() - start_time_query))


    print(table)
    timeBucketsOfCampaigns = list(table['campaigns_contribution'].tolist())
    
    all_campaigns = {}

    for eachTime in timeBucketsOfCampaigns:
        eachTime = json.loads(eachTime)
        
        for eachSource in eachTime.keys():
            if eachSource not in all_campaigns:
                all_campaigns[eachSource] = {}

            for eachCampaign in eachTime[eachSource].keys():
                if eachCampaign not in all_campaigns[eachSource]:
                    all_campaigns[eachSource][eachCampaign] = 0

                all_campaigns[eachSource][eachCampaign] += eachTime[eachSource][eachCampaign]

    # Convert all_campaigns to the desired structure
    formatted_campaigns = []
    for source, campaigns in all_campaigns.items():
        source_name_parts = source.split('__')
        source_name = ' | '.join(' '.join(part.upper() if part == 'sms' else part.title() for part in subpart.split('_')) for subpart in source_name_parts)

        formatted_source = {'source': source_name, 'campaigns': []}
        
        for campaign_name, contribution in campaigns.items():
            
            # Generate a simplified ID by removing special characters and spaces
            campaign_id = re.sub(r'[^a-zA-Z0-9]+', '', campaign_name).lower()
            formatted_campaign = {
                'id': campaign_id,
                'title': campaign_name,
                'contribution': contribution
            }
            formatted_source['campaigns'].append(formatted_campaign)
        formatted_campaigns.append(formatted_source)

    print(json.dumps(formatted_campaigns, indent=2))

    
    return formatted_campaigns
