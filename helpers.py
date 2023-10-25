import json

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def balance_elements(data_dict):
    """
    Balance the number of elements in each list in a dictionary.

    This function iterates over each key-value pair in the dictionary. If the value is a list,
    it appends Nones to the list until its length equals the maximum list length found in the
    dictionary. If the value is not a list, it is left unchanged.

    Args:
    data_dict (dict): The dictionary whose lists are to be balanced.

    Returns:
    dict: A dictionary with balanced lists.
    """

    values = data_dict.values()

    num_lists = sum(1 for v in values if isinstance(v, list))

    # If there are no lists in the dictionary values, just return the original dictionary
    if num_lists == 0:
        return data_dict

    # Get the max length among all value lists in the dictionary
    max_length = max(len(v) for v in values if isinstance(v, list))

    # Make all lists in the dictionary the same length as max_length
    for key, value in data_dict.items():
        if isinstance(value, list):
            diff = max_length - len(value)
            if diff > 0:
                data_dict[key].extend([None]*diff)

    return data_dict


def create_database_for_customer(db_host, db_port, db_user, db_password, db_name):
    """
    This function creates a new database for a customer in PostgreSQL.

    Args:
    db_host (str): The host name of the PostgreSQL server.
    db_port (int): The port number on which the PostgreSQL server is listening.
    db_user (str): The username to connect to the PostgreSQL server.
    db_password (str): The password to connect to the PostgreSQL server.
    db_name (str): The name of the database to be created.
    """

    # Print the input values in a nicely formatted manner
    print(f"Host: {db_host}")
    print(f"Port: {db_port}")
    print(f"User: {db_user}")
    print(f"Password: {db_password}")
    print(f"Database Name: {db_name}")

    # Connect to PostgreSQL
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password
        )
    except psycopg2.Error as error:
        print(f"Error while connecting to PostgreSQL: {error}")

    # Set AUTOCOMMIT to create a new database
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Create a new cursor object
    cursor = connection.cursor()

    # SQL query to create a new database
    create_database_query = f"CREATE DATABASE {db_name};"

    # Execute the query
    cursor.execute(create_database_query)

    # Close the cursor and connection
    cursor.close()
    connection.close()


def fetch_data_from_db_for_forecasting(organization_id, host, username, password, database):
    """
    Fetch data from a PostgreSQL database for forecasting.

    This function connects to a PostgreSQL database, executes a SQL query to fetch specific fields from a JSON object
    stored in a column, and then converts the results into a pandas DataFrame.

    Args:
        host (str): The host name of the PostgreSQL server.
        username (str): The username to connect to the PostgreSQL server.
        password (str): The password to connect to the PostgreSQL server.
        database (str): The name of the database to connect to.

    Returns:
        df (pandas.DataFrame): A DataFrame containing the fetched data, with columns for 'product_title', 'title',
                               'ordered_item_quantity', 'variant_title', 'sku', and 'day'. The 'day' column is
                               converted to the 'YYYY-MM-DD' format.
    """


    # Database connection details
    host = host
    username = username
    password = password
    database = database

    # Connect to the database
    conn = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cur = conn.cursor()

    # Transform organization_id into the required format
    folder_name = "__" + organization_id.replace("-", "_")

    # Execute the query to get the required data
    cur.execute(f"""
        SELECT 
            jsonb_array_elements(_airbyte_data->'line_items')->>'name' as name,
            jsonb_array_elements(_airbyte_data->'line_items')->>'title' as title,
            jsonb_array_elements(_airbyte_data->'line_items')->>'quantity' as quantity,
            jsonb_array_elements(_airbyte_data->'line_items')->>'variant_title' as variant_title,
            jsonb_array_elements(_airbyte_data->'line_items')->>'sku' as sku,
            _airbyte_data->'processed_at' as processed_at
        FROM {folder_name}._airbyte_raw_orders
    """)

    # Fetch the results as a list of tuples
    results = [row for row in cur.fetchall()]

    # Convert the list of tuples into a dataframe
    import pandas as pd
    df = pd.DataFrame(results, columns=[
                      'product_title', 'title', 'ordered_item_quantity', 'variant_title', 'sku', 'day'])
    df['ordered_item_quantity'] = df['ordered_item_quantity'].astype(int)

    df['day'] = pd.to_datetime(df['day']).apply(
        lambda x: x.strftime('%Y-%m-%d'))

    # Close the database connection
    cur.close()
    conn.close()

    return df
