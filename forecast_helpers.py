import numpy as np
import pandas as pd
from tqdm import tqdm


def generate_forecast_on_all(df, bucketing_duration: str, organization_id: str):
    assert bucketing_duration in [
        "daily",
        "weekly",
        "monthly",
        "quarterly",
    ], "Invalid bucketing_duration. Must be one of ['daily', 'weekly', 'monthly', 'quarterly']"

    # Get the list of products
    products = [
        {"product_title": title} for title in list(set(df["product_title"].tolist()))
    ]

    # (2) Loop over the products and get the demand (select duration) -> Use that to train the model and get the parameters for each.
    for i, eachProduct in enumerate(products):
        # The product
        product_title = eachProduct["product_title"]

        print(f"[{i}/{len(products)}] Product Title being Processed: ", product_title)

        # The demand or the volume of the products sold in the past
        filtered_df = df[df["product_title"] == product_title]

        # Bucket up the incoming demand
        demand, day_list = source_demand_bucketing(
            filtered_df, duration=bucketing_duration
        )

        # Segregating small records bcz here they are considered corrupted.
        if len(demand) > 5:
            params, forecast_table = exp_smooth_opti(demand, day_list)
            products[i] = {
                "organization_id": organization_id,
                "product_title": product_title,
                "params": params,
                "demand": demand,
                "forecast_table": forecast_table.to_json(),
            }

        else:
            continue

    # (3) Compute the forecast on every product
    # Remove all the items in products that do not have `params` key
    products = [item for item in products if "params" in item]

    return products


def seasonal_factors_add(s, d, slen, cols):

    for i in range(slen):

        s[i] = np.mean(d[i:cols:slen])

    s -= np.mean(s[:slen])

    return s


def extend_datetime_arr(date_strings):
    from datetime import datetime, timedelta

    # Convert strings to datetime objects, ignoring 'nan'
    dates = [
        datetime.strptime(s, "%Y-%m-%d") if s != "nan" else "nan" for s in date_strings
    ]

    # Calculate the difference between consecutive dates
    diffs = [
        (dates[i + 1] - dates[i]).days
        for i in range(len(dates) - 2)
        if dates[i] != "nan" and dates[i + 1] != "nan"
    ]

    # Average the differences to get a typical difference
    if diffs:
        avg_diff = sum(diffs) / len(diffs)
    else:
        avg_diff = 1  # or any other default value

    # Replace 'nan' with the date in sequence
    for i in range(1, len(dates)):
        if dates[i] == "nan":
            dates[i] = dates[i - 1] + timedelta(days=avg_diff)

    # Convert datetime objects back to strings
    date_strings = [
        date.strftime("%Y-%m-%d") if date != "nan" else "nan" for date in dates
    ]

    return date_strings


def triple_exp_smooth_add(
    demand,
    day_list,
    slen=12,
    extra_periods=1,
    params={"alpha": 0.4, "beta": 0.4, "phi": 0.9, "gamma": 0.3},
):
    cols = len(demand)  # Historical pteriod length
    d = np.append(demand, [np.nan] * extra_periods)

    # Take out the params
    alpha, beta, phi, gamma = params.values()

    # components initialization
    f, a, b, s = np.full((4, cols + extra_periods), np.nan)
    s = seasonal_factors_add(s, d, slen, cols)

    # Level & Trend initialization
    a[0] = d[0] - s[0]
    b[0] = (d[1] - s[1]) - (d[0] - s[0])

    # Create the forecast for the first season
    for t in range(1, slen):
        f[t] = a[t - 1] + phi * b[t - 1] + s[t]
        a[t] = alpha * (d[t] - s[t]) + (1 - alpha) * (a[t - 1] + phi * b[t - 1])
        b[t] = beta * (a[t] - a[t - 1]) + (1 - beta) * phi * b[t - 1]

    # Create all the t + 1 forecast
    for t in range(slen, cols):
        f[t] = a[t - 1] + phi * b[t - 1] + s[t - slen]
        a[t] = alpha * (d[t] - s[t - slen]) + (1 - alpha) * (a[t - 1] + phi * b[t - 1])
        b[t] = beta * (a[t] - a[t - 1]) + (1 - beta) * phi * b[t - 1]
        s[t] = gamma * (d[t] - a[t]) + (1 - gamma) * s[t - slen]

    # Forecast for all extra periods
    for t in range(cols, cols + extra_periods):
        f[t] = a[t - 1] + phi * b[t - 1] + s[t - slen]
        a[t] = f[t] - s[t - slen]
        b[t] = phi * b[t - 1]
        s[t] = s[t - slen]

    df = pd.DataFrame.from_dict(
        {
            "Demand": d,
            "Forecast": f,
            "Error": d - f,
            "Level": a,
            "Trend": b,
            "Season": s,
            "Day": extend_datetime_arr(np.append(day_list, [np.nan] * extra_periods)),
        }
    )

    return df


# Optimization Algorithm


# Define the ranges for potential values for parameters
alpha_values = [i / 100 for i in range(1, 60, 7)]
beta_values = [i / 100 for i in range(1, 41, 7)]
phi_values = [i / 100 for i in range(80, 99, 7)]
gamma_values = [i / 100 for i in range(10, 30, 7)]

# The number of combinations to search
print(
    f"Combinations to search: {len(alpha_values) * len(beta_values) * len(phi_values) * len(gamma_values)}"
)


def exp_smooth_opti(demand, day_list, extra_periods=12):
    params = []  # contains all the different parameters settings
    KPIs = []  # contains all the different KPIs
    dfs = []  # contains all the DataFrames returned by each of the models.

    for alpha in alpha_values:
        for beta in beta_values:
            for phi in phi_values:
                for gamma in gamma_values:

                    # Compute the results from the model
                    df = triple_exp_smooth_add(
                        demand=demand,
                        day_list=day_list,
                        slen=12,
                        extra_periods=extra_periods,
                        params={
                            "alpha": alpha,
                            "beta": beta,
                            "phi": phi,
                            "gamma": gamma,
                        },
                    )

                    # Add the parameters to the list of parameters
                    params.append(
                        {
                            "type": "Triple Exponential with Damped Trend + Seasonality",
                            "alpha": alpha,
                            "beta": beta,
                            "phi": phi,
                            "gamma": gamma,
                        }
                    )

                    # Add the results to the list of DataFrames
                    dfs.append(df)

                    # Metric = df['Error'].abs().mean() # MAE
                    metric = np.sqrt((df["Error"] ** 2).mean())  # RMSE

                    KPIs.append(metric)

    minimum_kpi = np.argmin(KPIs)
    # print(f"The best solution found for {params[minimum_kpi]['type']} with MAE of {round(KPIs[minimum_kpi], 2)}")
    # print(f"Parameters: {params[minimum_kpi]}")

    return params[minimum_kpi], dfs[minimum_kpi]


def source_demand_bucketing(df, duration="weekly"):
    """
    This function takes a DataFrame of sales data and returns a list of demand values,
    where the dates are bucketed according to the specified duration (daily, weekly, monthly, or quarterly).
    For each period, the 'ordered_item_quantity' values are summed to give the total demand for that period.

    Parameters:
    df (pandas.DataFrame): The sales data, which must include 'day' and 'ordered_item_quantity' columns.
    duration (str): The duration for bucketing the dates. Options are 'daily', 'weekly', 'monthly', or 'quarterly'.
                     Default is 'weekly'.

    Returns:
    demand_list (list): A list of demand values for each period.
    """

    df = df.copy()

    # Convert 'day' column to datetime
    df["day"] = pd.to_datetime(df["day"])

    # source_demand_bucketing(df.sort_values(['product_title', 'day']), duration="daily").head(20)
    if duration == "daily":
        demand_df = (
            df.sort_values(["product_title", "day"])
            .resample("D", on="day")["ordered_item_quantity"]
            .sum()
            .reset_index()
        )

    elif duration == "weekly":
        demand_df = (
            df.sort_values(["product_title", "day"])
            .resample("W", on="day")["ordered_item_quantity"]
            .sum()
            .reset_index()
        )

    elif duration == "monthly":
        demand_df = (
            df.sort_values(["product_title", "day"])
            .resample("M", on="day")["ordered_item_quantity"]
            .sum()
            .reset_index()
        )

    elif duration == "quarterly":
        demand_df = (
            df.sort_values(["product_title", "day"])
            .resample("Q", on="day")["ordered_item_quantity"]
            .sum()
            .reset_index()
        )

    else:
        raise ValueError(
            "Invalid duration. Options are 'daily', 'weekly', 'monthly', or 'quarterly'."
        )

    # Convert the 'ordered_item_quantity' column to a list
    demand_list = demand_df["ordered_item_quantity"].tolist()
    day_list = [
        timestamp.strftime("%Y-%m-%d") for timestamp in demand_df["day"].to_list()
    ]

    return demand_list, day_list
