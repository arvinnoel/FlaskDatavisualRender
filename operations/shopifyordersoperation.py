from database import db
from datetime import datetime


def fetch_orders():
    """
    Fetch all data from the MongoDB collection.
    :return: A list of all documents
    """
    collection = db['shopifyOrders']
    data = collection.find()  # Fetch all records
    return list(data)


def get_intervals():
    return ['daily', 'monthly', 'quarterly', 'yearly']


def get_years(group_by):
    # sales_data = get_sales_over_time(group_by)
    # year = sorted(set(item['_id']['year'] for item in sales_data if 'year' in item['_id']))
    # return year
    return [2022, 2023]


def get_sales_over_time(group_by, year):
    """
    Aggregate sales data over time, filtered by the specified year.

    :param group_by: Interval to group data by ('daily', 'monthly', 'quarterly', 'yearly')
    :param year: The year for which to fetch the sales data
    :return: Aggregated sales data
    """
    collection = db['shopifyOrders']

    # Define the group stage based on the interval
    if group_by == 'daily':
        group_stage = {
            '_id': {
                'day': {'$dayOfMonth': {'$dateFromString': {'dateString': '$created_at'}}},
                'month': {'$month': {'$dateFromString': {'dateString': '$created_at'}}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            }
        }
    elif group_by == 'monthly':
        group_stage = {
            '_id': {
                'month': {'$month': {'$dateFromString': {'dateString': '$created_at'}}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            }
        }
    elif group_by == 'quarterly':
        group_stage = {
            '_id': {
                'quarter': {'$ceil': {'$divide': [{'$month': {'$dateFromString': {'dateString': '$created_at'}}}, 3]}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            }
        }
    elif group_by == 'yearly':
        group_stage = {
            '_id': {
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            }
        }
    else:
        return []

    # Perform aggregation
    sales_data = collection.aggregate([
        # Filter by the specified year
        {'$match': {'$expr': {'$eq': [{'$year': {'$dateFromString': {'dateString': '$created_at'}}}, year]}}},
        # Group by the specified interval
        {'$group': {**group_stage, 'total_sales': {'$sum': {'$toDouble': '$total_price_set.shop_money.amount'}}}},
        # Sort results based on the group by interval
        {'$sort': {f'_id.{list(group_stage["_id"].keys())[0]}': 1}}
    ])

    return list(sales_data)


def get_sales_growth_rate(group_by, year):
    """
    Calculate the sales growth rate over time for a specific year.

    :param group_by: Interval to group data by ('daily', 'monthly', 'quarterly', 'yearly')
    :param year: The year for which to calculate the growth rate
    :return: Sales growth rate data
    """
    sales_data = get_sales_over_time(group_by, year)

    growth_rate_data = []

    for i in range(1, len(sales_data)):
        previous_period = sales_data[i - 1]
        current_period = sales_data[i]

        previous_sales = previous_period['total_sales']
        current_sales = current_period['total_sales']

        # Calculate growth rate
        if previous_sales != 0:
            growth_rate = ((current_sales - previous_sales) / previous_sales) * 100
        else:
            growth_rate = 0

        growth_rate_data.append({
            'period': current_period['_id'],
            'growth_rate': growth_rate
        })

    return growth_rate_data
