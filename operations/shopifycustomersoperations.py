from datetime import datetime

from database import db


def fetch_customers():
    """
    Fetch all data from the MongoDB collection.
    :return: A list of all documents
    """
    collection = db['shopifyCustomers']
    data = collection.find()  # Fetch all records
    return list(data)


def get_new_customers_years():
    return [2020, 2021]


def get_new_customers():
    """
    Fetch the names and creation dates of new customers.

    :return: A list of dictionaries containing customer names and their creation dates.
    """
    collection = db['shopifyCustomers']

    # Select only the fields 'first_name', 'last_name', and 'created_at'
    customer_data = collection.find({}, {'first_name': 1, 'last_name': 1, 'created_at': 1, '_id': 0})

    # Convert the cursor to a list and return
    return list(customer_data)


def get_repeat_customers_over_time(group_by):
    """
    Identify customers with more than one purchase across different time frames.

    :param group_by: Interval to group data by ('daily', 'monthly', 'quarterly', 'yearly')
    :return: A list of customers with more than one purchase, grouped by the specified time frame.
    """
    collection = db['shopifyOrders']

    # Define the group stage based on the specified interval
    if group_by == 'daily':
        group_stage = {
            '_id': {
                'customer_id': '$customer.id',
                'day': {'$dayOfMonth': {'$dateFromString': {'dateString': '$created_at'}}},
                'month': {'$month': {'$dateFromString': {'dateString': '$created_at'}}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            },
            'total_purchases': {'$sum': 1}
        }
    elif group_by == 'monthly':
        group_stage = {
            '_id': {
                'customer_id': '$customer.id',
                'month': {'$month': {'$dateFromString': {'dateString': '$created_at'}}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            },
            'total_purchases': {'$sum': 1}
        }
    elif group_by == 'quarterly':
        group_stage = {
            '_id': {
                'customer_id': '$customer.id',
                'quarter': {'$ceil': {'$divide': [{'$month': {'$dateFromString': {'dateString': '$created_at'}}}, 3]}},
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            },
            'total_purchases': {'$sum': 1}
        }
    elif group_by == 'yearly':
        group_stage = {
            '_id': {
                'customer_id': '$customer.id',
                'year': {'$year': {'$dateFromString': {'dateString': '$created_at'}}}
            },
            'total_purchases': {'$sum': 1}
        }
    else:
        return []

    # Perform aggregation to find repeat customers
    repeat_customers = collection.aggregate([
        {'$group': group_stage},
        {'$match': {'total_purchases': {'$gt': 1}}},
        {'$lookup': {
            'from': 'shopifyCustomers',
            'localField': '_id.customer_id',
            'foreignField': 'id',
            'as': 'customer_info'
        }},
        {'$unwind': '$customer_info'},
        {'$project': {
            'customer_id': '$_id.customer_id',
            'first_name': '$customer_info.first_name',
            'last_name': '$customer_info.last_name',
            'total_purchases': 1,  # Include total number of purchases
            'time_period': '$_id'
        }},
        {'$sort': {'total_purchases': -1}}
    ])

    return list(repeat_customers)


def get_customer_distribution():
    """
    Fetch the geographical distribution of customers based on their city,
    including customer names.

    :return: A list of dictionaries containing customer cities, counts, and names.
    """
    collection = db['shopifyCustomers']

    # Aggregate the number of customers per city and include customer names
    customer_distribution = collection.aggregate([
        {'$group': {
            '_id': '$default_address.city',
            'count': {'$sum': 1},
            'names': {'$push': {'first_name': '$first_name'}}
        }},
        {'$match': {'_id': {'$ne': None}}},
        {'$sort': {'count': -1}}
    ])

    return list(customer_distribution)


def get_customer_distribution_cities():
    """
    Fetch a unique list of cities from the 'shopifyCustomers' collection.

    :return: A list of dictionaries containing unique city names.
    """
    collection = db['shopifyCustomers']

    # Aggregate unique city names
    unique_cities = collection.aggregate([
        {'$group': {
            '_id': '$default_address.city'
        }},
        {'$match': {'_id': {'$ne': None}}},  # Exclude None values if there are any
        {'$sort': {'_id': 1}}  # Optional: Sort cities alphabetically
    ])

    # Convert to list of dictionaries with city names
    return [{'city': city['_id']} for city in unique_cities]


def get_clv_by_cohorts(year):
    """
    Calculate the Customer Lifetime Value (CLV) by cohorts based on the month of their first purchase.
    Includes customer names in the result. Filters data based on the specified year.

    :param year: The year to filter the results by (e.g., '2020' or '2021').
    :return: A list of dictionaries containing cohort information, average CLV, and customer names.
    """
    # Fetch total spent per customer
    orders_collection = db['shopifyOrders']
    total_spent_per_customer = orders_collection.aggregate([
        {'$group': {
            '_id': '$customer.id',
            'total_spent': {'$sum': {'$toDouble': '$total_price'}}
        }}
    ])

    # Fetch first purchase date and names for each customer
    customer_collection = db['shopifyCustomers']
    first_purchase_dates = {}
    customer_names = {}

    for customer in customer_collection.find():
        customer_id = str(customer['_id'])
        if 'created_at' in customer:
            first_purchase_date = customer['created_at']
            first_purchase_datetime = datetime.strptime(first_purchase_date, '%Y-%m-%dT%H:%M:%S%z')
            if first_purchase_datetime.year == int(year):
                first_purchase_month = first_purchase_datetime.strftime('%Y-%m')
                first_purchase_dates[customer_id] = first_purchase_month

        if 'first_name' in customer and 'last_name' in customer:
            customer_names[customer_id] = f"{customer['first_name']} {customer['last_name']}"

    # Calculate CLV by cohort
    cohort_clv = {}
    customer_details = {}

    for customer_spending in total_spent_per_customer:
        customer_id = str(customer_spending['_id'])
        total_spent = customer_spending['total_spent']

        if customer_id in first_purchase_dates:
            cohort_month = first_purchase_dates[customer_id]
            customer_name = customer_names.get(customer_id, "Unknown")

            if cohort_month not in cohort_clv:
                cohort_clv[cohort_month] = {'total_value': 0, 'customer_count': 0, 'customer_names': set()}

            cohort_clv[cohort_month]['total_value'] += total_spent
            cohort_clv[cohort_month]['customer_count'] += 1
            cohort_clv[cohort_month]['customer_names'].add(customer_name)

    # Calculate average CLV for each cohort and include customer names
    result = []
    for cohort, data in cohort_clv.items():
        average_clv = data['total_value'] / data['customer_count']
        result.append({
            'cohort': cohort,
            'average_clv': average_clv,
            'customer_names': list(data['customer_names'])
        })

    return result
