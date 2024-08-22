from flask import Blueprint, jsonify, request

from operations.shopifycustomersoperations import fetch_customers, get_new_customers, \
    get_repeat_customers_over_time, get_customer_distribution, get_clv_by_cohorts, \
    get_new_customers_years, get_customer_distribution_cities  # Import the logic
from operations.shopifyordersoperation import fetch_orders, get_sales_over_time, get_sales_growth_rate, get_intervals, \
    get_years
from operations.shopifyproductsoperation import fetch_products

# Create a Blueprint
routes_bp = Blueprint('routes_bp', __name__)


@routes_bp.route('/')
def main():
    return "Hello Arvin"


# Define a route
@routes_bp.route('/customers')
def customers():
    # Use the operation logic to fetch all data
    data = fetch_customers()

    return jsonify(data) if data else "No data found"


@routes_bp.route('/orders')
def orders():
    # Use the operation logic to fetch all data
    data = fetch_orders()

    return jsonify(data) if data else "No data found"


@routes_bp.route('/products')
def products():
    # Use the operation logic to fetch all data
    data = fetch_products()

    return jsonify(data) if data else "No data found"


@routes_bp.route('/sales/intervals', methods=['GET'])
def fetch_intervals():
    intervals = get_intervals()
    return jsonify(intervals)


@routes_bp.route('/sales/years', methods=['GET'])
def fetch_years():
    group_by = request.args.get('group_by', 'yearly')
    years = get_years(group_by)
    return jsonify(years)


@routes_bp.route('/sales/<string:interval>/<int:year>', methods=['GET'])
def get_sales(interval, year):
    """
    Endpoint to fetch sales data grouped by the specified interval and year.
    :param interval: The interval to group by ('daily', 'monthly', 'quarterly', 'yearly')
    :param year: The year for which to fetch the sales data
    :return: JSON response with aggregated sales data
    """
    valid_intervals = ['daily', 'monthly', 'quarterly', 'yearly']

    if interval not in valid_intervals:
        return jsonify({"error": "Invalid interval. Choose from 'daily', 'monthly', 'quarterly', 'yearly'."}), 400

    sales_data = get_sales_over_time(interval, year)
    return jsonify(sales_data), 200


@routes_bp.route('/sales-growth/<string:interval>/<int:year>', methods=['GET'])
def get_sales_growth(interval, year):
    """
    Endpoint to calculate the sales growth rate for the specified interval and year.
    :param interval: The interval to group by ('daily', 'monthly', 'quarterly', 'yearly')
    :param year: The year to filter by
    :return: JSON response with sales growth rate data
    """
    valid_intervals = ['daily', 'monthly', 'quarterly', 'yearly']

    if interval not in valid_intervals:
        return jsonify({"error": "Invalid interval. Choose from 'daily', 'monthly', 'quarterly', 'yearly'."}), 400

    growth_rate_data = get_sales_growth_rate(interval, year)
    return jsonify(growth_rate_data), 200


@routes_bp.route('/new-customers', methods=['GET'])
def fetch_new_customers():
    """
    Endpoint to fetch new customer names and creation dates.
    :return: JSON response with customer names and their creation dates.
    """
    customer_data = get_new_customers()
    return jsonify(customer_data), 200


@routes_bp.route('/new-customers/years', methods=['GET'])
def fetch_new_customer_year():
    years = get_new_customers_years()
    return jsonify(years)


@routes_bp.route('/repeat-customers/<string:interval>', methods=['GET'])
def fetch_repeat_customers(interval):
    """
    Endpoint to fetch repeat customer data grouped by the specified interval.
    :param interval: The interval to group by ('daily', 'monthly', 'quarterly', 'yearly')
    :return: JSON response with repeat customer data
    """
    valid_intervals = ['daily', 'monthly', 'quarterly', 'yearly']

    if interval not in valid_intervals:
        return jsonify({"error": "Invalid interval. Choose from 'daily', 'monthly', 'quarterly', 'yearly'."}), 400

    repeat_customers = get_repeat_customers_over_time(interval)
    return jsonify(repeat_customers), 200


@routes_bp.route('/customer-distribution', methods=['GET'])
def fetch_customer_distribution():
    """
    Endpoint to fetch the geographical distribution of customers.
    :return: JSON response with the number of customers per city.
    """
    customer_distribution = get_customer_distribution()
    return jsonify(customer_distribution), 200


@routes_bp.route('/distributed-cities', methods=['GET'])
def unique_cities():
    """
    Endpoint to get the list of unique cities.

    :return: JSON response with a list of unique cities.
    """
    cities = get_customer_distribution_cities()
    return jsonify(cities)


@routes_bp.route('/clv-by-cohorts/<year>', methods=['GET'])
def fetch_clv_by_cohorts(year):
    """
    Endpoint to fetch the Customer Lifetime Value by cohorts for a specific year.
    :param year: The year to filter the results by (e.g., '2020' or '2021').
    :return: JSON response with the CLV by cohort for the specified year.
    """
    # Validate the year parameter
    if year not in ['2020', '2021']:
        return jsonify({"error": "Invalid year. Only 2020 and 2021 are supported."}), 400

    clv_by_cohorts = get_clv_by_cohorts(year)
    return jsonify(clv_by_cohorts), 200
