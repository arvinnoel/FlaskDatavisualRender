from database import db


def fetch_products():
    """
    Fetch all data from the MongoDB collection.
    :return: A list of all documents
    """
    collection = db['shopifyProducts']
    data = collection.find()  # Fetch all records
    return list(data)
