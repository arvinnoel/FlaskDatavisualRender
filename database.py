# MongoDB connection
from pymongo import MongoClient

client = MongoClient(
    "mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/?retryWrites=true&w=majority&appName"
    "=Cluster0")
db = client.get_database("RQ_Analytics")  # Replace with your database name
