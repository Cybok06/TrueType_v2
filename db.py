from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://truetype05:0500868021Yaw@truetype.p763ech.mongodb.net/?retryWrites=true&w=majority&appName=TrueType"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("✅ Connected to MongoDB successfully.")
except Exception as e:
    print("❌ MongoDB connection error:", e)

db = client["truetype"]

# Declare collections here
users_collection = db["users"]
clients_collection = db["clients"]
orders_collection = db["orders"]
payments_collection = db["payments"]
settings_collection = db['settings']  # ✅ Add this

