from pymongo import ASCENDING, DESCENDING
from db import db  # ✅ Import the existing MongoDB connection from your project

# Collections
orders = db["orders"]
clients = db["clients"]

# Create indexes for faster queries
orders.create_index([("status", ASCENDING), ("date", DESCENDING)])
orders.create_index([("client_id", ASCENDING)])

clients.create_index("client_id", unique=True)

print("✅ Indexes created successfully.")
