import os
import pandas as pd
from pymongo import MongoClient

# Using environment variables for sensitive information
container_name = os.getenv('MONGO_CONTAINER_NAME', 'localhost')
port = 27017
username = os.getenv('MONGO_USERNAME', 'admin')
password = os.getenv('MONGO_PASSWORD', 'adminpassword')
database_name = "local"
collection_name = "employees_db"

# Read data from MongoDB
client = MongoClient(
    host=container_name,
    port=port,
    username=username,
    password=password
)


try:
    db = client[database_name]
    collection = db[collection_name]

    # Retrieve and print data
    documents = list(collection.find())
    df_employees = pd.DataFrame(documents)

finally:
    # Ensuring the client is closed
    client.close()

print(df_employees)