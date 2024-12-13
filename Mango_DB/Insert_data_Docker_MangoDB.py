from pymongo import MongoClient
import pandas as pd
import os

# Using environment variables for sensitive information
container_name = os.getenv('MONGO_CONTAINER_NAME', 'localhost')
port = 27017
username = os.getenv('MONGO_USERNAME', 'admin')
password = os.getenv('MONGO_PASSWORD', 'adminpassword')
database_name = "local"
collection_name = "employees_db"

# Read CSV data
try:
    data = pd.read_csv("../Data_Files/employees_info.csv")
except FileNotFoundError as fnf_error:
    print(f"CSV file not found: {fnf_error}")
    data = pd.DataFrame()  # proceed with an empty DataFrame if file is not found
except Exception as e:
    print(f"Error reading CSV file: {e}")
    data = pd.DataFrame()  # proceed with an empty DataFrame

try:
    client = MongoClient(
        host=container_name,
        port=port,
        username=username,
        password=password
    )

    db = client[database_name]
    collection = db[collection_name]

    for i, row in data.iterrows():
        document = row.to_dict()
        try:
            result = collection.insert_one(document)
            print(f"Inserted document id: {result.inserted_id}")
        except Exception as insert_error:
            print(f"Failed to insert document: {insert_error}")

except Exception as e:
    print(f"An error occurred with the MongoDB connection or operation: {e}")

finally:
    client.close()
