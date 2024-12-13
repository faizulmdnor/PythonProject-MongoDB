import pandas as pd
from pymongo import MongoClient
import os

def mongoDB_connect():
    """
    Establishes a connection to a MongoDB database using environment variables
    to configure the connection parameters. This function connects to a MongoDB
    instance that can be either local or containerized, depending on the setup.
    The connection details such as container name, username, and password are
    retrieved from environment variables with default values provided.

    :return: A MongoClient instance connected to the specified MongoDB server.
    :rtype: MongoClient
    """
    container_name = os.getenv('MONGO_CONTAINER_NAME', 'localhost')
    username = os.getenv('MONGO_USERNAME', 'admin')
    password = os.getenv('MONGO_PASSWORD', 'adminpassword')

    client = MongoClient(
        host=container_name,
        port=27017,
        username=username,
        password=password
    )

    return client

class mongodb_docker:
    """
    This class provides static methods for interacting with a MongoDB database hosted
    in a Docker container. It allows for retrieving data from a specified collection
    and inserting new data into a collection, with optional handling for a primary key
    to avoid duplicate entries.

    :ivar client: The MongoDB client instance used to connect to the database.
    :type client: pymongo.MongoClient
    """
    @staticmethod
    def get_data(dbase_name, collection_name):
        try:
            client = mongoDB_connect()
        except Exception as error_client:
            print(f"Error occurred: {error_client}")

        try:
            db = client[dbase_name]
            collection = db[collection_name]

            documents = list(collection.find())
            df_data = pd.DataFrame(documents)

        except Exception as error_find:
            print(f"Error retrieving documents: {error_find}")

        finally:
            client.close()

        return df_data

    @staticmethod
    def insert_data(df, dbase_name, collection_name, primary_key):
        client = mongoDB_connect()
        db = client[dbase_name]
        collection = db[collection_name]
        try:
            for i, row in df.iterrows():
                document = row.to_dict()

                query = {primary_key: document[primary_key]}

                if collection.count_documents(query) == 0:
                    try:
                        collection.insert_one(document)
                        print(f"{primary_key}: {document[primary_key]} - Successfully inserted in to {dbase_name}.{collection_name}")
                    except Exception as insert_error:
                        print(f"Failed to insert document: {insert_error}")
                else:
                    print(f"{primary_key}: {document[primary_key]} already exist")
        except Exception as e:
            print(f"Error occurred with the MongoDB connection or operation: {e}")

        finally:
            client.close()

