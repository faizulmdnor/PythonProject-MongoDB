import pandas as pd
from pymongo import MongoClient
import os

def mongoDB_connect():
    """
    Establishes a connection to a MongoDB database using the provided environment
    variables or their default values. This connection enables interaction with
    the MongoDB server through the `pymongo.MongoClient`.

    :raises pymongo.errors.ConnectionError: If the client fails to connect to the
        MongoDB server using the provided configuration.

    :return: A `MongoClient` instance connected to the specified MongoDB server.
    :rtype: pymongo.MongoClient
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
    A utility class to interact with MongoDB in a Docker environment.

    This class provides static methods to connect to a MongoDB instance, retrieve data
    from a specified collection, and insert data into the collection. It is designed
    to work specifically in environments where MongoDB is deployed in a Docker container.

    :ivar client: The MongoDB client connection instance.
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

