from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
import os

# Environment variables
host = os.getenv("MONGO_CONTAINER_NAME", "localhost")
port = int(os.getenv("MONGO_PORT", 27017))
username = os.getenv("MONGO_USERNAME", "admin")
password = os.getenv("MONGO_PASSWORD", "adminpassword")
database_name = "local"
collection_name = "employees_db"

app = Flask(__name__)

@app.route("/")
def display_data():
    try:
        # MongoDB Connection
        with MongoClient(host, port, username=username, password=password) as client:
            db = client[database_name]
            collection = db[collection_name]
            
            # Fetch data
            documents = list(collection.find())
            df_employees = pd.DataFrame(documents)
            
            # Handle empty data
            if df_employees.empty:
                html_table = "<p>No data found in the collection.</p>"
            else:
                html_table = df_employees.to_html(index=False)
    except Exception as e:
        html_table = f"<p>Error: {e}</p>"
    
    # Render HTML
    return render_template("index.html", table=html_table)

if __name__ == '__main__':
    app.run(debug=True)
