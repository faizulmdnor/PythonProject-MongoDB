import os
import pandas as pd
from flask import Flask, render_template_string
from pymongo import MongoClient


def avg_by_depart(df):
    """
    Computes a DataFrame containing aggregated salary statistics by department from
    the provided input DataFrame. The output DataFrame includes the average salary,
    total salary, and the number of employees for each department.

    :param df: A pandas DataFrame containing at least the following columns:
               'Department' (department identifier or name), 'salary' (employee salary),
               and 'emp_id' (unique employee identifier).
    :type df: pandas.DataFrame
    :return: A pandas DataFrame with aggregated salary-related statistics by department.
             The output contains the following columns:
             - 'Department': the department names or identifiers.
             - 'Number of Employees': the count of employees in each department.
             - 'Average Salary By Dept': the average salary for each department, rounded to 2 decimals.
             - 'Total Salary': the total salary paid in each department.
    :rtype: pandas.DataFrame
    """
    df_avg_salary_by_dept = round(df.groupby('Department')['salary'].mean().reset_index(), 2)
    df_avg_salary_by_dept.columns = ['Department', 'Average Salary By Dept']
    df_total_salary_by_dept = df.groupby('Department')['salary'].sum().reset_index()
    df_total_salary_by_dept.columns = ['Department', 'Total Salary']
    df_count = df.groupby('Department')['emp_id'].count().reset_index()
    df_count.columns = ['Department', 'Number of Employees']

    df_salary_dept = df_count.merge(df_avg_salary_by_dept, on='Department').merge(df_total_salary_by_dept,
                                                                                  on='Department')
    return df_salary_dept


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

    df_avg_salary_by_dept = avg_by_depart(df_employees)



finally:
    # Ensuring the client is closed
    client.close()

app = Flask(__name__)


@app.route('/')
def display_data():
    """
    Displays employee data and average salary by department in a web browser using Flask.

    This function fetches and converts two DataFrame objects, `df_employees` and
    `df_avg_salary_by_dept`, into their corresponding HTML table representations.
    The tables are then rendered into an HTML template, styled, and returned for
    the web browser to display. The function uses Flask's `render_template_string`
    for inline HTML rendering.

    :rtype: str
    :return: An HTML string containing tables styled and embedded within a
        complete HTML document, ready to be served to the web client.
    """
    # Convert DataFrame to HTML
    html_table1 = df_employees.to_html(index=False)
    html_table2 = df_avg_salary_by_dept.to_html(index=False)

    # Render HTML with Flask
    return render_template_string('''
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <title>Employee Data</title>
        <style>
          table { 
            border-collapse: collapse; 
            width: 100%; 
          }
          th, td { 
            padding: 8px 12px; 
            border: 1px solid #ddd; 
          }
          th { 
            background-color: #f2f2f2; 
          }
          .text-left { 
            text-align: left; 
          }
          .number-right { 
            text-align: right; 
          }
          .float-left{
            float: left
          }
        </style>
      </head>
      <body>
        <h1>Average Salary by Department</h1>
        {{ table2 | safe }}

        <h1>Employee Data</h1>
        {{ table1 | safe }}

      </body>
    </html>
    ''', table1=html_table1, table2=html_table2)


if __name__ == '__main__':
    app.run(debug=True)
