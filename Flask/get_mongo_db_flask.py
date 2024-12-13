import os

import pandas as pd
from flask import Flask, render_template_string
from pymongo import MongoClient

def avg_by_depart(df):
    """
    Calculates and aggregates various salary-related statistics by department.

    This function processes a dataframe containing employee information and computes
    the following statistics:
    - Average salary per department
    - Total salary paid per department
    - Number of employees per department
    - Number of employees earning below the overall average salary, grouped by department
    - Number of employees earning above the overall average salary, grouped by department

    The resulting dataframe contains consolidated information for each department.

    :param df: Input dataframe containing at least the columns 'Department', 'salary',
        and 'emp_id'. Assumed to have numeric 'salary' values and unique 'emp_id' values
        for valid processing.
    :type df: pandas.DataFrame
    :return: A dataframe containing aggregated salary statistics by department, including
        number of employees, below average salary counts, above average salary counts,
        average salary by department, and total salary by department.
    :rtype: pandas.DataFrame
    """

    df_avg_salary_by_dept = round(df.groupby('Department')['salary'].mean().reset_index(), 2)
    df_avg_salary_by_dept.columns = ['Department', 'Average Salary By Dept']
    df_total_salary_by_dept = df.groupby('Department')['salary'].sum().reset_index()
    df_total_salary_by_dept.columns = ['Department', 'Total Salary']
    df_count = df.groupby('Department')['emp_id'].count().reset_index()
    df_count.columns = ['Department', 'Number of Employees']

    # count number of employees below and above average salary by departments.
    # below average
    df_below = df[df['salary'] < df['salary'].mean()].groupby('Department')['emp_id'].count().reset_index()
    df_below.columns = ['Department', 'Below Average Salary']

    # above average
    df_above = df[df['salary'] > df['salary'].mean()].groupby('Department')['emp_id'].count().reset_index()
    df_above.columns = ['Department', 'Above Average Salary']

    df_salary_dept = df_count.merge(df_above, on='Department').merge(df_below, on='Department').merge(
        df_avg_salary_by_dept, on='Department').merge(df_total_salary_by_dept, on='Department')

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
    Handles the root route ('/') by displaying employee data and average salary by
    department in a styled HTML table format. Converts pandas DataFrames into HTML
    tables and uses Flask's `render_template_string` method to embed these tables
    into an HTML page for rendering.

    :return: The rendered HTML page displaying employee data and the average salary
        by department.
    :rtype: str
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
