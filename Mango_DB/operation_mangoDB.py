import pandas as pd
from MongoDB_Docker import mongodb_docker

data = pd.read_csv("../Data_Files/employees_info.csv")

mongodb_docker.insert_data(df=data, dbase_name='local', collection_name='employees_db', primary_key='emp_id')

df_employees = mongodb_docker.get_data(dbase_name='local', collection_name='employees_db')
print(df_employees)