import pandas as pd
import duckdb
import pyodbc
from sqlalchemy import create_engine

# Database connection parameters
# server = 'localhost,1433'  # Replace with your server name
server = 'localhost:1433'  # Replace with your server name
database = 'master'  # Replace with your database name
username = 'sa'  # Replace with your username
password = 'TeST@+$-'  # Replace with your password
table_name = 'MSreplication_options'  # Replace with the table name to import data from

# Create a connection to the SQL Server
# conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
# print(conn_str)
# connection = pyodbc.connect(conn_str)
# Read data from the SQL table
# query = f"SELECT * FROM {table_name}"
# dataframe = pd.read_sql(query, connection)

# Create an SQLAlchemy engine
# connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}"
print(connection_string)
engine = create_engine(connection_string)
print('engine created')
# Read data from the SQL table
query = f"SELECT * FROM {table_name}"
dataframe = pd.read_sql_query(query, engine)

# Verify the data
print(dataframe.head())

# Close the connection
connection.close()

conn = duckdb.connect('testdb.duckdb')
# conn.execute("""ATTACH 'dbname=duckdb_test user=postgres password=postgres host=localhost port=5432'
# AS pg (TYPE POSTGRES, SCHEMA 'public')""")
conn.execute("CREATE TABLE IF NOT EXISTS mssql_data2 AS SELECT * FROM dataframe")
conn.close()