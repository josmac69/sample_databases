import pandas as pd
import duckdb
# import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import sqlalchemy as sa
from urllib.parse import quote_plus

# Database connection parameters
# server = 'localhost,1433'  # Replace with your server name
# server = 'localhost:1433'  # Replace with your server name
# database = 'master'  # Replace with your database name
# username = 'sa'  # Replace with your username
# password = 'TeST@+$-'  # Replace with your password
# table_name = 'dbo.MSreplication_options'  # Replace with the table name to import data from

# Create a connection to the SQL Server
# conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
# print(conn_str)
# connection = pyodbc.connect(conn_str)
# Read data from the SQL table
# query = f"SELECT * FROM {table_name}"
# dataframe = pd.read_sql(query, connection)

# Create an SQLAlchemy engine
# check with: odbcinst -q -d
# connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
# connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server+Native+Client+11.0"
# connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}"
# print(connection_string)
# engine = create_engine(connection_string)

connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=TeST@+$-;"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine_string = "mssql+pyodbc:///?odbc_connect={}".format(quote_plus(connection_string))
print(connection_string)
print(connection_url)
print(engine_string)
# engine = create_engine(connection_url)
engine = sa.create_engine(engine_string, pool_pre_ping=True, poolclass=None)
print('engine created')
# Read data from the SQL table
query = "SELECT * FROM dbo.MSreplication_options"
try:
    with engine.begin() as conn:
        dataframe = pd.read_sql(sa.text(query), conn)
except Exception as error:
    print(f"Error: {error}")
    exit(1)

# Verify the data
print(dataframe.head())

# Close the connection
# connection.close()

conn = duckdb.connect('testdb.duckdb')
# conn.execute("""ATTACH 'dbname=duckdb_test user=postgres password=postgres host=localhost port=5432'
# AS pg (TYPE POSTGRES, SCHEMA 'public')""")
conn.execute("CREATE TABLE IF NOT EXISTS mssql_data2 AS SELECT * FROM dataframe")
conn.close()