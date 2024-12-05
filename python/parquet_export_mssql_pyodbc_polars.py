import pyodbc
import polars as pl
import duckdb

table_name = 'dbo.MSreplication_options'

conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=TeST@+$-"
connection = pyodbc.connect(conn_str)

query = f"SELECT * FROM {table_name}"
df = pl.read_database(query, connection)
connection.close()

duckdb_conn = duckdb.connect(database=':memory:')
duckdb_conn.execute("COPY (SELECT * FROM df) TO '../data/mssql_table.parquet' (FORMAT PARQUET)")
duckdb_conn.close()
