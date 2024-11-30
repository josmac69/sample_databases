import pyodbc
import polars as pl
import duckdb

source_table_name = 'dbo.MSreplication_options'
target_table_name = 'pg.mssql_table'

batch_size = 1
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=TeST@+$-"
connection = pyodbc.connect(conn_str)

mssql_cursor = connection.cursor()
query = f"SELECT * FROM {source_table_name}"
mssql_cursor.execute(query)
rows = mssql_cursor.fetchmany(batch_size)

schema = {column[0]: pl.datatypes.DataType.from_python(column[1]) for column in mssql_cursor.description}
df = pl.DataFrame([dict(zip(schema.keys(), row)) for row in rows], schema=schema, orient="row")

duckdb_conn = duckdb.connect(database=':memory:')
duckdb_conn.execute("""
    ATTACH 'dbname=duckdb_test user=postgres password=postgres host=localhost port=5432'
    AS pg (TYPE POSTGRES, SCHEMA 'public')""")
duckdb_conn.execute(f"CREATE TABLE {target_table_name} AS SELECT * FROM df")

while rows:
    rows = mssql_cursor.fetchmany(batch_size)
    if rows:
        df = pl.DataFrame([dict(zip(schema.keys(), row)) for row in rows], schema=schema, orient="row")
        duckdb_conn.execute(f"INSERT INTO {target_table_name} SELECT * FROM df")

mssql_cursor.close()
connection.close()
duckdb_conn.close()
