import pandas as pd
import duckdb
from dbfread import DBF

table = DBF('dbase_sample_data.dbf', load=True)
dataframe = pd.DataFrame(iter(table))

conn = duckdb.connect('testdb.duckdb')
conn.execute("""ATTACH 'dbname=duckdb_test user=postgres password=postgres host=localhost port=5432'
AS pg (TYPE POSTGRES, SCHEMA 'public')""")
conn.execute("CREATE TABLE IF NOT EXISTS pg.dbase_sample_data AS SELECT * FROM dataframe")
conn.close()
