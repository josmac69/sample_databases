"""
Script to download Github archive data and load them into PostgreSQL
"""
import argparse
import gzip
import os
import json
import random
from datetime import datetime, timedelta
import sys
import requests
import psycopg2
import yaml

def read_yaml(filename):
    """
    Parses YAML file
    """
    with open(filename, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(1)

def open_connection(connection):
    """
    Opens PostgreSQL connection
    """
    try:
        conn = psycopg2.connect(
            dbname=connection['dbname'],
            user=connection['user'],
            password=connection['password'],
            host=connection['host'],
            port=connection['port']
        )
        return conn
    except Exception as error:
        print(f"Error: {error}")
        sys.exit(1)

def parse_input():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Download Github archive data and load them into PostgreSQL')

    parser.add_argument(
        '-s',
        '--start',
        required=True,
        help='Start datetime in format YYYY-MM-DD-HH')

    parser.add_argument(
        '-e',
        '--end',
        required=True,
        help='End datetime in format YYYY-MM-DD-HH')

    parser.add_argument(
        '-r',
        '--runtime_file',
        required=True,
        help='File name for storing runtimes of each loop')

    parser.add_argument(
        '-rr',
        '--rewrite_runtime_file',
        action='store_true',
        help='If set script will rewrite runtime file')

    parser.add_argument(
        '-t',
        '--table_name',
        required=False,
        default="public.github_events_2023",
        help='Target table for data')

    parser.add_argument(
        '-c',
        '--connection',
        required=True,
        help='YAML file with connection credentials, if missing prompts are shown')

    parser.add_argument(
        '-gis',
        '--gin_inspection_script',
        required=False,
        help='SQL file with inspection script for the GIN index')

    parser.add_argument(
        '--gin_inspection_after_insert',
        action='store_true',
        help='If set script will run GIN inspection script after each insert')

    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Verbose debug output')

    parser.add_argument(
        '-tt',
        '--truncate_table',
        action='store_true',
        help='If set truncate table before inserting data')

    parser.add_argument(
        '-dt',
        '--drop_table',
        action='store_true',
        help='If set truncate table before inserting data')

    parser.add_argument(
        '-rd',
        '--random_drop',
        action='store_true',
        help='If set script drops randomly from 1 to 3 keys in each row')

    args = parser.parse_args()

    return args

def drop_random_keys(event):
    """
    Drops random keys from event
    """
    keys = list(event.keys())
    random.shuffle(keys)
    num_keys_to_drop = random.randint(1, 3)
    keys_to_drop = random.sample(keys, num_keys_to_drop)
    for key in keys_to_drop:
        del event[key]
    return event


def inspect_gin_index(conn, cur, table_name, args, insert_commit_runtime):
    """
    Inspects GIN index
    """
    print(f"  {datetime.now()}: inspect_gin_index: {table_name}") if args.debug else None
    if args.gin_inspection_script:
        # find name of gin index related to the table using select from database
        query = ("SELECT quote_ident(relnamespace::regnamespace::text)||'.'||quote_ident(relname) "
            "FROM pg_class where relnamespace::regnamespace::text||'.'||relname IN ("
            f"SELECT schemaname||'.'||indexname from pg_indexes WHERE schemaname||'.'||tablename = '{table_name}' "
            "AND indexdef ilike '% using gin %')")
        print(f"  {datetime.now()}: query: {query}") if args.debug else None

        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            gin_index_name = row[0]
            print(f"  {datetime.now()}: gin_index_name: {gin_index_name}") if args.debug else None

            cur.execute(f"SET inspect_gin_index.table_name='{table_name}'")
            cur.execute(f"SET inspect_gin_index.index_name='{gin_index_name}'")
            cur.execute("SET inspect_gin_index.result_target='table'")
            cur.execute(f"SET inspect_gin_index.insert_commit_runtime='{insert_commit_runtime}'")

            print(f"  {datetime.now()}: inspect_gin_index_script: {args.gin_inspection_script}") if args.debug else None
            with open(args.gin_inspection_script, 'r') as file:
                query = file.read()
                cur.execute(query)
                conn.commit()


# Function to download, process, and delete files
def download_process_file(conn, cur, start_date, args):
    """
    Downloads, processes, and deletes files
    """
    date_str = start_date.strftime("%Y-%m-%d-%H")
    url = f"https://data.gharchive.org/{date_str}.json.gz"
    print(f"* {datetime.now()}: Processing {url}")

    # calculate unix timestamp for the date
    unix_timestamp = datetime.timestamp(start_date)
    print(f"unix_timestamp: {unix_timestamp}") if args.debug else None

    # check type of the table
    if '.' in args.table_name:
        schema_name, table_name = args.table_name.split('.')
        query_type = (f"SELECT relkind FROM pg_class WHERE relnamespace::regnamespace::text = '{schema_name}' AND relname = '{table_name}';")
    else:
        query_type = (f"SELECT relkind FROM pg_class WHERE relname = '{args.table_name}';")

    print(f"  {datetime.now()}: query_type: {query_type}")
    cur.execute(query_type)
    relkind = cur.fetchone()[0]
    print(f"  {datetime.now()}: table type: {relkind}")

    partition_date = ""
    # if relkind == 'r' then partition_date is empty
    if relkind == 'p':
        partition_date = F"_{date_str.replace('-', '')[:8]}"
        print(f"  {datetime.now()}: partition_date: {partition_date}")

    row = 0
    errors = 0
    table_size = 0
    relation_size = 0
    indexes_size = 0
    loop_start = datetime.now()

    try:
        loop_start = datetime.now()
        local_filename = "/tmp/" + url.split('/')[-1]
        # Download the file - randomize the file name to avoid conflicts
        local_filename = local_filename + '.' + str(loop_start.strftime("%Y-%m-%d-%H-%M-%S-%f"))
        print(f"  {loop_start}: downloading {local_filename} ")

        with requests.get(url, stream=True, timeout=300) as req:
            req.raise_for_status()
            with open(local_filename, 'wb') as file:
                for chunk in req.iter_content(chunk_size=8192):
                    file.write(chunk)

        print(f"  {datetime.now()}: downloaded")
        file_stats = os.stat(local_filename)
        print(f"  {datetime.now()}: file size: {file_stats.st_size}")

        # Uncompress and process the file
        with gzip.open(local_filename, 'rb') as file:
            # start time of the loop
            loop_start = datetime.now()
            print(f"  {loop_start}: processing {local_filename}, table {args.table_name}")
            for line in file:
                event = json.loads(line)
                if args.random_drop:
                    event = drop_random_keys(event)

                event_str = json.dumps(event).replace(r'\u0000', '').replace('`', "'")
                row += 1

                # print number of rows processed every 25000 rows
                if row % 25000 == 0:
                    print(f"  {datetime.now()}: processed {row} rows")

                # Process and insert the data into PostgreSQL here
                try:
                    conn.commit()
                    insert_start = datetime.now()
                    query = f"INSERT INTO {args.table_name} (jsonb_data) VALUES (%s)"
                    cur.execute(query, (event_str, ))
                    conn.commit()
                    insert_commit_runtime = datetime.now() - insert_start

                    if args.gin_inspection_after_insert:
                        print(f"GIN inspection: file {date_str} after {row} rows inserted")
                        inspect_gin_index(conn, cur, f'{args.table_name}{partition_date}', args, insert_commit_runtime)

                except Exception as error:
                    print(f" {datetime.now()}: Skipping row: {row}, Error: {error}")
                    errors += 1

        print(f"  Inserted into {args.table_name}: {row} rows, errors: {errors}")
        conn.commit()

        # Delete the file
        os.remove(local_filename)
        loop_end = datetime.now()
        print(f"  {loop_end}: processed in {loop_end - loop_start}")
    except requests.exceptions.HTTPError:
        print(f"  file for {date_str} not found")
    except Exception as error:
        print(f"Row: {row}, Error: {error}")
        sys.exit(1)

    # end time of the loop
    loop_end = datetime.now()
    runtime = loop_end - loop_start
    total_run_time_seconds = round(runtime.total_seconds(),3)
    rows_per_second = round(row / total_run_time_seconds,3)

    # inspect GIN index
    inspect_gin_index(conn, cur, f'{args.table_name}{partition_date}', args, runtime)

    query_sizes = (f"SELECT pg_relation_size('{args.table_name}{partition_date}'), "
                  f"pg_table_size('{args.table_name}{partition_date}'), "
                  f"pg_indexes_size('{args.table_name}{partition_date}');")

    print(f"  {datetime.now()}: query_sizes: {query_sizes}")
    cur.execute(query_sizes)

    sizes = cur.fetchone()
    relation_size = sizes[0]
    table_size = sizes[1]
    indexes_size = sizes[2]
    print(f"  {datetime.now()}: table size: {table_size}")

    # open new csv file for writing runtimes of each loop
    with open(args.runtime_file, 'a') as csv_file:
        csv_file.write(f'{date_str},{unix_timestamp},{loop_start},'
                       f'{loop_end},{runtime},{total_run_time_seconds},'
                       f'{relation_size},{table_size},{indexes_size},'
                       f'{row},{rows_per_second},{errors}\n')


def main():
    """
    Main function
    """
    print(f"Start: {datetime.now()}")
    delta = timedelta(hours=1)

    # Parse command line arguments
    args = parse_input()

    if args.gin_inspection_after_insert and args.gin_inspection_script is None:
        print("ERROR: You requested GIN index inspection after each insert but GIN inspection script is not set!")
        sys.exit(1)

    start_date = datetime.strptime(args.start, "%Y-%m-%d-%H")
    end_date = datetime.strptime(args.end, "%Y-%m-%d-%H")

    print(f"connection file: {args.connection}") if args.debug else None

    # Parse connection file
    connection = read_yaml(args.connection)
    print(f"connection: {connection}") if args.debug else None

    # Open PostgreSQL connection
    print("Opening connection to PostgreSQL") if args.debug else None
    conn = open_connection(connection)

    # Create cursor
    print("Creating cursor") if args.debug else None
    cur = conn.cursor()

    # Drop table if requested
    if args.drop_table:
        print(f"Dropping table: {args.table_name}")
        cur.execute(f"DROP TABLE IF EXISTS {args.table_name};")
        conn.commit()

    # Check if table exists
    if '.' in args.table_name:
        schema_name, table_name = args.table_name.split('.')
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)", (schema_name, table_name))
    else:
        cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (args.table_name,))
    table_exists = cur.fetchone()[0]

    if not table_exists:
        # Create table
        create_table_query = f"CREATE TABLE {args.table_name} (id SERIAL PRIMARY KEY, jsonb_data JSONB compression lz4, data_source VARCHAR)"
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table {args.table_name} created.")

    # Truncate table if requested
    if args.truncate_table:
        print(f"Truncating table: {args.table_name}")
        cur.execute(f"TRUNCATE TABLE {args.table_name};")

    print(f"Date range {start_date} - {end_date}")
    print(f"Table: {args.table_name}")
    # Loop over the timeframe

    # open new csv file for writing runtimes of each loop
    print(f'Runtimes file: {args.runtime_file}') if args.debug else None

    # if rewrite_runtime_file is set, delete the file if exists
    if args.rewrite_runtime_file:
        os.remove(args.runtime_file) if os.path.exists(args.runtime_file) else None

    if not os.path.exists(args.runtime_file):
        with open(args.runtime_file, 'w') as csv_file:
            csv_file.write(
                'file_name,unix_timestamp,loop_start,loop_end,runtime,total_run_time_seconds,relation_size,table_size,index_size,rows_inserted,rows_per_second,errors\n')

    while start_date <= end_date:
        # Download, process, and delete the file
        download_process_file(conn, cur, start_date, args)
        start_date += delta

    # Commit and close PostgreSQL connection
    conn.commit()
    cur.close()
    conn.close()
    print(f"End: {datetime.now()}")


# Date range for downloading files

if __name__ == "__main__":
    main()
