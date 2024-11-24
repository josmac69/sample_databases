"""
Script analyzes JSON data and inserts it into a PostgreSQL database.
It supports fetching JSON data from either a file or an API,
validating the data, and inserting it into the database.
It also provides an option to analyze the JSON structure and
suggest the optimal way to convert the data into rows.
"""
import argparse
import json
import requests
# import traceback
import psycopg2
from psycopg2 import extras
import yaml

def parse_arguments():
    """
    Function to parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Process JSON data and insert into PostgreSQL.')
    parser.add_argument('--source', required=True, help='File path or API URL')
    parser.add_argument('--data_type', required=True, choices=['file', 'api'], help='Type of source: file or api')
    parser.add_argument('--data_source_name', required=True, help='Description of the data source for the table')
    parser.add_argument('--analyze_only', action='store_true', help='Just analyze the JSON structure and find patterns')
    parser.add_argument('--structure_path', required=False, help='JSON path to the structure containing rows')
    parser.add_argument('--debug', action='store_true', help='Print debug messages')
    parser.add_argument('--table_name', required=False, help='Name of the PostgreSQL table (ev. schema.table) to insert data into')
    parser.add_argument('--config_file', required=False, help='Path to the config YAML file')
    return parser.parse_args()

def parse_config_file(config_file):
    """
    Function to parse the YAML config file and extract PostgreSQL credentials.
    :param config_file: Path to the config YAML file.
    :return: Dictionary containing PostgreSQL credentials.
    """
    with open(config_file, 'r') as file:
        config_data = yaml.safe_load(file)
    return config_data

def analyze_json_structure(json_data, path=""):
    """
    Recursively analyze the JSON structure to identify patterns.
    :param json_data: The JSON data to analyze.
    :param path: The current path in the JSON structure.

    The function checks if the json_data is a dictionary or a list.
    If it's a dictionary, it iterates over its key-value pairs and recursively calls itself with the value and an updated path.
    If it's a list and the list is not empty, it checks if the first element is a dictionary.
    If it is, it prints a message indicating a potential row structure at the current path.
    If the json_data is neither a dictionary nor a list, it is considered a terminal value (e.g., string, number) and the function does nothing.
    """
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            print(f"Analyzing: {path}/{key}")
            analyze_json_structure(value, f"{path}/{key}")
    elif isinstance(json_data, list):
        if json_data and isinstance(json_data[0], dict):
            print(f"Found a potential row structure at path: {path} with keys: {json_data[0].keys()}")
    else:
        # This is a terminal value (e.g., string, number, etc.)
        pass

def suggest_structure(json_data):
    """
    The suggest_structure() function is a helper function that calls analyze_json_structure()
    to analyze the JSON structure and prints a message indicating that the analysis is complete.
    This function is called when the --analyze_only command-line argument is provided.
    """
    print("Analyzing JSON structure...")
    analyze_json_structure(json_data)
    print("Analysis complete. Review the suggestions to determine the optimal way to convert the data into rows.")

def load_json(source):
    """
    The load_json() function is responsible for loading JSON data from a file.
    It takes the file path as a parameter and uses the json module to read and
    parse the JSON data from the file. The function returns the parsed JSON data.
    """
    with open(source, 'r') as file:
        return json.load(file)

def fetch_json(source, data_type):
    """
    The fetch_json() function is used to fetch JSON data either from a file or an API.
    It takes two parameters:
        source (the file path or API URL)
        data_type (the type of source: file or API).
    If the data_type is "file", it calls the load_json()
        function to load the JSON data from the file.
    If the data_type is "api", it makes an HTTP GET request
        to the source URL using the requests module and returns the JSON response.
    """
    if data_type == 'file':
        return load_json(source)
    elif data_type == 'api':
        response = requests.get(source)
        return response.json()

def validate_json(json_data):
    """
    The validate_json() function is responsible for validating the JSON data.
    It takes the JSON data as a parameter and iterates over each row in the data.
    It tries to convert each row to JSON by serializing and deserializing it using json.dumps() and json.loads().
    If the conversion is successful, the row is considered valid and added to the valid_rows list.
    If a json.JSONDecodeError occurs during the conversion, the row is skipped.
    The function returns the list of valid rows.
    """
    valid_rows = []
    for row in json_data:
        try:
            valid_rows.append(json.loads(json.dumps(row)))
        except json.JSONDecodeError:
            continue
    return valid_rows

def insert_json(json_data, args):
    """
    The insert_json() function is used to insert the JSON data into the PostgreSQL database.
    It takes several parameters:
        json_data (the JSON data to insert),
        data_source (the description of the data source for the table),
        structure_path (the JSON path to the structure containing rows),
        debug (a flag indicating whether to print debug messages).
    """
    if not args.config_file:
        print("Error: No config file provided.")
        return

    if not args.table_name:
        print("Error: No table name provided.")
        return

    config_data = parse_config_file(args.config_file)
    db_credentials = {
        'database': config_data['dbname'],
        'user': config_data['user'],
        'password': config_data['password'],
        'host': config_data['host'],
        'port': config_data['port']
    }

    print("Opening connection to database...") if args.debug else None
    conn = psycopg2.connect(**db_credentials)
    cur = conn.cursor()

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

    if args.structure_path:
        print(f"Using structure path: {args.structure_path}") if args.debug else None
        structure_path_parts = args.structure_path.split("/")
        print(f"Structure path parts: {structure_path_parts}") if args.debug else None
        for part in structure_path_parts:
            print(f"Getting part: {part}") if args.debug else None
            json_data = json_data[part]

    print("Validating JSON data...") if args.debug else None
    valid_rows = validate_json(json_data)
    print(f"Inserting {len(valid_rows)} rows...") if args.debug else None
    inserted = 0
    errors = 0
    for row in valid_rows:
        try:
            row_str = json.dumps(row).replace(r'\u0000', '')
            # print(f"Inserting row str: {row_str}") if args.debug else None
            cur.execute(f"INSERT INTO {args.table_name} (jsonb_data, data_source) VALUES (%s, %s)", (row_str, args.data_source_name))
            conn.commit()
            inserted += 1
        except Exception as e:
            print(f"Error inserting data: {args.data_source_name}, table: {args.table_name}, row: {row}. Error: {str(e)}") if args.debug else None
            # traceback.print_exc()
            conn.rollback()
            errors += 1

    conn.close()
    print(f"Insert done: inserted: {inserted} / errors: {errors}")

def main():
    args = parse_arguments()

    print("****** import of JSON data into PostgreSQL ******")
    print(f"Source: {args.source}")
    print(f"Data type: {args.data_type}")
    print(f"Data source name: {args.data_source_name}")
    print(f"Analyze only: {args.analyze_only}")
    print(f"Structure path: {args.structure_path}")
    print(f"Debug: {args.debug}")
    print(f"Table name: {args.table_name}")

    print("Fetching JSON data...") if args.debug else None
    json_data = fetch_json(args.source, args.data_type)
    if args.analyze_only:
        suggest_structure(json_data)
    else:
        print("Inserting JSON data into PostgreSQL...") if args.debug else None
        insert_json(json_data, args)
    print("Done.")

if __name__ == "__main__":
    main()
