import apache_beam as beam
import apache_beam.io.jdbc as jdbc
import apache_beam.io as io

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive.display import pipeline_graph
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import json
import csv
import os
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from collections import namedtuple


def read_column_mapping(mapping_path):
    with open(mapping_path, 'r') as f:
        mapping = json.load(f)
    return mapping


def read_table_names(mapping_path):
    # Expecting the mapping JSON to have 'history_table' key for SCD2
    with open(mapping_path, 'r') as f:
        mapping = json.load(f)
    main_table = mapping.get('target_table')
    history_table = mapping.get('history_table')
    return main_table, history_table


def transform_row(row, mapping):
    # Map CSV columns to target columns
    return {mapping[csv_col]: value for csv_col, value in row.items() if csv_col in mapping}


def get_jdbc_write_config(table_name, db_url, db_user, db_pass):
    return {
        'driverClassName': 'org.postgresql.Driver',
        'jdbcUrl': db_url,
        'username': db_user,
        'password': db_pass,
        'statement': f'INSERT INTO {table_name} (%s) VALUES (%s)'
    }


def parse_database_url(database_url):
    parsed = urlparse(database_url)
    db_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    db_user = parsed.username
    db_pass = parsed.password
    return db_url, db_user, db_pass

def parse_csv_line(line, column_names):
    reader = csv.DictReader([line], fieldnames=column_names)
    return next(reader)

def dict_to_namedtuple(name, d):
    NT = namedtuple(name, d.keys())
    return NT(**d)

def run_pipeline(csv_path, mapping_path, db_url, db_user, db_pass):
    mapping = read_column_mapping(mapping_path)
    main_table = mapping['target_table']
    history_table = mapping['target_history_table']
    column_map = mapping['column_mapping']
    columns = list(column_map.values())

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        # Read CSV as dicts
        rows = (
            p
            | 'Read CSV' >> beam.io.ReadFromText(csv_path, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(lambda line: parse_csv_line(line, list(column_map.keys())))
            | 'Map Columns' >> beam.Map(lambda row: {column_map[k]: v for k, v in row.items() if k in column_map})

            # | 'print Rows' >> beam.Map(lambda x: print("Row dict:",x))  # Debugging step to see rows
            | 'To NamedTuple' >> beam.Map(lambda d: dict_to_namedtuple('Row', d))
            | 'print Rows' >> beam.Map(lambda x: print("Row dict:",x))  # Debugging step to see rows
        )
        # Write to main table
        # _ = (
        #     rows
        #     | 'Write Main Table' >> jdbc.WriteToJdbc(
        #         table_name=f"public.{main_table}",
        #         driver_class_name='org.postgresql.Driver',
        #         jdbc_url=db_url,
        #         username=db_user,
        #         password=db_pass,
        #         statement=f"INSERT INTO public.{main_table} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])})"
        #     )
        # )
        # # Read current history table for SCD2
        # history_rows = (
        #     p
        #     | 'Read History Table' >> jdbc.ReadFromJdbc(
        #         table_name=history_table,
        #         driver_class_name='org.postgresql.Driver',
        #         jdbc_url=db_url,
        #         username=db_user,
        #         password=db_pass,
        #         query=f"SELECT * FROM {history_table} WHERE is_current = TRUE"
        #     )
        # )
        # # Find changed rows (simple example: compare all columns)

        # def is_changed(new_row, old_rows):
        #     for old in old_rows:
        #         if all(new_row[k] == old.get(k) for k in columns):
        #             return False
        #     return True
        # changed_rows = (
        #     rows
        #     | 'Pair with history' >> beam.FlatMap(lambda row, history: [(row, history)], beam.pvalue.AsList(history_rows))
        #     | 'Filter Changed' >> beam.Filter(lambda pair: is_changed(pair[0], pair[1]))
        #     | 'Extract Changed' >> beam.Map(lambda pair: pair[0])
        # )
        # # Mark old versions as not current and set end_date

        # def mark_old_versions(row, old_rows):
        #     now = datetime.utcnow().isoformat()
        #     updates = []
        #     for old in old_rows:
        #         if any(row[k] != old.get(k) for k in columns):
        #             update = dict(old)
        #             update['end_date'] = now
        #             update['is_current'] = False
        #             updates.append(update)
        #     return updates
        # _ = (
        #     rows
        #     | 'Pair for Update' >> beam.FlatMap(lambda row, history: [(row, history)], beam.pvalue.AsList(history_rows))
        #     | 'Mark Old Versions' >> beam.FlatMap(lambda pair: mark_old_versions(pair[0], pair[1]))
        #     | 'Update Old Versions' >> jdbc.WriteToJdbc(
        #         table_name=history_table,
        #         driver_class_name='org.postgresql.Driver',
        #         jdbc_url=db_url,
        #         username=db_user,
        #         password=db_pass,
        #         statement=f"UPDATE {history_table} SET end_date = ?, is_current = ? WHERE surrogate_key = ?"
        #     )
        # )
        # # Write changed rows to history table (with SCD2 fields)

        # def add_scd2_fields(row):
        #     now = datetime.utcnow().isoformat()
        #     row['start_date'] = now
        #     row['end_date'] = None
        #     row['is_current'] = True
        #     return row
        # _ = (
        #     changed_rows
        #     | 'Add SCD2 Fields' >> beam.Map(add_scd2_fields)
        #     | 'Write History Table' >> jdbc.WriteToJdbc(
        #         table_name=history_table,
        #         driver_class_name='org.postgresql.Driver',
        #         jdbc_url=db_url,
        #         username=db_user,
        #         password=db_pass,
        #         statement=f"INSERT INTO {history_table} ({', '.join(columns + ['start_date', 'end_date', 'is_current'])}) VALUES ({', '.join(['?' for _ in columns + ['start_date', 'end_date', 'is_current']])})"
        #     )
        # )


def run_pipeline_from_json(mapping_path):
    print(mapping_path)
    # Load .env
    load_dotenv()
    # Read mapping JSON
    with open(mapping_path, 'r') as f:
        mapping = json.load(f)
    csv_path = mapping['source_csv']
    main_table = mapping['target_table']
    history_table = mapping['target_history_table']
    column_map = mapping['column_mapping']
    columns = list(column_map.values())
    # Parse DB connection from .env
    database_url = os.getenv('DATABASE_URL')
    db_url, db_user, db_pass = parse_database_url(database_url)
    # Call the main pipeline
    run_pipeline(csv_path, mapping_path, db_url, db_user, db_pass)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Beam pipeline for CSV to Postgres with SCD2')
    parser.add_argument('--mapping', required=True,
                        help='Path to column mapping JSON')
    parser.add_argument('--from-json', action='store_true',
                        help='Extract all config from JSON and .env')
    parser.add_argument(
        '--csv', help='Path to CSV file (if not using --from-json)')
    parser.add_argument(
        '--db_url', help='JDBC URL for Postgres (if not using --from-json)')
    parser.add_argument(
        '--db_user', help='DB username (if not using --from-json)')
    parser.add_argument(
        '--db_pass', help='DB password (if not using --from-json)')
    args = parser.parse_args()
    # if args.from_json:
    print("from json")
    run_pipeline_from_json(args.mapping)
    # else:
    #     run_pipeline(args.csv, args.mapping, args.db_url, args.db_user, args.db_pass)
