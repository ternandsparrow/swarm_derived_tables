import psycopg2 as pg
import os
from dotenv import load_dotenv
import pandas as pd
import traceback

dir_name = os.path.dirname(os.path.abspath(__file__))
load_dotenv(f'{dir_name}/dev.env')

credentials = {
    'user': os.getenv('DBUSER'),
    'password': os.getenv('PASSWORD'),
    'host': os.getenv('HOST'),
    'port': int(os.getenv('PORT', 5432)),
    'database': os.getenv('DATABASE'),
}

db_conn = pg.connect(**credentials)
db_conn.autocommit = False

cursor = db_conn.cursor()


def make_tables():
    try:
        create_table_query_folder = f'{dir_name}/create_table_query'
        for filename in os.listdir(create_table_query_folder):
            with open(f'{create_table_query_folder}/{filename}') as f:
                query = f.read()
                cursor.execute(query)
            table_name = filename.split('.')[0].replace('create_', '')
            ingest_data_from_csv(table_name)
        db_conn.commit()
        print('Created all tables, and ingested data')

    except Exception:
        print(traceback.format_exc())
        db_conn.rollback()


def ingest_data_from_csv(table_name):
    csv_path = f'{dir_name}/data/{table_name}.csv'
    df = pd.read_csv(csv_path, keep_default_na=False)
    # drop duplicates
    df = df.drop_duplicates()
    rows = df.to_dict(orient='records')

    # FIXME: would be faster if use pycopg extra with execute_values , but we need to loop through rows anyways
    # for this table we need to get site_location_id form site_unique
    for row in rows:
        if table_name == 'plot_level_derived_indices':
            row['site_location_visit_id'] = row['site_unique'].split('-')[-1]
            # get site location id
            query = f'''
            SELECT site_location_id FROM site_location WHERE site_location_name = '{row['site_unique'].split('-')[0]}'
            '''
            cursor.execute(query)
            row['site_location_id'] = cursor.fetchone()[0]

        if table_name == 'species_level_invasion_status' and row['AAFSS_scientific_name'] == 'NA':
            continue
            
        
        # drop key with NA values
        for key in list(row.keys()):
            if row[key] == 'NA':
                del row[key]

        columns = ','.join(row.keys())
        placeholders = ','.join(['%s'] * len(row))
        query = f'''
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders})
        '''

        values = tuple(row.values())

        try:
            cursor.execute(query, values)
        except Exception as e:
            print(query)
            print(row)
            print(values)
            print(table_name)
            raise e


make_tables()
cursor.close()
