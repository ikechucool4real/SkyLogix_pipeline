from dotenv import load_dotenv
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime, timezone
import logging
import os
import httpx
import sys
import json

#Configure logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - [%(levelname)s] - %(message)s'
)

#Get environment variables
base_dir = Path(__file__).resolve().parent.parent
load_dotenv(base_dir / '.env')

api_key = os.getenv('api_key')
api_url = os.getenv('api_url')
raw_file_path = base_dir / os.getenv('raw_data')
db_config = {
    'uri' : os.getenv('db_uri'),
    'host': os.getenv('db_host'),
    'port': os.getenv('db_port'),
    'name': os.getenv('db_name')
}

raw_collection = os.getenv('raw_collection')
clean_collection = os.getenv('clean_collection')

#Extract data from url
def extract_data(url, key):
    base_url = f'{url}?city=Lagos&key={key}'
    try: 
        logging.info('Requesting data')
        response = httpx.get(base_url, timeout=10.0)
        response.raise_for_status()
        logging.info('Data extracted successfully')
        return response.json()
    except Exception as e:
        logging.exception(f'Unexpected error: {e}')
    return None

#load raw data to datastore
def load_raw_data(data, dir):
    if not data:
        logging.warning('No data to save')
        return False
    
    with open(dir, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)  # save as formatted JSON
        logging.info(f'Raw data loaded to {dir}')

    return True

#Load raw data to mongodb
def load_raw_to_mongodb(data, db, collection_name):
    try:
        logging.info('Connecting to Database')
        client = MongoClient(db['uri'])
        client.admin.command("ping")
        logging.info('Database connection successful')

        database = client[db['name']]
        collection = database[collection_name]

        doc = {
            **data,
            '_ingested_at_': datetime.now(timezone.utc)
        }

        collection.insert_one(doc) 
        logging.info('Raw data loaded into Database')
        return True

    except Exception as e:
        logging.error(f'failed to load raw data into database: {e}')
        return False

#transform data
def transform_data(data):
    records = []

    for entry in data['data']:
        row = {
            'city_name': data['city_name'],
            'country_code': data['country_code'],
            'ts_datetime': datetime.fromtimestamp(entry['ts'], tz=timezone.utc),
            **entry,
            'weather_icon': entry['weather']['icon'],
            'weather_code': entry['weather']['code'],
            'weather_description': entry['weather']['description']
        }

        del row['weather']
        records.append(row)

    return records

#Load clean data to mongodb
def load_clean_to_mongodb(data, db, collection_name):
    try:
        logging.info('Connecting to Database')
        client = MongoClient(db['uri'])
        client.admin.command("ping")
        logging.info('Database connection successful')

        database = client[db['name']]
        collection = database[collection_name]

        for record in data:
            record['_ingested_at_'] = datetime.now(timezone.utc)
        
        collection.insert_many(data)
        logging.info(f"Inserted {len(data)} records")
        return True

    except Exception as e:
        logging.error('failed to load clean data into database: {e}')
        return False


#Entry point to app
def main():
    raw_data = extract_data(api_url, api_key)
    if not raw_data:
        return 1
    
    if not load_raw_data(raw_data, raw_file_path):        
        return 1
    
    if not load_raw_to_mongodb(raw_data, db_config, raw_collection):        
        return 1
    
    clean_data = transform_data(raw_data) 
    if not clean_data:
        return 1

    if not load_clean_to_mongodb(clean_data, db_config, clean_collection):        
        return 1
           
    return 0
        

if __name__ == '__main__':
    sys.exit(main())