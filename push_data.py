import os
import sys
import json
import certifi

os.environ['SSL_CERT_FILE'] = certifi.where()

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

#import certifi
#ca=certifi.where()  ## ca = certified authorities 

import numpy as np
import pandas as pd 
import pymongo
from supplychain.Exception.exception import SupplychainException
from supplychain.Logging.logging import logging

class DataExtract():
    def __init__(self): 
        try: 
            pass
        except Exception as e:
            raise SupplychainException(e,sys)
        
    def csv_to_json_convertor(self,file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e: 
            raise SupplychainException(e,sys)
        
    def insert_data_mongodb (self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records
            #self.mongo_clients = pymongo.MongoClient(MONGO_DB_URL)
            self.mongo_clients = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=certifi.where())

            self.database = self.mongo_clients[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            return(len(self.records))

        except Exception as e:
            raise SupplychainException(e,sys)
"""
        
if __name__=="__main__":
    FILE_PATH = "Datasets/Weather_Data_2023.csv"
    DATABASE = "Lajvi"
    Collection = "Datasets"
    supplyobj = DataExtract()
    records = supplyobj.csv_to_json_convertor(file_path=FILE_PATH)
    print(records)
    no_of_records =supplyobj.insert_data_mongodb(records, DATABASE, Collection)
    print(no_of_records)
"""

if __name__ == "__main__":
    DATABASE = "Lajvi"


     # Step 1: Merge Weather Files
    try:
        print("Merging weather datasets...")
        weather_2023 = pd.read_csv("Datasets/Weather_Data_2023.csv")
        weather_2024 = pd.read_csv("Datasets/Weather_Data_2024.csv")
        weather_2025 = pd.read_csv("Datasets/Weather_Data_2025.csv")
        merged_weather = pd.concat([weather_2023, weather_2024, weather_2025], ignore_index=True)
        merged_weather.to_csv("Datasets/Weather_Data.csv", index=False)
        print("Merged weather data saved to Weather_Data.csv")
    except Exception as e:
        raise SupplychainException(f"Failed to merge weather files: {e}", sys)
    
    # Step 2: Upload Data to MongoDB
    file_collection_map = {
        "Datasets/Weather_Data.csv": "Weather",
        "Datasets/Shipment_Route_Data.csv": "Shipments",
        "Datasets/Geopolitical_Data.csv": "Conflicts",
    }

    supplyobj = DataExtract()

    for file_path, collection_name in file_collection_map.items():
        try:
            records = supplyobj.csv_to_json_convertor(file_path=file_path)
            no_of_records = supplyobj.insert_data_mongodb(records, DATABASE, collection_name)
            print(no_of_records)

        except Exception as e:
            print(f" Failed to insert data from {file_path}: {str(e)}")

     # Step 3: Upload Merged Dataset to MongoDB
    try:
        print("Uploading merged ML dataset to MongoDB...")
        merged_file_path = "Datasets/SupplyChain_Data.csv"
        merged_collection_name = "SupplyChain_Data"

        merged_records = supplyobj.csv_to_json_convertor(file_path=merged_file_path)
        merged_count = supplyobj.insert_data_mongodb(merged_records, DATABASE, merged_collection_name)

        print(f"Inserted {merged_count} records into collection '{merged_collection_name}'")
    except Exception as e:
        print(f"Failed to insert merged dataset: {str(e)}")