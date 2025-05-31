import os
import sys
import numpy as np
import pandas as pd

'''
defining common constant variable for traning pipeline
'''
TARGET_COLUMN = "Result"
PIPELINE_NAME: str ="SupplyChain"
ARTIFACT_DIR: str ="Artifacts"
FILE_NAME: dict = {
    "weather": "Weather_Data.csv",
    "shipments": "Shipment_Route_Data.csv",
    "conflicts": "Geopolitical_Data.csv"
}

TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"
#DATA_INGESTION_COLLECTION_NAME_SELECTED = None
#FILE_NAME_SELECTED = None
'''
    Data Ingestion related constants 
'''

DATA_INGESTION_COLLECTION_NAME = {
    "weather": "Weather",
    "shipments": "Shipments",
    "conflicts": "Conflicts"
}
DATA_INGESTION_DATABASE_NAME: str = "Lajvi"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATION: float = 0.2
