# TESTING THE CODE
#FROM HERE WE WILL TRIGGER EVERYTHING

from supplychain.components.data_ingestion import DataIngestion
from supplychain.Exception.exception import SupplychainException
from supplychain.Logging.logging import logging
from supplychain.entity.config_entity import DataIngestionConfig
from supplychain.entity.config_entity import TrainingPipelineConfig
from supplychain.constant.training_pipeline import DATA_INGESTION_COLLECTION_NAME_SELECTED, FILE_NAME_SELECTED

import sys
'''

if __name__ =='__main__':
    try: 
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initate Data Ingestion")

        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)


    except Exception as e:
        raise SupplychainException(e,sys)
'''

if __name__ == '__main__':
    try:
        logging.info("Pipeline execution started")

        # Initialize pipeline and config
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)

        # Run data ingestion
        data_ingestion = DataIngestion(data_ingestion_config)
        logging.info("Initiating data ingestion process...")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        # Print the artifact (paths to train/test data)
        print(f"Train file saved at: {data_ingestion_artifact.train_file_path}")
        print(f"Test file saved at: {data_ingestion_artifact.test_file_path}")
        logging.info("Pipeline execution completed successfully")

    except Exception as e:
        logging.error("An error occurred during the pipeline execution.")
        raise SupplychainException(e, sys)