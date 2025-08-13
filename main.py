# TESTING THE CODE
#FROM HERE WE WILL TRIGGER EVERYTHING

from supplychain.components.data_ingestion import DataIngestion
from supplychain.Exception.exception import SupplychainException
from supplychain.Logging.logging import logging
from supplychain.entity.config_entity import DataIngestionConfig
from supplychain.entity.config_entity import TrainingPipelineConfig

import sys


if __name__ =='__main__':
    try: 
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initate Data Ingestion")

        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        print(dataingestionartifact)

        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_validation = DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate the data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("data validation completed")
        print(data_validation_artifact)

    except Exception as e:
        raise SupplychainException(e,sys)

    


    except Exception as e:
        raise SupplychainException(e,sys)
