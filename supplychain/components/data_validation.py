# Here, we will read the data given by data ingestion artifact
# which is the train.csv and test.csv

from SupplyChain.entity.artifact_entity import DataIgestionArtifact, DataValidationArtifact
from SupplyChain.entity.config_entity import DataValidationConfig
from SupplyChain.Exception.exception import SupplychainException
from SupplyChain.Logging.logging import logging
from SupplyChain.constant.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import pandas as pd
import os, sys
from SupplyChain.utils.main_utils import read_yaml_file, write_yaml_file

class DataValidation:
    '''
    What's happening ?
     1. The artifact from the previous step (data ingestion) which 
     contains paths to train.csv and test.csv

     2. The config for validation (e.g. output file paths)

     3. The schema YAML file so it knows what the correct column names/types are
    '''
    def __init__(self, data_ingestion_artifact:DataIgestionArtifact,
                 data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config=read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise SupplychainException(e,sys)

    #Reads any CSV and returns it as a pandas DataFrame.  
    @staticmethod
    def read_data(file_path)-> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise SupplychainException(e,sys)

    #Checks whether the number of columns in the DataFrame matches what's expected in the schema.
    def validate_no_of_columns(self, dataframe:pd.DataFrame)->bool:
        try:
            no_of_columns = len(self._schema_config)
            logging.info(f"Required no of columns : {no_of_columns}")
            logging.info(f"Data frame has number of columns : {len(dataframe.columns)}")

            if len(dataframe.columns) == no_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise SupplychainException(e,sys)
        
    '''
    Uses the Kolmogorov–Smirnov test to compare the distribution of each column 
    in train.csv vs test.csv.

    If the p-value is very small (below threshold), it means the data distributions are different (data drift).
    It saves the results in a YAML file.
    '''
        
    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}

            for column in base_df.columns:
                d1=base_df[column]
                d2 = current_df[column]

                is_sample_dist_same = ks_2samp(d1,d2)
                if threshold<=is_sample_dist_same.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False

                report.update({column:{
                    "p_value":float(is_sample_dist_same.pvalue),
                    "drift_status":is_found
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            #Creaate directory 
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            return status

        except Exception as e:
            raise SupplychainException(e,sys)
        
#in the function below, first we need to get the information about train and test data 
#from the data ingestion artifact
        '''
        main method that : 

        - load the train and test dataset
        - Validates number of columns
        - Detects data drift
        - Saves clean versions of train/test CSVs
        - Returns a DataValidationArtifact object

        '''
               
    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

        ## Read the data from the train and test files then we need to validate the no of columns
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            error_message = ""

            #Validate the number of columns
            status = self.validate_no_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = f"{error_message} Train datafram does not contain all columns. \n"
            status = self.validate_no_of_columns(dataframe=test_dataframe)

            if not status:
                error_message = f"datafram does not contain all columns \n"

            ##Check datadrift
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path, index = False, header=True
            )
                
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )

            data_validation_artifact = DataValidationArtifact(
                validation_status = status,
                valid_train_file_path = self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,

            )
            return data_validation_artifact

            #validate the numeric columns 
        except Exception as e:
            raise SupplychainException(e,sys)
