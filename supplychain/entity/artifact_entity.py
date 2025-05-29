from dataclasses import dataclass #Acts like decorator that creates variable for the empty class

#The output of data ingestion config is data ingestion artifacts
@dataclass
class DataIgestionArtifact:
    train_file_path:str
    test_file_path:str