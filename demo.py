import pandas as pd

data= pd.read_csv("Datasets/SupplyChain_Data.csv")
print(data.head())
#print(data.info())

dataset_info = {
    "Shape": data.shape,
    "Columns": data.columns.tolist(),
    "Missing Values": data.isnull().sum(),
    "Data Types": data.dtypes,
    "Descriptive Statistics": data.describe(include='all').transpose()
}

print(dataset_info)


data = data.drop(columns=['Result'], errors='ignore')
data.to_csv("Datasets/SupplyChain_Data.csv")
print("Null values are removed")