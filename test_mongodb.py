import os
import certifi
from pymongo.mongo_client import MongoClient

os.environ['SSL_CERT_FILE'] = certifi.where()

uri = "mongodb+srv://lb71:Admin123@cluster0.ln7yvfh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)