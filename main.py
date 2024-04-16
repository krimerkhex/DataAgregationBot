import datetime
from pymongo import MongoClient
from bson import decode_all
from bson.json_util import dumps

# Make connect to mongoDB.
client = MongoClient('localhost', 27017)
print(client)

# Make databases
data = client['salary']
print(data)

# Create a collection
storage = data['data']

# Insert data
with open("dump/sampleDB/sample_collection.bson", 'rb') as file:
    file_data = decode_all(file.read())
    status = storage.insert_many(file_data)
