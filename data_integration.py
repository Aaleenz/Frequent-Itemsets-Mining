from pymongo import MongoClient
import jsonlines
import json
# Function to insert data into MongoDB chunk by chunk
def insert_data_in_chunks(data, chunk_size, collection):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        collection.insert_many(chunk)

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['amazon_database']  # Replace 'your_database' with your database name
collection = db['amazon_dataset']  # Replace 'your_collection' with your collection name

# Open the JSON file using jsonlines and insert data into MongoDB chunk by chunk
with open('preprocessed_data.json', 'r') as file:
    chunk_size = 1000  # Adjust chunk size as needed
    batch = []
    for line in file:
        batch.append(json.loads(line))
        if len(batch) >= chunk_size:
            collection.insert_many(batch)
            batch = []
    if batch:
        collection.insert_many(batch)

