import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from collections import defaultdict

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'amazon_reviews_topic'

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['amazon_reviews_db']
collection = db['brand_usage']

# Create Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Dictionary to store brand usage and its association with categories
brand_usage = defaultdict(int)
brand_category_association = defaultdict(list)

# Function to update brand usage and association with categories based on received data
def update_brand_usage(data):
    asin = data.get('asin', '')
    brand = data.get('brand', '')
    category = data.get('category', '')

    print("Received data - ASIN:", asin, "Brand:", brand, "Category:", category)

    if brand:
        brand_usage[brand] += 1
        if category:
            brand_category_association[brand].append(category)

            # Update MongoDB collection with brand usage and association
            collection.update_one(
                {"brand": brand},
                {"$set": {"brand": brand, "usage": brand_usage[brand], "categories": brand_category_association[brand]}},
                upsert=True
            )

# Process messages
for message in consumer:
    data = message.value
    update_brand_usage(data)

# Function to print out popular brands and their association with categories
def print_popular_brands_and_categories(num_brands=5):
    sorted_brands = sorted(brand_usage.items(), key=lambda x: x[1], reverse=True)[:num_brands]
    print("Most popular brands:")
    for brand, count in sorted_brands:
        if brand:
            print("- Brand:", brand)
            print("  Frequency:", count)
            categories = brand_category_association.get(brand, [])
            if categories:
                print("  Associated Categories:", categories)
            else:
                print("  No associated categories found.")

# Print out popular brands and their association with categories
print_popular_brands_and_categories()

