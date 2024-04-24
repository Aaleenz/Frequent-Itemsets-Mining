import pymongo
from kafka import KafkaConsumer
import json
from collections import Counter

# Kafka configuration
bootstrap_servers = ['tooba:9092']
topic = 'amazon_reviews_topic'

# PCY parameters
bucket_size = 1000  # Size of hash table
hash_table = [0] * bucket_size  # Hash table to count item pairs and triples
bitmap = [0] * bucket_size  # Bitmap to mark frequent buckets
support_threshold = 50 # Support threshold for frequent itemsets
item_support = Counter()  # Dictionary to track support count of individual items

# MongoDB configuration
mongo_client = pymongo.MongoClient("mongodb://localhost:9092/")
mongo_db = mongo_client["amazon_reviews_db"]
mongo_collection = mongo_db["itemsets"]

def hash_pair(item1, item2):
    """Hash function for pairs of items"""
    combined_hash = hash(item1) + hash(item2)
    return combined_hash % bucket_size

def hash_triple(item1, item2, item3):
    """Hash function for triples of items"""
    combined_hash = hash(item1) + hash(item2) + hash(item3)
    return combined_hash % bucket_size

def process_itemset(itemset):
    """Process an itemset and update hash table and bitmap"""
    global hash_table, bitmap
    
    if isinstance(itemset, list):
        # Update hash table for pairs
        for i in range(len(itemset)):
            for j in range(i + 1, len(itemset)):
                pair_hash = hash_pair(itemset[i], itemset[j])
                hash_table[pair_hash] += 1
                
                if hash_table[pair_hash] >= support_threshold:
                    bitmap[pair_hash] = 1

        # Update hash table for triples
        for i in range(len(itemset)):
            for j in range(i + 1, len(itemset)):
                for k in range(j + 1, len(itemset)):
                    triple_hash = hash_triple(itemset[i], itemset[j], itemset[k])
                    hash_table[triple_hash] += 1

                    if hash_table[triple_hash] >= support_threshold:
                        bitmap[triple_hash] = 1

def insert_into_mongodb(items_with_support):
    """Insert items and their support counts into MongoDB"""
    for item, support_count in items_with_support.items():
        item_data = {"name": item, "support_count": support_count}
        mongo_collection.insert_one(item_data)

def consume_messages_and_insert():
    """Consume messages from Kafka, process itemsets, and insert into MongoDB"""
    consumer = KafkaConsumer(topic,
                             group_id='group_1',
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    total_itemsets = 0  # Counter for total processed itemsets

    for message in consumer:
        itemset = message.value

        if isinstance(itemset, list):
            process_itemset(itemset)
            total_itemsets += 1
            
            if total_itemsets % 100 == 0:
                # Insert into MongoDB every 100 processed itemsets
                insert_into_mongodb(item_support)
                item_support.clear()  # Clear item support data

# Call the function to start consuming messages and insert into MongoDB
consume_messages_and_insert()

