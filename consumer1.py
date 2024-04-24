import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from itertools import combinations

# Kafka consumer settings
bootstrap_servers = 'localhost:9092'
topic = 'amazon_reviews_topic'

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['amazon_reviews_db']
collection = db['frequent_itemsets']

# Function to generate candidate itemsets
def generate_candidate_itemsets(frequent_itemsets, k):
    candidate_itemsets = set()
    for itemset1 in frequent_itemsets:
        for itemset2 in frequent_itemsets:
            if len(itemset1.union(itemset2)) == k:
                candidate_itemsets.add(itemset1.union(itemset2))
    return candidate_itemsets

# Function to calculate support for an itemset
def calculate_support(transactions, candidate_itemset):
    count = 0
    for transaction in transactions:
        if candidate_itemset.issubset(transaction):
            count += 1
    return count / len(transactions)

# Function to generate frequent itemsets
def generate_frequent_itemsets(transactions, min_support):
    frequent_itemsets = [set()]
    k = 1
    while True:
        candidate_itemsets = generate_candidate_itemsets(frequent_itemsets, k)
        frequent_itemsets = [itemset for itemset in candidate_itemsets if calculate_support(transactions, itemset) >= min_support]
        if not frequent_itemsets:
            break
        yield frequent_itemsets
        k += 1

# Function to insert frequent itemsets into MongoDB
def insert_frequent_itemsets_into_mongodb(frequent_itemsets, min_support):
    for itemset in frequent_itemsets:
        itemset_dict = {'itemset': sorted(list(itemset)), 'support': calculate_support(transactions, itemset)}
        collection.insert_one(itemset_dict)

# Create Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Process messages
for message in consumer:
    data = message.value
    also_buy = data.get('also_buy', [])
    transactions = [set(also_buy)]
    min_support = 0.1  # Minimum support threshold

    print("Received data:", data)
    print("Transactions:", transactions)

    # Generate frequent itemsets using the Apriori algorithm
    frequent_itemsets = next(generate_frequent_itemsets(transactions, min_support), None)

    if frequent_itemsets:
        print("Frequent itemsets:", frequent_itemsets)

        # Insert frequent itemsets into MongoDB
        insert_frequent_itemsets_into_mongodb(frequent_itemsets, min_support)
    else:
        print("No frequent itemsets found.")

