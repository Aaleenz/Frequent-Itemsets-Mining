import json
from kafka import KafkaProducer
from time import sleep

bootstrap_servers = ['localhost:9092']
topic = 'amazon_reviews_topic'
json_file_path = '/home/asmariaz/Downloads/preprocessed_data.json'

def load_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = [json.loads(line) for line in file]
        return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in '{file_path}': {e}")
    return None

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
data = load_json_file(json_file_path)

if data:
    for item in data:
        producer.send(topic, value=item)
        producer.flush()
        print('Record sent to Kafka: ', item)
        sleep(5)
else:
    print('No data loaded from JSON file.')

