from kafka import KafkaConsumer
import json
import os

TOPIC = "yelp_review_raw"
RAW_DIR = "data/raw/review"

os.makedirs(RAW_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='consumer-review',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for i, message in enumerate(consumer):
    with open(os.path.join(RAW_DIR, f'review_{i}.json'), 'w') as f:
        json.dump(message.value, f)
