from kafka import KafkaProducer
import json

TOPICS = {
    "review": "yelp_review_raw"
}

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

file_path = '/home/my/yelp_data_engineering_project/data/yelp_academic_dataset_review.json'

with open(file_path, 'r') as f:
    for line in f:
        data = json.loads(line)
        producer.send(TOPICS["review"], value=data)

producer.flush()
print(f"All review data sent to topic '{TOPICS['review']}'!")
