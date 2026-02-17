from kafka import KafkaConsumer
import json
import os

TOPICS = {
    "business": "yelp_business_raw",
    "review": "yelp_review_raw",
    "user": "yelp_user_raw"
}

OUTPUT_DIR = os.path.join(os.getcwd(), "data/raw")

os.makedirs(OUTPUT_DIR, exist_ok=True)

for name, topic in TOPICS.items():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id=f'{name}_group'
    )
    
    output_file = os.path.join(OUTPUT_DIR, f"{name}.json")
    with open(output_file, "w") as f:
        for msg in consumer:
            f.write(msg.value.decode() + "\n")
            # Stop after consuming all messages currently in topic
            if consumer.position(consumer.assignment().pop()) == 0:
                break
    print(f"[+] Saved {topic} to {output_file}")
