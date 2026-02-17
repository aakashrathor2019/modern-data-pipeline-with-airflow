from kafka import KafkaProducer
import json, time, os

KAFKA_BROKER = "localhost:9092"

TOPICS = {
    "business": "yelp_business_raw",
    "review": "yelp_review_raw",
    "user": "yelp_user_raw"
}

SLEEP_TIME = float(os.getenv("SLEEP_TIME", "0.005"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def publish_file(file_path, topic):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    print(f"Publishing {file_path} → {topic}")
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:

            try:
                record = json.loads(line)
                producer.send(topic, value=record)

                time.sleep(SLEEP_TIME)

            except Exception as e:
                print("Bad record skipped:", e)

    producer.flush()

    print(f"Completed {topic}")


if __name__ == "__main__":

    base_path = os.path.join(os.path.dirname(__file__),"../data")
    publish_file(os.path.join(base_path, "yelp_academic_dataset_business.json"),TOPICS["business"])
    publish_file(os.path.join(base_path, "yelp_academic_dataset_review.json"),
        TOPICS["review"]
    )

    publish_file(
        os.path.join(base_path, "yelp_academic_dataset_user.json"),
        TOPICS["user"]
    )
