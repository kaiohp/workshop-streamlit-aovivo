import json
import os

from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

consumer_conf = {
    # Required connection configs for Kafka producer, consumer, and admin
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["SASL_USERNAME"],
    "sasl.password": os.environ["SASL_PASSWORD"],
}


def get_message():
    consumer_conf["group.id"] = "streamlit-app"
    consumer_conf["auto.offset.reset"] = "earliest"
    consumer_conf["session.timeout.ms"] = 45000
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["orders"])

    try:
        while True:
            message = consumer.poll(1.0)
            if message is not None and message.error() is None:
                key = message.key().decode("utf-8")
                data = message.value().decode("utf-8")
                print(f"order with key {key}: value = {data}")
                dict_data = json.loads(data)
                return dict_data
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    new_order = get_message()
    print(new_order)
