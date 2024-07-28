import json
import os
import time

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker("pt_BR")

regions = ["Sul", "Sudeste", "Centro-Oeste", "Norte", "Nordeste"]
vendors = ["Kaio Silva", "Luciano Galvão", "Fabio Melo", "Estagiario"]


def generate_fake_order(start_date="today", end_date="today"):
    quantity = fake.random_int(min=1, max=10)
    unit_price = fake.random_int(min=50, max=200)
    total_price = quantity * unit_price
    return {
        "order_id": fake.uuid4(),
        "order_date": str(fake.date_between(start_date=start_date, end_date=end_date)),
        "product_id": fake.uuid4(),
        "region": fake.random_element(elements=regions),
        "vendor": fake.random_element(elements=vendors),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
    }


def generate_fake_orders_csv(n_rows=1000):
    import pandas as pd

    data = [generate_fake_order() for _ in range(n_rows)]
    pd.DataFrame(data).to_csv("fake_orders.csv", index=False)


producer_conf = {
    # Required connection configs for Kafka producer, consumer, and admin
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["SASL_USERNAME"],
    "sasl.password": os.environ["SASL_PASSWORD"],
}

producer = Producer(producer_conf)


def generate_message(data):
    data = data
    key = data["order_id"]
    value = json.dumps(data)
    producer.produce(topic="orders", key=key, value=value)
    print(f"orders with key {key}: value = {value}")


if __name__ == "__main__":
    while True:
        data = generate_fake_order(start_date="-30d", end_date="today")
        generate_message(data)
        time.sleep(0.1)
