from confluent_kafka import Consumer, Producer


def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config


def produce(topic, config):
    # creates a new producer instance
    producer = Producer(config)

    # produces a sample message
    key = "key"
    value = "value"
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


def consume(topic, config):
    # sets the consumer group ID and offset
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer instance
    consumer = Consumer(config)

    # subscribes to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print(
                    f"Consumed message from topic {topic}: key = {key:12} value = {value:12}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()


def main():
    config = read_config()
    topic = "orders"

    produce(topic, config)
    consume(topic, config)


main()
