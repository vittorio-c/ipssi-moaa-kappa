import json

from kafka import KafkaConsumer


def get_consumer(topic, server):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[server],
        value_deserializer=lambda m: json.loads(m.decode("ascii")),
    )
