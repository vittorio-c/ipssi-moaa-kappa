import json

from kafka import KafkaProducer


def get_producer(server):
    return KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=lambda message: json.dumps(message, default=str).encode("ascii"),
    )
