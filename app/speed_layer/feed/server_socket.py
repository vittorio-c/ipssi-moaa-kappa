import socket
import os
import pickle

from app.speed_layer.kafka.kafka_producer_factory import get_producer


def publish_to_kafka(message):
    print('Publishing of Kafka topic...')
    producer = get_producer('localhost:9092')
    producer.send('raw_probes_results', message)


def close_connection(server):
    print("-" * 20)
    print("Shutting down...")
    server.close()
    os.remove("/tmp/python_unix_sockets_example")
    print("Done")


if os.path.exists("/tmp/python_unix_sockets_example"):
    os.remove("/tmp/python_unix_sockets_example")

print("Opening socket...")
server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
server.bind("/tmp/python_unix_sockets_example")

print("Listening...")
try:
    while True:
        datagram = server.recv(100000)
        if not datagram:
            break
        else:
            print("-" * 20)
            decoded = pickle.loads(datagram)
            print('Decoded data. Station : ' + decoded['STATION'] + '. Date: ' + str(decoded['DATE']))
            publish_to_kafka(decoded)
except KeyboardInterrupt as k:
    close_connection(server)

close_connection(server)
