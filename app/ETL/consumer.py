# Récuperer un message sérialisé depuis le BUS

# Importer pyspark

# Importer les fonctions de MAJ de la data qui m'intéressent

# Insérer en DB en écrasant la donnée
from kafka.consumer.fetcher import ConsumerRecord

from kafka_consumer_factory import get_consumer

consumer = get_consumer('moaa', "localhost:9092")
print(consumer)
message: ConsumerRecord
for message in consumer:
    print(f"Consumming message n° {message.offset} on topic moaa\n")
    print(message)



