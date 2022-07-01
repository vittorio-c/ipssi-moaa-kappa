import datetime

from app.speed_layer.kafka.kafka_consumer_factory import get_consumer
from kafka.consumer.fetcher import ConsumerRecord
import mysql.connector

mariabdb_client = mysql.connector.connect(
    host="localhost",
    user="lambda_user",
    password="user_password",
    database="moaa_db"
)


consumer = get_consumer('agg_probes_results', 'localhost:9092')


def create_table_if_not_exists():
    cursor = mariabdb_client.cursor()
    sql = """
        CREATE TABLE IF NOT EXISTS real_time_tmp_by_hour (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reported_hour DATETIME UNIQUE NOT NULL,
            mean_tmp FLOAT NOT NULL,
            min_tmp FLOAT NOT NULL,
            max_tmp FLOAT NOT NULL 
        );
        """

    mariabdb_client.commit()
    cursor.execute(sql)
    cursor.close()


create_table_if_not_exists()

print(consumer)
message: ConsumerRecord
for message in consumer:
    print(f"Consumming message n° {message.offset} on topic agg_probes_results\n")
    decoded_message = message.value

    cursor = mariabdb_client.cursor()
    reported_hour = datetime.datetime.fromisoformat(decoded_message['datetime']['start']).strftime("%Y-%m-%d %H:%M:%S")
    mean_tmp = decoded_message['mean_tmp']
    max_tmp = decoded_message['max_tmp']
    min_tmp = decoded_message['min_tmp']
    sql = f"""
        INSERT INTO real_time_tmp_by_hour (reported_hour, mean_tmp, max_tmp, min_tmp) 
        VALUES ("{str(reported_hour)}", {mean_tmp}, {max_tmp}, {min_tmp})
        ON DUPLICATE KEY UPDATE 
            mean_tmp = {mean_tmp},
            max_tmp = {max_tmp},
            min_tmp = {min_tmp}
        """

    try:
        cursor.execute(sql)
    except Exception as e:
        print('Exception during mysql insert step !')
        print(e)

    mariabdb_client.commit()
    cursor.close()
