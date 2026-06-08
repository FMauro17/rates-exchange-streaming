import json
import os 
import psycopg2
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'btc_rates')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'rates_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

def crear_conexion_postgre():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def crear_consumer_kafka():
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'postgres_consumer_group',
        'auto.offset.reset': 'earliest'
    })

def insertar_en_postgres(conn, datos):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO btc_rates (data) VALUES (%s)",
        (json.dumps(datos),)
    )
    conn.commit()
    cursor.close()

def main():
    conn = crear_conexion_postgre()
    consumer = crear_consumer_kafka()
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Consumer PostgreSQL escuchando topic: {KAFKA_TOPIC}") 

    while True:
        mensaje = consumer.poll(timeout=1.0)

        if mensaje is None:
            continue
        if mensaje.error():
            if mensaje.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Error de Kafka: {mensaje.error()}")
            continue

        try:
            datos = json.loads(mensaje.value().decode('utf-8')) 
            insertar_en_postgres(conn, datos)
            print(f"Insertado en PostgreSQL: {datos}")
        except Exception as e:
            print(f"Error al insertar: {e}") 

if __name__ == "__main__":
    main()
