import json
import os
import uuid
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'btc_rates')
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'btc-rates')

def crear_cliente_minio():
    return boto3.client(
        's3',
        endopint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
        config=boto3.session.Config(signature_version='s3v4')
    )

def verificar_o_crear_bucket(s3):
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' ya existe.")
    except ClientError: 
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' creado exitosamente.") 

def crear_consumer_kafka():
    return Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'minio_consumer_group',
        'auto.offset.reset': 'earliest'
    })

def guardar_en_minio(s3, datos): 
    nombre_archivo = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=nombre_archivo,
        Body=json.dumps(datos).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Guardado en MinIO: {nombre_archivo}")

def main():
    s3 = crear_cliente_minio()
    verificar_o_crear_bucket(s3)
    consumer = crear_consumer_kafka()
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Consumer MinIO escuchando topic: {KAFKA_TOPIC}") 

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
            guardar_en_minio(s3, datos)
        except Exception as e:
            print(f"Error al guardar en MinIO: {e}")

        
if __name__ == "__main__":
    main()