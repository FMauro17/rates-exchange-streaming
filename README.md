# Rates Exchange Streaming

Pipeline de streaming en tiempo real que consulta el precio de BTC/USD, publica los datos en Kafka y los consume simultáneamente en PostgreSQL y MinIO.

## Arquitectura

API BTC/USD → Producer → Kafka → Consumer PostgreSQL
                                → Consumer MinIO

## Servicios

- **Zookeeper** → gestor de Kafka
- **Kafka** → sistema de mensajería
- **Kafka UI** → interfaz web para monitorear mensajes
- **PostgreSQL** → almacenamiento relacional
- **pgAdmin** → interfaz web para PostgreSQL
- **MinIO** → almacenamiento tipo Data Lake

## Tecnologías

- Python 3.11
- Apache Kafka
- PostgreSQL
- MinIO (S3 compatible)
- Docker y Docker Compose

## Cómo ejecutar

1. Clonar el repositorio
2. Ejecutar `docker compose up --build -d`
3. Verificar Kafka UI en `http://localhost:8080`
4. Verificar pgAdmin en `http://localhost:5050`
5. Verificar MinIO en `http://localhost:9001`

## Estructura del proyecto

rates-exchange-streaming/
├── app/
│   ├── producer/
│   │   └── producer.py
│   ├── consumer/
│   │   ├── consumer_postgres.py
│   │   └── consumer_minio.py
│   └── sql/
│       └── init.sql
├── Dockerfile
├── docker-compose.yml
└── requirements.txt

