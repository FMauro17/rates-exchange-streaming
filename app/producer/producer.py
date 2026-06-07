import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'btc_rates')
API_URL = "https://api.coindesk.com/v1/bpi/currentprice/USD.json"
INTERVAL = 10 


def obtener_precio_btc():
    respuesta = requests.get(API_URL, timeout=10)
    respuesta.raise_for_status()
    datos = respuesta.json()
    precio = datos['bpi']['USD']['rate_float']
    return precio

def publicar_mensaje(producer, precio):
    mensaje = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": "BTC/USD",
        "price": precio   
    }
    producer.produce(
        topic=KAFKA_TOPIC,
        key="btc",
        value=json.dumps(mensaje).encode("utf-8")
    )
    producer.flush()
    print(f"Mensaje publicado: {mensaje}") 

def main():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    print(f"Producer conectado a {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")

    while True:
        try:
            precio = obtener_precio_btc()
            publicar_mensaje(producer, precio)
        except requests.exceptions.RequestException as e:
            print(f"Error al llamar la API: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}") 
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main() 
