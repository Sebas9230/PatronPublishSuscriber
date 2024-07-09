from confluent_kafka import Consumer, KafkaError

# Configuración del consumidor
conf = {
    'bootstrap.servers': '',
    'group.id': 'python-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '',
    'sasl.password': ''
}

# Crear el consumidor
consumer = Consumer(conf)

# Suscribirse a los topics
consumer.subscribe(['science', 'sports'])

# Consumir mensajes
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f"Mensaje recibido en {msg.topic()}: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    # Cerrar el consumidor al terminar
    consumer.close()
