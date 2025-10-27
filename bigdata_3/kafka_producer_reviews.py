import time
import json
import random
from kafka import KafkaProducer

reviews = [
    "Excelente producto, me encantó la calidad.",
    "Muy mala experiencia, no lo recomiendo.",
    "Regular, esperaba más del producto.",
    "El empaque llegó dañado pero el producto está bien.",
    "Superó mis expectativas, volveré a comprar."
]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    review = random.choice(reviews)
    data = {"review": review, "timestamp": int(time.time())}
    producer.send('amazon_reviews', value=data)
    print(f"Sent: {data}")
    time.sleep(2)

