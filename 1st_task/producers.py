from confluent_kafka import Producer, Consumer, TopicPartition
import time
import random
import json

TOPIC_NAME = "devices"
NUM_DEVICES = 5
SLEEPING_TIME = 1

measures = [('pressure', (0, 200)),
            ('temperature', (-200, 200)),
            ('moisture', (0, 100)),
            ]

producers = []
for i in range(NUM_DEVICES):
    producer = Producer({'bootstrap.servers': "localhost:9092"})
    measure, range  = random.choice(measures)
    
    producers.append((f' Device {i}', measure, range,  producer))

    # Генерация и отправка сообщений

messages_sent = 0

while True:
    

    name, measure, range, producer = random.choice(producers)

    data = {
        "name": name,
        "timestamp": str(time.time()),
        "measure": str(measure),
        "value": str(random.uniform(range[0], range[1]))
        }
    
    producer.produce(TOPIC_NAME, value=json.dumps(data, indent=2).encode('utf-8'), callback="localhost:9092")
    producer.flush()

    messages_sent += 1

    time.sleep(SLEEPING_TIME)