from confluent_kafka import Producer, admin
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

def delivery_report(err, msg):
    if err is not None:
        print(f'[ERROR] Ошибка доставки сообщения: {err}')
    else:
        print(f'[INFO] Сообщение успешно доставлено: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}')

producers = []
for i in range(NUM_DEVICES):
    producer = Producer({'bootstrap.servers': "localhost:9092"})
    measure, range  = random.choice(measures)
    
    producers.append((f' Device {i}', measure, range,  producer))

messages_sent = 0

while True:
    

    name, measure, range, producer = random.choice(producers)

    data = {
        "name": name,
        "timestamp": time.time(),
        "measure": measure,
        "value": random.uniform(range[0], range[1])
        }
    
    producer.produce(TOPIC_NAME, key=str(time.time()).encode('utf-8'), value=json.dumps(data, indent=2).encode('utf-8'), callback=delivery_report)
    time.sleep(SLEEPING_TIME)
    producer.flush()

    messages_sent += 1

# def delete_topic(topic_name):
#     a = admin.AdminClient({
#                 "bootstrap.servers": "localhost:9092"
#             })
#     result = a.delete_topics([topic_name])
#     for topic, future in result.items():
#         try:
#             future.result()  # Дождитесь завершения удаления
#             print(f"Тема {topic} успешно удалена")
#         except Exception as e:
#             print(f"Ошибка при удалении темы {topic}: {str(e)}")

# delete_topic("devices")