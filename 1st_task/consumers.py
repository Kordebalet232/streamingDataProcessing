from confluent_kafka import Producer, Consumer, TopicPartition
import time
import sys

TOPIC_NAME = "devices"
TIME_TO_SLEEP = 10

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "group1",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC_NAME])

try:
    while True:
        while True:
            message = consumer.poll(1.0)
            if message is None:  # если сообщение отсутствует - переходим к следующей итерации
                break
            elif not message.error():  # если сообщение получено без ошибок
                print('Received message: {}'.format(message.value().decode('utf-8')))  # обрабатываем полученное сообщение
            else:
                print('Error occured')
        
        time.sleep(TIME_TO_SLEEP)
except KeyboardInterrupt:
    sys.stderr.write('Aborted by user\n')

finally:
    consumer.close()