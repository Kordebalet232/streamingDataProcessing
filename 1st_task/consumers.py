from confluent_kafka import Producer, Consumer, TopicPartition
import time
import sys
import json
import pandas as pd

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
        messages = []
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:  # если сообщение отсутствует - переходим к следующей итерации
                break
            elif not message.error():  # если сообщение получено без ошибок
                value = message.value().decode("utf-8")
                data = json.loads(value)
                messages.append(data)
                # print('Received message: {}'.format(message.value().decode('utf-8')))  # обрабатываем полученное сообщение
            else:
                print('Error occured')

        print(f"Получено {len(messages)} сообщений")
        if len(messages) > 0:
            df = pd.DataFrame(messages)
            print("Среднее по названию датчика: ")
            mean_by_name = df.groupby("name").value.mean()
            print(mean_by_name.to_markdown())
            print("Среднее по типу датчика: ")
            mean_by_type = df.groupby("measure").value.mean()
            print(mean_by_type.to_markdown())
        time.sleep(TIME_TO_SLEEP)

except KeyboardInterrupt:
    sys.stderr.write('Aborted by user\n')

finally:
    consumer.close()