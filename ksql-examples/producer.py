import json
import random
import time

from kafka import KafkaProducer


USERS_DATA = [
    {'userid': '1', 'name': 'User 1', 'age': 25},
    {'userid': '2', 'name': 'User 2', 'age': 32},
    {'userid': '3', 'name': 'User 3', 'age': 29},
    {'userid': '4', 'name': 'User 4', 'age': 18},
    {'userid': '5', 'name': 'User 5', 'age': 46},
]


class UserProducer:

    KAFKA_TOPIC_NAME = 'users'

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def push_data(self, data):
        print(f'Pushing new data to kafka: {data}')
        json_data = json.dumps(data).encode('utf-8')
        key = data['userid'].encode('utf-8')
        self.producer.send(self.KAFKA_TOPIC_NAME, key=key, value=json_data)

    def run(self):
        while True:
            user = random.choice(USERS_DATA)
            data = {
                **user,
                'itemscount': random.randint(0, 100),
                'recordtime': int(time.time() * 1000),
            }
            self.push_data(data)
            time.sleep(1)


if __name__ == '__main__':
    producer = UserProducer()
    print(f'Starting the {producer.KAFKA_TOPIC_NAME} producer')
    producer.run()
