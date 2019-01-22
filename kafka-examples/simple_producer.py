import arrow
import time
from kafka import KafkaProducer


class SimpleProducer:

    KAFKA_TOPIC_NAME = 'simple_kafka'

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def push_data(self, data):
        print(f'Pushing new data to kafka: {data}')
        self.producer.send(self.KAFKA_TOPIC_NAME, data.encode('utf-8'))

    def run(self):
        while True:
            data = f'Sending new data at {arrow.utcnow()}'
            self.push_data(data)
            time.sleep(5)


if __name__ == '__main__':
    producer = SimpleProducer()
    print(f'Starting the {producer.KAFKA_TOPIC_NAME} producer')
    producer.run()
