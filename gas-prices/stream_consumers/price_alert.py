import json

from kafka import KafkaConsumer


class AlertsConsumer:

    KAFKA_TOPIC_NAME = 'price_alert'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, bootstrap_servers=['localhost:9092']
        )

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            data = json.loads(message.value)
            print(f'Received new data: {data}')


if __name__ == '__main__':
    consumer = AlertsConsumer()
    consumer.run()
