from kafka import KafkaConsumer


class SimpleConsumer:

    KAFKA_TOPIC_NAME = 'simple_kafka'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, bootstrap_servers=['localhost:9092']
        )

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            print(f'Received new data: {message.value}')


if __name__ == '__main__':
    consumer = SimpleConsumer()
    consumer.run()
