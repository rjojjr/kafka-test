from kafka3 import KafkaProducer, KafkaConsumer
import json

class Producer:
    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            port: int,
            client_id: str
    ):
        self.producer = KafkaProducer(
            bootstrap_servers="{}:{}".format(host, port),
            sasl_plain_username=username,
            sasl_plain_password=password,
            client_id = client_id,
            value_serializer=lambda v: v.encode('utf-8')
        )

    def publish(self, topic: str, payload: str):
        print("publishing {} to {}".format(payload, topic))
        self.producer.send(topic, payload)

class Consumer:
    def __init__(
            self,
            username: str,
            password: str,
            host: str,
            port: int
    ):
        self.consumer = KafkaConsumer(
            bootstrap_servers="{}:{}".format(host, port),
            sasl_plain_username=username,
            sasl_plain_password=password
        )

    def subscribe(self, topic: str, onMessageReceived):
        print("subscribing to {}".format(topic))
        self.consumer.subscribe(topics=[topic])
        for message in self.consumer:
            print('received message from topic {}'.format(topic))
            onMessageReceived(message)
