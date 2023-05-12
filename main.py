from producer import Producer, Consumer

import threading
import time

def main():
    username=''
    password=''
    host='localhost'
    port=9092
    topic='test'
    message='{"id":"fffctfct"}'

    producer = Producer(
        username,
        password,
        host,
        port,
        ''
    )

    consumer = Consumer(
        username,
        password,
        host,
        port
    )

    build_and_run_threads(consumer, message, producer, topic)

    while True:
        time.sleep(10)


def build_and_run_threads(consumer, message, producer, topic):
    print("building threads")
    sub_thread = threading.Thread(target=subscribe, args=(topic, consumer))
    publish_thread = threading.Thread(target=publish, args=(topic, message, producer))

    print('starting threads')
    sub_thread.start()
    publish_thread.start()


def publish(topic, msg, producer):
    time.sleep(3)
    producer.publish(topic, msg)

def subscribe(topic, consumer):
    consumer.subscribe(topic, print)

main()
