""" Delivery consumer prints the delivery informations.
"""

import json
import confluent_kafka as ck 

from time import sleep


conf = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'reddit.app', 
    'auto.offset.reset': 'smallest'
}




def from_message(msg: ck.Message):
        payload = msg.value().decode('utf-8')
        order = json.loads(payload)
        print(order)


if __name__ == '__main__':
    consumer = ck.Consumer(conf)
    topics = ['reddit_post']
    running = True

    try: 
        consumer.subscribe(topics)
        while running:
            print('Iteration started.\nFetching a message...')
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                print('no messages.')
                continue
            if msg.error():
                if msg.error().code() == ck.KafkaError._PARTITION_EOF:
                    print(f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n")
                elif msg.error():
                    raise ck.KafkaException(msg.error())
            else:
                from_message(msg)
            sleep(3)
    finally:
        consumer.close()