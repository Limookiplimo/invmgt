from confluent_kafka import Consumer, KafkaError

def consume_topic(topic):
    bootstrap_servers = 'localhost:9092'
    group_id = 'flink_consumer_group'
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.partition()}")
                else:
                    print(f"Error while consuming: {msg.error()}")
            else:
                print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Interrupted, closing consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    topic_name = 'orders'
    consume_topic(topic_name)
