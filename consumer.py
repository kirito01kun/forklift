from confluent_kafka import Consumer, KafkaError

def consume_messages(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                continue
            else:
                print(f'Error: {msg.error()}')
                break

        print(f'Received message: {msg.value().decode("utf-8")}')

    consumer.close()

def main():
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    group_id = 'test-group'
    topic = 'LogForkliftEvents'

    # Create Kafka Consumer instance
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'  # Start reading at the earliest message in the topic
    }
    consumer = Consumer(**conf)

    # Consume messages
    consume_messages(consumer, topic)

if __name__ == '__main__':
    main()