from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def produce_message(producer, topic, message):
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.poll(0)

def main():
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'SquareColorViz'

    # Create Kafka Producer instance
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(**conf)
#kafka-topics.sh --create --topic SquareColorViz --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

    # Produce messages
    try:
        while True:
            message = input("Enter message (or 'exit' to quit): ")
            if message.lower() == 'exit':
                break
            produce_message(producer, topic, message)
    except KeyboardInterrupt:
        pass

    # Flush and close producer
    producer.flush()
    producer.close()

if __name__ == '__main__':
    main()
