import sys
from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QApplication, QLabel, QVBoxLayout, QWidget
from confluent_kafka import Consumer, KafkaError

class KafkaConsumerThread(QThread):
    message_received = Signal(str)

    def __init__(self):
        super().__init__()

        # Kafka configuration
        bootstrap_servers = 'localhost:9092'
        group_id = 'test-group'
        topic = 'test'

        # Create Kafka Consumer instance
        conf = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
        self.consumer = Consumer(**conf)
        self.consumer.subscribe([topic])

    def run(self):
        while True:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(f'Error: {msg.error()}')
                    break

            message = msg.value().decode("utf-8")
            self.message_received.emit(message)

    def close(self):
        self.consumer.close()

class KafkaConsumerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka Consumer App")

        # Create UI
        self.label = QLabel("Waiting for messages...")
        self.layout = QVBoxLayout()
        self.layout.addWidget(self.label)
        self.setLayout(self.layout)

        # Start Kafka Consumer thread
        self.consumer_thread = KafkaConsumerThread()
        self.consumer_thread.message_received.connect(self.update_label)
        self.consumer_thread.start()

    def update_label(self, message):
        self.label.setText(message)

    def closeEvent(self, event):
        self.consumer_thread.close()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    consumer_app = KafkaConsumerApp()
    consumer_app.show()
    sys.exit(app.exec())