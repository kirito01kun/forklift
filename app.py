import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QPushButton, QLabel, QHBoxLayout, QStackedWidget, QScrollArea, QTextEdit
from PySide6.QtGui import QPainter, QColor, QBrush
from PySide6.QtCore import QThread, Signal, Qt
from confluent_kafka import Consumer, KafkaError

class Square(QWidget):
    def __init__(self, number, color=QColor("red")):
        super().__init__()
        self.color = color
        self.number = number

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(event.rect(), QBrush(self.color))
        painter.drawText(event.rect(), Qt.AlignCenter, str(self.number))

    def set_color(self, color):
        self.color = color
        self.update()

class KafkaSquareConsumer(QThread):
    message_received = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, bootstrap_servers, group_id, topic):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.running = False

    def run(self):
        self.running = True
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 10000
        })
        consumer.subscribe([self.topic])

        while self.running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.error_occurred.emit(f'Error: {msg.error()}')
                    break

            message = msg.value().decode("utf-8")
            self.message_received.emit(message)

            consumer.commit(msg)

        consumer.close()

    def stop(self):
        self.running = False

class KafkaLogConsumer(QThread):
    log_received = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, bootstrap_servers, group_id, topic):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.running = False

    def run(self):
        self.running = True
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 10000
        })
        consumer.subscribe([self.topic])

        while self.running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.error_occurred.emit(f'Error: {msg.error()}')
                    break

            message = msg.value().decode("utf-8")
            self.log_received.emit(message)

            consumer.commit(msg)

        consumer.close()

    def stop(self):
        self.running = False

class HomePage(QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window

        layout = QVBoxLayout(self)

        # Start Kafka Consumer Button
        self.start_button = QPushButton("Start Kafka Consumer")
        layout.addWidget(self.start_button)
        self.start_button.clicked.connect(self.main_window.start_consumer)

        self.squares_container = QWidget()
        self.squares_container.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc; border-radius: 10px; padding: 10px;")
        self.squares_container.setFixedSize(500, 300)
        self.squares_layout = QVBoxLayout(self.squares_container)
        self.squares_container.setLayout(self.squares_layout)

        layout.addWidget(self.squares_container, alignment=Qt.AlignTop | Qt.AlignLeft)

        self.grid_layout = QGridLayout()
        self.squares = []

        self.create_squares_grid_layout(self.main_window.racks[self.main_window.current_rack_index])
        self.squares_layout.addLayout(self.grid_layout)

        # Add navigation buttons
        navigation_layout = QHBoxLayout()
        self.prev_rack_button = QPushButton("Previous Rack")
        self.next_rack_button = QPushButton("Next Rack")
        navigation_layout.addWidget(self.prev_rack_button)
        navigation_layout.addWidget(self.next_rack_button)
        layout.addLayout(navigation_layout)

        # Connect buttons to slots in main_window
        self.prev_rack_button.clicked.connect(self.main_window.move_to_previous_rack)
        self.next_rack_button.clicked.connect(self.main_window.move_to_next_rack)

        # Log container
        self.log_container = QScrollArea()
        self.log_container.setWidgetResizable(True)
        self.log_widget = QWidget()
        self.log_layout = QVBoxLayout(self.log_widget)
        self.log_container.setWidget(self.log_widget)

        # Add dummy logs
        for i in range(20):
            log_label = QLabel(f"Log entry {i+1}")
            self.log_layout.addWidget(log_label)

        layout.addWidget(self.log_container)

    def create_squares_grid_layout(self, rack_name):
        for level in range(3, -1, -1):
            for col in range(8):
                location_number = col + 1
                square_number = f"{rack_name}{level}{location_number}"
                square = Square(square_number)
                square.setFixedSize(50, 50)
                row = 3 - level
                self.grid_layout.addWidget(square, row, col)
                self.squares.append(square)

    def update_squares_container(self, rack_name, colors_str):
        colors = ["red" if digit == '0' else "green" for digit in colors_str]
        self.squares.clear()

        for i in reversed(range(self.squares_layout.count())):
            item = self.squares_layout.itemAt(i)
            if item is not None and item.widget() is not None:
                item.widget().setParent(None)

        rack_label = QLabel(f"Current Rack: {rack_name}")
        self.squares_layout.addWidget(rack_label)

        self.create_squares_grid_layout(rack_name)
        for square, color in zip(self.squares, colors):
            square.set_color(QColor(color))

    def update_log_container(self, log_message):
        log_label = QLabel(log_message)
        self.log_layout.addWidget(log_label)

class PutawayPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        label = QLabel("Putaway Page")
        layout.addWidget(label)

class PickupPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        label = QLabel("Pickup Page")
        layout.addWidget(label)

class LocationTransferPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        label = QLabel("Location Transfer Page")
        layout.addWidget(label)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.racks = ['A', 'B', 'C', 'D']
        self.current_rack_index = 0
        self.rack_colors = {}

        self.bootstrap_servers = 'localhost:9092'
        self.group_id = 'test-group'
        self.color_topic = 'SquareColorViz'
        self.log_topic = 'SquareLogTopic'

        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        layout = QHBoxLayout(main_widget)

        left_menu_widget = QWidget()
        left_menu_layout = QVBoxLayout(left_menu_widget)
        left_menu_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(left_menu_widget)

        self.home_button = QPushButton("Home")
        self.putaway_button = QPushButton("Putaway")
        self.pickup_button = QPushButton("Pickup")
        self.location_transfer_button = QPushButton("Location Transfer")

        left_menu_layout.addWidget(self.home_button)
        left_menu_layout.addWidget(self.putaway_button)
        left_menu_layout.addWidget(self.pickup_button)
        left_menu_layout.addWidget(self.location_transfer_button)

        self.status_label = QLabel("Consumer Status: Not Started")
        left_menu_layout.addWidget(self.status_label)

        self.error_label = QLabel()
        left_menu_layout.addWidget(self.error_label)

        left_menu_widget.setFixedWidth(200)

        self.stacked_widget = QStackedWidget()
        layout.addWidget(self.stacked_widget)

        self.home_page = HomePage(self)
        self.putaway_page = PutawayPage()
        self.pickup_page = PickupPage()
        self.location_transfer_page = LocationTransferPage()

        self.stacked_widget.addWidget(self.home_page)
        self.stacked_widget.addWidget(self.putaway_page)
        self.stacked_widget.addWidget(self.pickup_page)
        self.stacked_widget.addWidget(self.location_transfer_page)

        self.home_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.home_page))
        self.putaway_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.putaway_page))
        self.pickup_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.pickup_page))
        self.location_transfer_button.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.location_transfer_page))

        self.kafka_color_consumer = KafkaSquareConsumer(self.bootstrap_servers, self.group_id, self.color_topic)
        self.kafka_color_consumer.message_received.connect(self.update_colors)
        self.kafka_color_consumer.error_occurred.connect(self.display_error)

        self.kafka_log_consumer = KafkaLogConsumer(self.bootstrap_servers, self.group_id, self.log_topic)
        self.kafka_log_consumer.log_received.connect(self.update_logs)
        self.kafka_log_consumer.error_occurred.connect(self.display_error)

    def start_consumer(self):
        if not self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.start()
            self.status_label.setText("Consumer Status: Running")
            self.home_page.start_button.setEnabled(False)
        
        if not self.kafka_log_consumer.isRunning():
            self.kafka_log_consumer.start()
            self.status_label.setText("Consumer Status: Running")
            self.home_page.start_button.setEnabled(False)

    def update_colors(self, message):
        rack_name = message[0]
        colors_str = message[1:]
        self.rack_colors[rack_name] = colors_str
        if self.racks[self.current_rack_index] == rack_name:
            self.home_page.update_squares_container(rack_name, colors_str)

    def update_logs(self, log_message):
        self.home_page.update_log_container(log_message)

    def display_error(self, error_message):
        self.error_label.setText(error_message)

    def closeEvent(self, event):
        if self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.stop()
            self.kafka_color_consumer.wait()
        
        if self.kafka_log_consumer.isRunning():
            self.kafka_log_consumer.stop()
            self.kafka_log_consumer.wait()

        event.accept()

    def move_to_previous_rack(self):
        self.current_rack_index -= 1
        if self.current_rack_index < 0:
            self.current_rack_index = len(self.racks) - 1
        self.home_page.update_squares_container(self.racks[self.current_rack_index], self.rack_colors.get(self.racks[self.current_rack_index], "0" * 32))

    def move_to_next_rack(self):
        self.current_rack_index += 1
        if self.current_rack_index >= len(self.racks):
            self.current_rack_index = 0
        self.home_page.update_squares_container(self.racks[self.current_rack_index], self.rack_colors.get(self.racks[self.current_rack_index], "0" * 32))

app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec())