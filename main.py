import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QPushButton, QLabel, QHBoxLayout, QStackedWidget, QScrollArea
from PySide6.QtGui import QPainter, QColor, QBrush
from PySide6.QtCore import QThread, Signal, Qt
from confluent_kafka import Consumer, KafkaError, Producer
# this is a cmnt
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

class KafkaLogProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def produce_log(self, message):
        self.producer.produce(self.topic, message.encode('utf-8'))
        self.producer.flush()

class HomePage(QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window

        layout = QVBoxLayout(self)

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
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.pallet_id = None
        self.location_id = None

        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)  # Centralize the layout
        
        self.pallet_label = QLabel("Scanning Pallet...")
        self.pallet_label.setAlignment(Qt.AlignCenter)
        self.pallet_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
        self.pallet_label.setFixedSize(250, 50)
        layout.addWidget(self.pallet_label)

        self.location_label = QLabel("Scanning Location...")
        self.location_label.setAlignment(Qt.AlignCenter)
        self.location_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
        self.location_label.setFixedSize(250, 50)
        layout.addWidget(self.location_label)

        self.submit_button = QPushButton("Submit")
        self.submit_button.setEnabled(False)
        layout.addWidget(self.submit_button)

        self.pallet_label.mousePressEvent = self.scan_pallet
        self.location_label.mousePressEvent = self.scan_location
        self.submit_button.clicked.connect(self.submit_ids)

    def scan_pallet(self, event):
        self.pallet_id = "PAL009"  # Replace this with actual scanning logic
        self.pallet_label.setText(self.pallet_id)
        self.pallet_label.setStyleSheet("QLabel { background-color : lightgreen; border: 1px solid black; }")
        self.check_ready_to_submit()

    def scan_location(self, event):
        self.location_id = "B04"  # Replace this with actual scanning logic
        self.location_label.setText(self.location_id)
        self.location_label.setStyleSheet("QLabel { background-color : lightgreen; border: 1px solid black; }")
        self.check_ready_to_submit()

    def check_ready_to_submit(self):
        if self.pallet_id and self.location_id:
            self.submit_button.setEnabled(True)

    def submit_ids(self):
        if self.pallet_id and self.location_id:
            log_message = f"Putaway - Pallet: {self.pallet_id}, Location: {self.location_id}"
            self.main_window.kafka_log_producer.produce_log(log_message)
            self.main_window.home_page.update_log_container(log_message)
            # Reset the labels and IDs
            self.pallet_id = None
            self.location_id = None
            self.pallet_label.setText("Scanning Pallet...")
            self.pallet_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
            self.location_label.setText("Scanning Location...")
            self.location_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
            self.submit_button.setEnabled(False)


class PickupPage(QWidget):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        label = QLabel("Pickup Page")
        layout.addWidget(label)

class LocationTransferPage(QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.sourceLocation_id = None
        self.distinationLocation_id = None

        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)  # Centralize the layout
        
        self.sourceLocation_label = QLabel("Scanning Source Location...")
        self.sourceLocation_label.setAlignment(Qt.AlignCenter)
        self.sourceLocation_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
        self.sourceLocation_label.setFixedSize(350, 50)
        layout.addWidget(self.sourceLocation_label)

        self.distinationLocation_label = QLabel("Scanning Distination Location...")
        self.distinationLocation_label.setAlignment(Qt.AlignCenter)
        self.distinationLocation_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
        self.distinationLocation_label.setFixedSize(350, 50)
        layout.addWidget(self.distinationLocation_label)

        self.submit_button = QPushButton("Submit")
        self.submit_button.setEnabled(False)
        layout.addWidget(self.submit_button)

        self.sourceLocation_label.mousePressEvent = self.scan_sourceLocation
        self.distinationLocation_label.mousePressEvent = self.scan_distinationLocation
        self.submit_button.clicked.connect(self.submit_ids)

    def scan_sourceLocation(self, event):
        self.sourceLocation_id = "A09"  # Replace this with actual scanning logic
        self.sourceLocation_label.setText(self.sourceLocation_id)
        self.sourceLocation_label.setStyleSheet("QLabel { background-color : lightgreen; border: 1px solid black; }")
        self.check_ready_to_submit()

    def scan_distinationLocation(self, event):
        self.distinationLocation_id = "B04"  # Replace this with actual scanning logic
        self.distinationLocation_label.setText(self.distinationLocation_id)
        self.distinationLocation_label.setStyleSheet("QLabel { background-color : lightgreen; border: 1px solid black; }")
        self.check_ready_to_submit()

    def check_ready_to_submit(self):
        if self.sourceLocation_id and self.distinationLocation_id:
            self.submit_button.setEnabled(True)

    def submit_ids(self):
        if self.sourceLocation_id and self.distinationLocation_id:
            log_message = f"Location Transfer - Source: {self.sourceLocation_id}, Distination: {self.distinationLocation_id}"
            self.main_window.kafka_log_producer.produce_log(log_message)
            self.main_window.home_page.update_log_container(log_message)
            # Reset the labels and IDs
            self.sourceLocation_id = None
            self.distinationLocation_id = None
            self.sourceLocation_label.setText("Scan Source Location...")
            self.sourceLocation_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
            self.distinationLocation_label.setText("Scan Distination Location...")
            self.distinationLocation_label.setStyleSheet("QLabel { background-color : lightgray; border: 1px solid black; }")
            self.submit_button.setEnabled(False)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.racks = ['A', 'B', 'C', 'D']
        self.current_rack_index = 0
        self.rack_colors = {}

        self.bootstrap_servers = 'localhost:9092'
        self.group_id = 'test-group'
        self.color_topic = 'SquareColorViz'
        self.log_topic = 'LogForkliftEvents'

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
        self.putaway_page = PutawayPage(self)  # Pass MainWindow reference
        self.pickup_page = PickupPage()
        self.location_transfer_page = LocationTransferPage(self)

        self.stacked_widget.addWidget(self.home_page)
        self.stacked_widget.addWidget(self.putaway_page)
        self.stacked_widget.addWidget(self.pickup_page)
        self.stacked_widget.addWidget(self.location_transfer_page)

        self.home_button.clicked.connect(lambda: self.switch_page("Home"))
        self.putaway_button.clicked.connect(lambda: self.switch_page("Putaway"))
        self.pickup_button.clicked.connect(lambda: self.switch_page("Pickup"))
        self.location_transfer_button.clicked.connect(lambda: self.switch_page("Location Transfer"))

        self.kafka_color_consumer = KafkaSquareConsumer(self.bootstrap_servers, self.group_id, self.color_topic)
        self.kafka_color_consumer.message_received.connect(self.update_colors)
        self.kafka_color_consumer.error_occurred.connect(self.display_error)

        self.kafka_log_producer = KafkaLogProducer(self.bootstrap_servers, self.log_topic)

        # Start the consumer automatically
        self.start_consumer()

    def start_consumer(self):
        if not self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.start()
            self.status_label.setText("Consumer Status: Running")

    def switch_page(self, page_name):
        if page_name == "Home":
            self.stacked_widget.setCurrentWidget(self.home_page)
        elif page_name == "Putaway":
            self.stacked_widget.setCurrentWidget(self.putaway_page)
        elif page_name == "Pickup":
            self.stacked_widget.setCurrentWidget(self.pickup_page)
        elif page_name == "Location Transfer":
            self.stacked_widget.setCurrentWidget(self.location_transfer_page)

        log_message = f"{page_name} button clicked"
        self.home_page.update_log_container(log_message)
        self.kafka_log_producer.produce_log(log_message)  # Send log message immediately

    def update_colors(self, message):
        rack_name = message[0]
        colors_str = message[1:]
        self.rack_colors[rack_name] = colors_str
        if self.racks[self.current_rack_index] == rack_name:
            self.home_page.update_squares_container(rack_name, colors_str)

    def display_error(self, error_message):
        self.error_label.setText(error_message)

    def closeEvent(self, event):
        if self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.stop()
            self.kafka_color_consumer.wait()

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
