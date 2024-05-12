import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QPushButton, QLabel, QHBoxLayout
from PySide6.QtGui import QPainter, QColor, QBrush
from PySide6.QtCore import QThread, Signal, Qt
from confluent_kafka import Consumer, KafkaError

class Square(QWidget):
    def __init__(self, number, color=QColor("red")):
        super().__init__()
        self.color = color
        self.number = number  # Number for the square

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.fillRect(event.rect(), QBrush(self.color))
        # Draw the number in the center of the square
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
            'auto.offset.reset': 'earliest'
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

        consumer.close()

    def stop(self):
        self.running = False

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.bootstrap_servers = 'localhost:9092'
        self.group_id = 'test-group'
        self.color_topic = 'SquareColorViz'  # Topic for squares' colors visualization

        main_widget = QWidget()
        self.setCentralWidget(main_widget)

        layout = QHBoxLayout()  # Main layout is now a QHBoxLayout
        main_widget.setLayout(layout)

        # Left side menu
        left_menu_widget = QWidget()
        left_menu_layout = QVBoxLayout(left_menu_widget)
        left_menu_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(left_menu_widget)

        self.start_button = QPushButton("Start Kafka Consumer")
        self.putaway_button = QPushButton("Putaway")
        self.pickup_button = QPushButton("Pickup")
        self.location_transfer_button = QPushButton("Location Transfer")

        left_menu_layout.addWidget(self.start_button)
        left_menu_layout.addWidget(self.putaway_button)
        left_menu_layout.addWidget(self.pickup_button)
        left_menu_layout.addWidget(self.location_transfer_button)

        self.status_label = QLabel("Consumer Status: Not Started")
        left_menu_layout.addWidget(self.status_label)

        self.error_label = QLabel()
        left_menu_layout.addWidget(self.error_label)

        # Set fixed width for the left menu
        left_menu_width = 200
        left_menu_widget.setFixedWidth(left_menu_width)

        # Right side content
        right_content_widget = QWidget()
        right_content_layout = QVBoxLayout(right_content_widget)
        layout.addWidget(right_content_widget)

        # Create a widget to hold the grid layout and its content (card-like design)
        self.squares_container = QWidget()
        self.squares_container.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc; border-radius: 10px; padding: 10px;")
        self.squares_container.setFixedSize(500, 300)  # Set fixed size for the squares container
        self.squares_layout = QVBoxLayout(self.squares_container)
        self.squares_container.setLayout(self.squares_layout)

        right_content_layout.addWidget(self.squares_container, alignment=Qt.AlignTop | Qt.AlignLeft)

        # Create squares grid layout
        self.grid_layout = QGridLayout()
        self.squares = []  # Initialize the squares list
        self.create_squares_grid_layout()  # Populate the grid with squares

        self.squares_layout.addLayout(self.grid_layout)

        # Initialize Kafka consumer for squares' colors visualization
        self.kafka_color_consumer = KafkaSquareConsumer(self.bootstrap_servers, self.group_id, self.color_topic)
        self.kafka_color_consumer.message_received.connect(self.update_colors)  # Connect the signal to the slot
        self.kafka_color_consumer.error_occurred.connect(self.display_error)

        # Connect buttons to slots
        self.start_button.clicked.connect(self.start_consumer)
        self.putaway_button.clicked.connect(lambda: self.print_button_clicked("Putaway"))
        self.pickup_button.clicked.connect(lambda: self.print_button_clicked("Pickup"))
        self.location_transfer_button.clicked.connect(lambda: self.print_button_clicked("Location Transfer"))

        # Navigation buttons
        self.prev_rack_button = QPushButton("Previous Rack")
        self.next_rack_button = QPushButton("Next Rack")

        left_menu_layout.addWidget(self.prev_rack_button)
        left_menu_layout.addWidget(self.next_rack_button)

        # Connect navigation buttons to slots
        self.prev_rack_button.clicked.connect(self.move_to_previous_rack)
        self.next_rack_button.clicked.connect(self.move_to_next_rack)

        self.current_rack_index = 0
        self.racks = ['A', 'B', 'C', 'D']  # List of rack names
        self.rack_colors = {}  # Dictionary to store colors for each rack

    def start_consumer(self):
        if not self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.start()
            self.status_label.setText("Consumer Status: Running")
            self.start_button.setEnabled(False)

    def update_colors(self, message):
        rack_name = message[0]  # Extract the first character to identify the rack
        colors_str = message[1:]  # Extract the rest of the message as colors string
        colors = colors_str.split(",")
        print("Rack Name:", rack_name)
        print("Colors:", colors)
        self.rack_colors[rack_name] = colors  # Store colors for the received rack
        if self.racks[self.current_rack_index] == rack_name:
            self.update_squares_container()  # Update squares if the current rack matches the received rack


    def display_error(self, error_message):
        self.error_label.setText(error_message)

    def print_button_clicked(self, button_text):
        print(f"{button_text} button clicked")

    def closeEvent(self, event):
        if self.kafka_color_consumer.isRunning():
            self.kafka_color_consumer.stop()
            self.kafka_color_consumer.wait()

        event.accept()

    def create_squares_grid_layout(self):
        for level in range(3, -1, -1):  # Iterate over levels from L3 to L0
            for col in range(8):  # Iterate over locations from 1 to 8
                rack_name = 'A'  # Assuming rack name is 'A'
                location_number = col + 1
                square_number = f"{rack_name}{level}{location_number}"
                square = Square(square_number)  # Pass the square number to the Square constructor
                square.setFixedSize(50, 50)
                row = 3 - level  # Calculate row based on level (L3 = row 0, L2 = row 1, ...)
                self.grid_layout.addWidget(square, row, col)
                self.squares.append(square)  # Add squares to the list


    def move_to_previous_rack(self):
        self.current_rack_index -= 1
        if self.current_rack_index < 0:
            self.current_rack_index = len(self.racks) - 1
        self.update_squares_container()

    def move_to_next_rack(self):
        self.current_rack_index += 1
        if self.current_rack_index >= len(self.racks):
            self.current_rack_index = 0
        self.update_squares_container()

    def update_squares_container(self):
        layout = self.squares_layout
        for i in reversed(range(layout.count())):
            item = layout.itemAt(i)
            if item is not None and item.widget() is not None:
                item.widget().setParent(None)

        rack_name = self.racks[self.current_rack_index]
        colors_str = self.rack_colors.get(rack_name, "0" * 32)  # Get colors for the current rack
        print(colors_str)
        colors = ["red" if digit == '0' else "green" for digit in colors_str[0]]
        print("clr : ", colors)
        self.squares.clear()  # Clear the squares list

        # Add rack label
        rack_label = QLabel(f"Current Rack: {rack_name}")
        layout.addWidget(rack_label)

        self.create_squares_grid_layout()  # Repopulate the grid with squares
        for square, color in zip(self.squares, colors):
            square.set_color(QColor(color))




app = QApplication(sys.argv)
window = MainWindow()
window.show()
sys.exit(app.exec())