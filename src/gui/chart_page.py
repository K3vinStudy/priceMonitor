from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget


class ChartPage(QWidget):
    def __init__(self):
        super().__init__()

        label = QLabel("图表页面开发中")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        layout = QVBoxLayout()
        layout.addWidget(label)

        self.setLayout(layout)