from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from gui.task_page import TaskPage
from gui.chart_page import ChartPage


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("汽车价格监测系统")
        self.resize(1200, 800)

        self.task_page = TaskPage()
        self.chart_page = ChartPage()

        self.stack = QStackedWidget()
        self.stack.addWidget(self.task_page)
        self.stack.addWidget(self.chart_page)

        self.task_btn = QPushButton("任务")
        self.chart_btn = QPushButton("图表")

        for btn in (self.task_btn, self.chart_btn):
            btn.setMinimumHeight(48)
            btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        self.task_btn.clicked.connect(lambda: self.switch_page(0))
        self.chart_btn.clicked.connect(lambda: self.switch_page(1))

        nav_layout = QVBoxLayout()
        nav_layout.addWidget(self.task_btn)
        nav_layout.addWidget(self.chart_btn)
        nav_layout.addStretch()

        nav_widget = QWidget()
        nav_widget.setLayout(nav_layout)
        nav_widget.setFixedWidth(120)

        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(0)
        content_layout.addWidget(nav_widget)
        content_layout.addWidget(self.stack, 1)

        content_widget = QWidget()
        content_widget.setLayout(content_layout)

        self.setCentralWidget(content_widget)

        self.switch_page(0)

    def switch_page(self, index: int):
        self.stack.setCurrentIndex(index)

        buttons = [self.task_btn, self.chart_btn]
        for i, btn in enumerate(buttons):
            if i == index:
                btn.setEnabled(False)
            else:
                btn.setEnabled(True)