import sys
import time

from PyQt5.QtCore import QObject, pyqtSignal, QThread
from PyQt5.QtWidgets import QApplication, QLabel, QGridLayout, \
    QWidget, QComboBox, QPushButton, QFileDialog, QProgressBar


class Worker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal(int)

    def __init__(self, func):
        super().__init__()
        self.func = func

    def run(self):
        self.func()
        self.finished.emit()

    def setWorkerValue(self, value):
        self.progress.emit(value)


class GUI(QWidget):
    def __init__(self):
        super().__init__()
        self.layout = QGridLayout()
        self.appTitle = 'SHHLogFeaturesCounter'
        self.left = 500
        self.top = 300
        self.width = 450
        self.height = 150

        self.logPath = ''

        self.filePath = ''

        self.start = None

        self.init()

    def init(self):
        self.setLayout(self.layout)
        self.layout.setRowStretch(4, 2)
        self.setWindowTitle(self.appTitle)
        self.setGeometry(self.left, self.top,
                         self.width, self.height)

        self.setDBComboBox()
        self.setInstanceComboBox()
        self.setChooseLogButton()
        self.setChooseFileButton()
        self.setProgressBar()
        self.setStartButton()

        self.show()

    def setDBComboBox(self):
        def on_combobox_changed(value):
            self.database = value

        db_combo = QComboBox()
        db_combo.addItems(['DB1', 'DB2', 'DB3', 'DB4', 'DB5', 'DB6'])
        self.layout.addWidget(QLabel('Wybierz bazę danych:'), 0, 0)
        self.layout.addWidget(db_combo, 1, 0)
        self.database = db_combo.currentText()

        db_combo.currentTextChanged.connect(on_combobox_changed)

    def setInstanceComboBox(self):
        def on_combobox_changed(value):
            self.instance = value

        instance_combo = QComboBox()
        instance_combo.addItems(['1', '2'])

        self.layout.addWidget(QLabel('Wybierz instancję bazy danych:'), 0, 1)
        self.layout.addWidget(instance_combo, 1, 1)
        self.instance = instance_combo.currentText()

        instance_combo.currentTextChanged.connect(on_combobox_changed)

    def get_logPath(self):
        self.logPath = QFileDialog.getExistingDirectory(
            self, "Wybierz inny folder logów")

    def setChooseLogButton(self):
        btn = QPushButton('Zmienić folder z logami?')
        self.layout.addWidget(btn, 2, 0)
        btn.clicked.connect(self.get_logPath)

    def get_filePath(self):
        self.filePath = QFileDialog.getExistingDirectory(
            self, "Wybierz inny folder logów")

    def setChooseFileButton(self):
        btn = QPushButton('Zmienić folder z plikami?')
        self.layout.addWidget(btn, 3, 0)
        btn.clicked.connect(self.get_filePath)

    def setProgressBar(self):
        self.pbar = QProgressBar()
        self.layout.addWidget(self.pbar, 2, 2)

    def setProgressBarValue(self, value):
        self.pbar.setValue(value)

    def execute(self, func):
        self.start.clicked.connect(lambda: self.runTask(func))

    def runTask(self, func):
        self.thread = QThread()
        self.worker = Worker(func)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.progress.connect(self.setProgressBarValue)

        self.thread.start()
        self.start.setEnabled(False)
        self.thread.finished.connect(
            lambda: self.start.setEnabled(True)
        )

    def setStartButton(self):
        self.start = QPushButton('Start')
        self.layout.addWidget(self.start, 3, 2)
