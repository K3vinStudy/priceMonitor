from PySide6.QtCore import QThread, Qt, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QToolButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

import config
import main as app_main


class UrlSettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("高级设置")
        self.resize(560, 220)

        self.base_url_input = QTextEdit()
        self.first_url_input = QTextEdit()
        self.page_base_url_input = QTextEdit()
        self.base_url_input.setMinimumWidth(420)
        self.first_url_input.setMinimumWidth(420)
        self.page_base_url_input.setMinimumWidth(420)
        self.base_url_input.setFixedHeight(56)
        self.first_url_input.setFixedHeight(56)
        self.page_base_url_input.setFixedHeight(56)

        form_layout = QFormLayout()
        form_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        form_layout.setHorizontalSpacing(16)
        form_layout.setVerticalSpacing(14)
        form_layout.addRow("BASE_URL", self.base_url_input)
        form_layout.addRow("FIRST_URL", self.first_url_input)
        form_layout.addRow("PAGE_BASE_URL", self.page_base_url_input)

        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addLayout(form_layout)
        layout.addWidget(self.button_box)
        self.setLayout(layout)

        self.load_from_env()

    def _read_env_file(self) -> dict:
        config.ensure_env_file()
        env_map = dict(config.DEFAULT_ENV)

        if config.ENV_FILE.exists():
            for line in config.ENV_FILE.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                env_map[key.strip()] = value.strip()

        return env_map

    def load_from_env(self):
        env_map = self._read_env_file()
        self.base_url_input.setPlainText(env_map.get("BASE_URL", ""))
        self.first_url_input.setPlainText(env_map.get("FIRST_URL", ""))
        self.page_base_url_input.setPlainText(env_map.get("PAGE_BASE_URL", ""))

    def save_to_env(self):
        env_map = self._read_env_file()
        env_map["BASE_URL"] = self.base_url_input.toPlainText().strip()
        env_map["FIRST_URL"] = self.first_url_input.toPlainText().strip()
        env_map["PAGE_BASE_URL"] = self.page_base_url_input.toPlainText().strip()
        config.write_env_file(env_map)
        config.refresh_env_cache()


class FetchThread(QThread):
    log_signal = Signal(str)
    progress_signal = Signal(dict)
    error_signal = Signal(str)
    finished_signal = Signal()

    def __init__(self, rounds: int):
        super().__init__()
        self.rounds = rounds
        self._stop_requested = False

    def stop(self):
        self._stop_requested = True

    def run(self):
        try:
            app_main.setup_app(log=self.log_signal.emit)
            app_main.fetch(
                self.rounds,
                log=self.log_signal.emit,
                progress=self.progress_signal.emit,
                should_stop=lambda: self._stop_requested,
            )
        except Exception as e:
            self.error_signal.emit(str(e))
        finally:
            self.finished_signal.emit()


class TaskPage(QWidget):
    QWEN_MODELS = [
        "qwen-long",
        "qwen-long-latest",
        "qwen3.6-plus",
        "qwen3.5-plus",
        "qwen-plus",
    ]
    GPT_MODELS = [
        "gpt-5.2",
    ]

    def __init__(self):
        super().__init__()

        self.rounds_input = QSpinBox()
        self.rounds_input.setMinimum(1)
        self.rounds_input.setMaximum(999999)
        self.rounds_input.setValue(1)

        self.llm_workers_input = QSpinBox()
        self.llm_workers_input.setMinimum(1)
        self.llm_workers_input.setMaximum(128)
        self.llm_workers_input.setValue(8)

        self.timeout_input = QSpinBox()
        self.timeout_input.setMinimum(1)
        self.timeout_input.setMaximum(999999)
        self.timeout_input.setValue(600)

        self.db_batch_size_input = QSpinBox()
        self.db_batch_size_input.setMinimum(1)
        self.db_batch_size_input.setMaximum(10000)
        self.db_batch_size_input.setValue(50)

        self.llm_type_input = QComboBox()
        self.llm_type_input.addItem("千问", 1)
        self.llm_type_input.addItem("ChatGPT", 2)
        chatgpt_index = self.llm_type_input.count() - 1
        self.llm_type_input.model().item(chatgpt_index).setEnabled(False)

        self.model_type_input = QComboBox()
        self.qwen_model_name = "qwen3.5-plus"
        self.gpt_model_name = "gpt-5.2"

        self.qwen_api_key = ""
        self.gpt_api_key = ""
        self._current_llm_type = self.llm_type_input.currentData()

        self.api_key_input = QLineEdit()
        self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_input.setPlaceholderText("请输入 API Key")

        self.show_api_key_checkbox = QCheckBox("显示")
        
        self.advanced_settings_btn = QToolButton()
        self.advanced_settings_btn.setText("⚙")
        self.advanced_settings_btn.setToolTip("修改 BASE_URL / FIRST_URL / PAGE_BASE_URL")
        self.advanced_settings_btn.setFixedSize(32, 32)
        # self.advanced_settings_btn.setStyleSheet("font-size: 18px; font-weight: bold;")
        self.advanced_settings_btn.setStyleSheet("font-size: 24px; font-weight: bold; padding: 0px; margin: 0px;")      # 更大的齿轮按钮

        self.start_btn = QPushButton("开始任务")
        self.apply_settings_btn = QPushButton("应用设置")
        self.stop_btn = QPushButton("停止任务")
        self.stop_btn.setEnabled(False)

        self.status_label = QLabel("状态：未开始")
        self.current_gid_label = QLabel("当前进度：0% | gid 总数：0")
        self.stats_label = QLabel("成功：0（空结果：0） | 失败：0")

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)

        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        self.log_edit.setPlaceholderText("这里会显示运行日志...")

        self.refresh_llm_dependent_fields(self.llm_type_input.currentData())
        self._build_ui()
        self._bind_signals()
        self.show_api_key_checkbox.setChecked(False)
        self.load_settings_from_config()
        self.fetch_thread = None

    def _build_ui(self):
        config_group = QGroupBox("任务参数")

        left_form = QFormLayout()
        left_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        left_form.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        left_form.setHorizontalSpacing(16)
        left_form.setVerticalSpacing(12)
        left_form.addRow("rounds", self.rounds_input)
        left_form.addRow("LLM_WORKERS", self.llm_workers_input)
        left_form.addRow("LLM_TIMEOUT_SECONDS", self.timeout_input)

        right_form = QFormLayout()
        right_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        right_form.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        right_form.setHorizontalSpacing(16)
        right_form.setVerticalSpacing(12)
        right_form.addRow("DB_BATCH_SIZE", self.db_batch_size_input)
        right_form.addRow("LLM_TYPE", self.llm_type_input)
        right_form.addRow("MODEL_TYPE", self.model_type_input)

        api_key_row = QHBoxLayout()
        api_key_row.addWidget(self.api_key_input, 1)
        api_key_row.addWidget(self.show_api_key_checkbox)

        api_key_layout = QVBoxLayout()
        api_key_layout.setContentsMargins(0, 0, 0, 0)
        api_key_layout.setSpacing(6)
        api_key_layout.addWidget(QLabel("API_KEY"))
        api_key_layout.addLayout(api_key_row)

        forms_layout = QHBoxLayout()
        forms_layout.setContentsMargins(0, 0, 0, 0)
        forms_layout.setSpacing(32)
        forms_layout.addLayout(left_form, 1)
        forms_layout.addLayout(right_form, 1)

        config_layout = QVBoxLayout()
        config_layout.addLayout(forms_layout)
        config_layout.addSpacing(12)
        config_layout.addLayout(api_key_layout)
        config_group.setLayout(config_layout)

        control_group = QGroupBox("任务控制")
        control_header_layout = QHBoxLayout()
        control_header_layout.setContentsMargins(0, 0, 0, 0)
        control_header_layout.addStretch()
        control_header_layout.addWidget(self.advanced_settings_btn)

        control_layout = QVBoxLayout()
        control_layout.addLayout(control_header_layout)
        control_layout.addStretch()
        control_layout.addWidget(self.apply_settings_btn)
        control_layout.addSpacing(18)
        control_layout.addWidget(self.start_btn)
        control_layout.addWidget(self.stop_btn)
        control_layout.addStretch()
        control_group.setLayout(control_layout)
        control_group.setMaximumWidth(280)

        status_group = QGroupBox("运行状态")
        status_layout = QVBoxLayout()
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.current_gid_label)
        status_layout.addWidget(self.stats_label)
        status_layout.addWidget(self.progress_bar)
        status_group.setLayout(status_layout)

        log_group = QGroupBox("日志输出")
        log_layout = QVBoxLayout()
        log_layout.addWidget(self.log_edit)
        log_group.setLayout(log_layout)

        top_layout = QHBoxLayout()
        top_layout.addWidget(config_group, 3)
        top_layout.addWidget(control_group, 1)

        main_layout = QVBoxLayout()
        main_layout.addLayout(top_layout)
        main_layout.addWidget(status_group)
        main_layout.addWidget(log_group, 1)

        self.setLayout(main_layout)

    def _bind_signals(self):
        self.apply_settings_btn.clicked.connect(self.apply_settings)
        self.start_btn.clicked.connect(self.start_task)
        self.stop_btn.clicked.connect(self.stop_mock_task)
        self.show_api_key_checkbox.toggled.connect(self.toggle_api_key_visibility)
        self.llm_type_input.currentIndexChanged.connect(self.on_llm_type_changed)
        self.advanced_settings_btn.clicked.connect(self.open_url_settings_dialog)

    def open_url_settings_dialog(self):
        dialog = UrlSettingsDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            dialog.save_to_env()
            self.append_log("[CONFIG] 高级设置已应用")
            self.status_label.setText("状态：高级设置已应用")
    
    def on_llm_type_changed(self):
        previous_llm_type = self._current_llm_type
        current_text = self.api_key_input.text()
        current_model = self.model_type_input.currentText()

        if previous_llm_type == 1:
            self.qwen_api_key = current_text
            if current_model:
                self.qwen_model_name = current_model
        elif previous_llm_type == 2:
            self.gpt_api_key = current_text
            if current_model:
                self.gpt_model_name = current_model

        new_llm_type = self.llm_type_input.currentData()
        self.refresh_llm_dependent_fields(new_llm_type)
        self._current_llm_type = new_llm_type

    def refresh_llm_dependent_fields(self, llm_type: int):
        self.model_type_input.blockSignals(True)
        self.model_type_input.clear()

        if llm_type == 1:
            self.model_type_input.addItems(self.QWEN_MODELS)
            target_model = self.qwen_model_name or "qwen3.5-plus"
            target_index = self.model_type_input.findText(target_model)
            if target_index < 0:
                target_index = self.model_type_input.findText("qwen3.5-plus")
            if target_index >= 0:
                self.model_type_input.setCurrentIndex(target_index)
            self.api_key_input.setText(self.qwen_api_key)
        else:
            self.model_type_input.addItems(self.GPT_MODELS)
            target_model = self.gpt_model_name or "gpt-5.2"
            target_index = self.model_type_input.findText(target_model)
            if target_index < 0:
                target_index = 0
            self.model_type_input.setCurrentIndex(target_index)
            self.api_key_input.setText(self.gpt_api_key)

        self.model_type_input.blockSignals(False)

    def toggle_api_key_visibility(self, checked: bool):
        if checked:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)

    def _read_env_file(self) -> dict:
        config.ensure_env_file()
        env_map = dict(config.DEFAULT_ENV)

        if config.ENV_FILE.exists():
            for line in config.ENV_FILE.read_text(encoding="utf-8").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, value = stripped.split("=", 1)
                env_map[key.strip()] = value.strip()

        return env_map

    def load_settings_from_config(self):
        env_map = self._read_env_file()

        llm_type = int(env_map.get("LLM", "1"))
        self.llm_workers_input.setValue(int(env_map.get("LLM_WORKERS", "8")))
        self.timeout_input.setValue(int(env_map.get("LLM_TIMEOUT_SECONDS", "600")))
        self.db_batch_size_input.setValue(int(env_map.get("DB_BATCH_SIZE", "50")))

        self.qwen_api_key = env_map.get("Qwen_API_KEY", "")
        self.gpt_api_key = env_map.get("GPT_API_KEY", "")
        self.qwen_model_name = env_map.get("QWEN_MODEL", "qwen3.5-plus")
        self.gpt_model_name = env_map.get("GPT_MODEL", "gpt-5.2")

        llm_index = self.llm_type_input.findData(llm_type)
        if llm_index >= 0:
            self.llm_type_input.setCurrentIndex(llm_index)
        self._current_llm_type = self.llm_type_input.currentData()
        self.refresh_llm_dependent_fields(self._current_llm_type)

    def apply_settings(self):
        current_llm_type = self.llm_type_input.currentData()
        current_api_key = self.api_key_input.text()
        current_model = self.model_type_input.currentText()

        if current_llm_type == 1:
            self.qwen_api_key = current_api_key
            if current_model:
                self.qwen_model_name = current_model
        elif current_llm_type == 2:
            self.gpt_api_key = current_api_key
            if current_model:
                self.gpt_model_name = current_model

        env_map = self._read_env_file()
        env_map["LLM"] = str(current_llm_type)
        env_map["Qwen_API_KEY"] = self.qwen_api_key
        env_map["GPT_API_KEY"] = self.gpt_api_key
        env_map["QWEN_MODEL"] = self.qwen_model_name
        env_map["GPT_MODEL"] = self.gpt_model_name
        env_map["LLM_WORKERS"] = str(self.llm_workers_input.value())
        env_map["LLM_TIMEOUT_SECONDS"] = str(self.timeout_input.value())
        env_map["DB_BATCH_SIZE"] = str(self.db_batch_size_input.value())

        config.write_env_file(env_map)
        config.refresh_env_cache()
        self.append_log("[CONFIG] 设置已应用")
        self.status_label.setText("状态：设置已应用")

    def set_task_params_enabled(self, enabled: bool):
        widgets = [
            self.rounds_input,
            self.llm_workers_input,
            self.timeout_input,
            self.db_batch_size_input,
            self.llm_type_input,
            self.model_type_input,
            self.api_key_input,
            self.show_api_key_checkbox,
            self.apply_settings_btn,
            self.advanced_settings_btn,
        ]
        for widget in widgets:
            widget.setEnabled(enabled)

    def start_task(self):
        rounds = self.rounds_input.value()
        self.apply_settings()
        self.set_task_params_enabled(False)

        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.status_label.setText("状态：准备启动...")
        self.current_gid_label.setText("当前进度：0% | gid 总数：0")
        self.stats_label.setText("成功：0（空结果：0） | 失败：0")
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.log_edit.clear()

        llm_label = self.llm_type_input.currentText()
        llm_type = self.llm_type_input.currentData()
        model_name = self.model_type_input.currentText()
        api_key_preview = "已填写" if self.api_key_input.text().strip() else "未填写"

        self.append_log("[TASK] 开始任务")
        self.append_log(f"[CONFIG] LLM_TYPE={llm_label} ({llm_type})")
        self.append_log(f"[CONFIG] MODEL_TYPE={model_name}")
        self.append_log(f"[CONFIG] API_KEY={api_key_preview}")
        self.append_log(f"[CONFIG] rounds={rounds}")

        self.fetch_thread = FetchThread(rounds)
        self.fetch_thread.log_signal.connect(self.append_log)
        self.fetch_thread.progress_signal.connect(self.handle_progress)
        self.fetch_thread.error_signal.connect(self.handle_error)
        self.fetch_thread.finished_signal.connect(self.on_task_finished)
        self.fetch_thread.start()

    def handle_progress(self, payload: dict):
        stage = payload.get("stage")
        message = payload.get("message", "")
        total = payload.get("total", 0)
        done = payload.get("done", 0)
        success = payload.get("success", 0)
        empty = payload.get("empty", 0)
        failed = payload.get("failed", 0)
        current_gid = payload.get("current_gid")

        if message:
            self.status_label.setText(f"状态：{message}")

        if stage == "getting_gids":
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setFormat("正在批量获取 gid...")
        else:
            max_value = total if total > 0 else 1
            current_value = min(done, max_value)
            self.progress_bar.setRange(0, max_value)
            self.progress_bar.setValue(current_value)
            self.progress_bar.setFormat(f"{current_value}/{max_value}")
            self.progress_bar.update()

        if total > 0:
            percent = int((done / total) * 100)
        else:
            percent = 0
        self.current_gid_label.setText(f"当前进度：{percent}% | gid 总数：{total}")

        self.stats_label.setText(f"成功：{success}（空结果：{empty}） | 失败：{failed}")

    def handle_error(self, message: str):
        self.append_log(f"[ERROR] {message}")
        self.status_label.setText("状态：运行出错")

    def on_task_finished(self):
        self.set_task_params_enabled(True)
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setFormat("任务结束")
        total = self.progress_bar.maximum()
        if total > 0:
            self.current_gid_label.setText(f"当前进度：100% | gid 总数：{total}")
        if self.fetch_thread is not None:
            self.fetch_thread.deleteLater()
            self.fetch_thread = None

    def start_mock_task(self):
        self.append_log("[TASK] mock 方法已弃用，请使用 start_task")

    def stop_mock_task(self):
        if self.fetch_thread is not None:
            self.fetch_thread.stop()
            self.append_log("[TASK] 已发送停止信号，等待当前任务安全结束")
            self.status_label.setText("状态：正在停止...")
        else:
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.status_label.setText("状态：已停止")
            self.append_log("[TASK] 当前没有正在运行的任务")

    def append_log(self, message: str):
        self.log_edit.appendPlainText(message)