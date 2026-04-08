from collections import Counter, defaultdict
import inspect
import re
from typing import Any, Iterable

from PySide6.QtCore import QTimer, Qt, QUrl
from PySide6.QtGui import QDesktopServices, QWheelEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from matplotlib import rcParams
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

from database.op import (
    count_distinct_gids,
    count_price_records,
    count_price_records_by_gid,
    list_distinct_series,
    list_price_records,
)


rcParams["font.family"] = "sans-serif"
rcParams["font.sans-serif"] = [
    "PingFang SC",
    "Hiragino Sans GB",
    "Microsoft YaHei",
    "Noto Sans CJK SC",
    "WenQuanYi Zen Hei",
    "Arial Unicode MS",
    "SimHei",
    "DejaVu Sans",
]
rcParams["axes.unicode_minus"] = False


class StatCard(QFrame):
    def __init__(self, title: str, value: str = "-"):
        super().__init__()
        self.setFrameShape(QFrame.Shape.StyledPanel)

        self.title_label = QLabel(title)
        self.value_label = QLabel(value)
        self.value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.value_label.setStyleSheet("font-size: 22px; font-weight: bold;")

        layout = QVBoxLayout()
        layout.addWidget(self.title_label)
        layout.addStretch()
        layout.addWidget(self.value_label)
        layout.addStretch()
        self.setLayout(layout)

    def set_value(self, value: str):
        self.value_label.setText(value)


class AnimatedMplCanvas(FigureCanvas):
    def __init__(self, parent=None, wheel_callback=None):
        self.figure = Figure(figsize=(6, 3.8), dpi=100)
        self.figure.patch.set_facecolor("white")
        self.axes = self.figure.add_subplot(111)
        self.axes.set_facecolor("white")
        super().__init__(self.figure)
        if parent is not None:
            self.setParent(parent)
        self.setMinimumHeight(340)
        self.setMinimumWidth(520)
        self.wheel_callback = wheel_callback

    def wheelEvent(self, event: QWheelEvent):
        if self.wheel_callback is None:
            super().wheelEvent(event)
            return

        delta_x = event.angleDelta().x()
        delta_y = event.angleDelta().y()
        delta = delta_x if delta_x != 0 else delta_y
        if delta != 0:
            self.wheel_callback(delta)
            event.accept()
            return

        super().wheelEvent(event)


class ChartPage(QWidget):
    def __init__(self):
        super().__init__()

        self.refresh_btn = QPushButton("刷新图表")
        self.series_filter_combo = QComboBox()
        self.series_filter_combo.addItem("全部")
        self.series_filter_combo.setMinimumWidth(180)
        self.status_label = QLabel("状态：正在加载数据库数据")
        self.data_count_label = QLabel("共 0 条数据")

        self.total_records_card = StatCard("总记录数")
        self.total_gid_card = StatCard("讨论帖数量")
        self.avg_price_card = StatCard("平均价格")
        self.latest_date_card = StatCard("最新日期")

        self.trend_canvas = AnimatedMplCanvas(wheel_callback=self._scroll_trend_chart)
        self.series_canvas = AnimatedMplCanvas(wheel_callback=self._scroll_series_chart)
        self.secondary_chart_group = None

        self.visible_units = 6.5
        self.edge_padding_units = 1.0
        self.mask_padding_units = 1.0
        self.max_chart_records = 65535
        self.chart_batch_size = 5000

        self.trend_animation_months = []
        self.trend_animation_min_values = []
        self.trend_animation_max_values = []
        self.trend_window_start = 0.0

        self.series_animation_labels = []
        self.series_animation_values = []
        self.series_window_start = 0.0

        self.pending_trend_delta = 0.0
        self.pending_series_delta = 0.0

        self.trend_scroll_timer = QTimer(self)
        self.trend_scroll_timer.setSingleShot(True)
        self.trend_scroll_timer.setInterval(16)
        self.trend_scroll_timer.timeout.connect(self._apply_trend_scroll)

        self.series_scroll_timer = QTimer(self)
        self.series_scroll_timer.setSingleShot(True)
        self.series_scroll_timer.setInterval(16)
        self.series_scroll_timer.timeout.connect(self._apply_series_scroll)

        self.data_table = QTableWidget(0, 6)
        self.data_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.data_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.data_table.setHorizontalHeaderLabels(["日期", "url", "车系", "地域", "价格", "备注"])
        self.data_table.setMouseTracking(True)
        self.data_table.viewport().setMouseTracking(True)
        self.data_table.horizontalHeader().setStretchLastSection(True)

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

        self.all_records_cache = []
        self.available_series = []
        self._build_ui()
        self._bind_signals()
        self.load_real_data()
    def _clamp_window_start(self, start: float, item_count: int) -> float:
        max_start = max(0.0, item_count - self.visible_units)
        if start < 0:
            return 0.0
        if start > max_start:
            return max_start
        return start

    def _scroll_trend_chart(self, delta: int):
        self.pending_trend_delta += delta / 480.0
        if not self.trend_scroll_timer.isActive():
            self.trend_scroll_timer.start()

    def _scroll_series_chart(self, delta: int):
        self.pending_series_delta += delta / 480.0
        if not self.series_scroll_timer.isActive():
            self.series_scroll_timer.start()


    def _apply_trend_scroll(self):
        if self.pending_trend_delta == 0:
            return

        self.trend_window_start = self._clamp_window_start(
            self.trend_window_start - self.pending_trend_delta,
            len(self.trend_animation_months),
        )
        self.pending_trend_delta = 0.0

        self._draw_trend_frame(
            self.trend_animation_months,
            self.trend_animation_min_values,
            self.trend_animation_max_values,
            self.trend_window_start,
        )

    def _apply_series_scroll(self):
        if self.pending_series_delta == 0:
            return

        self.series_window_start = self._clamp_window_start(
            self.series_window_start - self.pending_series_delta,
            len(self.series_animation_labels),
        )
        self.pending_series_delta = 0.0

        self._draw_series_frame(
            self.series_animation_labels,
            self.series_animation_values,
            self.series_window_start,
        )

    def _build_ui(self):
        toolbar_layout = QHBoxLayout()
        toolbar_layout.addWidget(self.refresh_btn)
        toolbar_layout.addSpacing(12)
        toolbar_layout.addWidget(QLabel("车系筛选"))
        toolbar_layout.addWidget(self.series_filter_combo)
        toolbar_layout.addStretch()
        toolbar_layout.addWidget(self.status_label)

        stats_layout = QGridLayout()
        stats_layout.addWidget(self.total_records_card, 0, 0)
        stats_layout.addWidget(self.total_gid_card, 0, 1)
        stats_layout.addWidget(self.avg_price_card, 0, 2)
        stats_layout.addWidget(self.latest_date_card, 0, 3)

        stats_group = QGroupBox("统计概览")
        stats_group.setLayout(stats_layout)

        trend_group = QGroupBox("月度价格区间图")
        trend_layout = QVBoxLayout()
        trend_layout.setContentsMargins(8, 8, 8, 18)
        trend_layout.addWidget(self.trend_canvas)
        trend_group.setLayout(trend_layout)

        self.secondary_chart_group = QGroupBox("车系记录分布图")
        series_layout = QVBoxLayout()
        series_layout.setContentsMargins(8, 8, 8, 18)
        series_layout.addWidget(self.series_canvas)
        self.secondary_chart_group.setLayout(series_layout)

        charts_layout = QVBoxLayout()
        charts_layout.addWidget(trend_group, 1)
        charts_layout.addWidget(self.secondary_chart_group, 1)

        charts_group = QGroupBox("图表区域")
        charts_group.setLayout(charts_layout)

        table_header_layout = QHBoxLayout()
        table_header_layout.addWidget(self.data_count_label)
        table_header_layout.addStretch()

        table_layout = QVBoxLayout()
        self.data_table.setMinimumHeight(320)
        self.data_table.setColumnWidth(1, 400)
        table_layout.addLayout(table_header_layout)
        table_layout.addWidget(self.data_table)

        table_group = QGroupBox("数据预览")
        table_group.setLayout(table_layout)

        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.addLayout(toolbar_layout)
        content_layout.addWidget(stats_group)
        content_layout.addWidget(charts_group)
        content_layout.addWidget(table_group)
        content_layout.addStretch()

        self.scroll_area.setWidget(content_widget)

        page_layout = QVBoxLayout()
        page_layout.setContentsMargins(0, 0, 0, 0)
        page_layout.setSpacing(0)
        page_layout.addWidget(self.scroll_area)
        self.setLayout(page_layout)

    def _bind_signals(self):
        self.refresh_btn.clicked.connect(self.load_real_data)
        self.series_filter_combo.currentIndexChanged.connect(self._on_series_filter_changed)
        self.data_table.cellEntered.connect(self._on_table_cell_entered)
        self.data_table.cellClicked.connect(self._on_table_cell_clicked)

    def _on_table_cell_entered(self, row: int, column: int):
        item = self.data_table.item(row, column)
        if item is None:
            self.data_table.viewport().unsetCursor()
            QToolTip.hideText()
            return

        text = item.text() or ""

        if column == 1:
            self.data_table.viewport().setCursor(Qt.CursorShape.PointingHandCursor)
        else:
            self.data_table.viewport().unsetCursor()

        if column == 5 and text and text != "--":
            rect = self.data_table.visualItemRect(item)
            global_pos = self.data_table.viewport().mapToGlobal(rect.bottomRight())
            QToolTip.showText(global_pos, text, self.data_table)
        else:
            QToolTip.hideText()

    def _on_table_cell_clicked(self, row: int, column: int):
        if column != 1:
            return

        item = self.data_table.item(row, column)
        if item is None:
            return

        text = (item.text() or "").strip()
        if not text or text == "--":
            return

        QDesktopServices.openUrl(QUrl(text))

    def _on_series_filter_changed(self):
        self._refresh_view_from_selection()

    def _safe_call(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return func()

    def _normalized_location(self, value: Any) -> str:
        text = str(value).strip() if value is not None else ""
        return text if text else "其他"

    def _load_records(self, series_filter: str | None = None) -> list[Any]:
        supports_series = False
        try:
            sig = inspect.signature(list_price_records)
            params = sig.parameters
            target_series = series_filter if series_filter and series_filter != "全部" else None

            supports_limit = "limit" in params
            supports_offset = "offset" in params
            supports_page = "page" in params
            supports_series = "series" in params

            if supports_limit and (supports_offset or supports_page):
                all_records = []
                fetched_total = 0
                page_index = 1
                offset_value = 0

                while fetched_total < self.max_chart_records:
                    current_limit = min(self.chart_batch_size, self.max_chart_records - fetched_total)
                    kwargs = {"limit": current_limit}
                    if supports_offset:
                        kwargs["offset"] = offset_value
                    elif supports_page:
                        kwargs["page"] = page_index
                    if supports_series and target_series:
                        kwargs["series"] = target_series

                    batch = list_price_records(**kwargs)

                    if batch is None:
                        break
                    if isinstance(batch, Iterable) and not isinstance(batch, (str, bytes, dict)):
                        batch_list = list(batch)
                    else:
                        batch_list = [batch]

                    if not batch_list:
                        break

                    all_records.extend(batch_list)
                    fetched_total += len(batch_list)

                    if len(batch_list) < current_limit:
                        break

                    if supports_offset:
                        offset_value += len(batch_list)
                    elif supports_page:
                        page_index += 1

                normalized = all_records
            else:
                kwargs = {}
                if supports_limit:
                    kwargs["limit"] = self.max_chart_records
                if supports_series and target_series:
                    kwargs["series"] = target_series
                records = list_price_records(**kwargs) if kwargs else list_price_records()

                if records is None:
                    normalized = []
                elif isinstance(records, Iterable) and not isinstance(records, (str, bytes, dict)):
                    normalized = list(records)
                else:
                    normalized = [records]
        except Exception:
            records = self._safe_call(list_price_records)
            if records is None:
                normalized = []
            elif isinstance(records, Iterable) and not isinstance(records, (str, bytes, dict)):
                normalized = list(records)
            else:
                normalized = [records]

        if target_series and not supports_series:
            filtered = []
            for row in normalized:
                row_series = str(self._extract_value(row, ["series"], "")).strip()
                if row_series == target_series:
                    filtered.append(row)
            return filtered[: self.max_chart_records]

        return normalized[: self.max_chart_records]

    def _update_series_filter_options(self, records: list[Any]):
        try:
            unique_series = list_distinct_series()
        except Exception:
            series_values = []
            for row in records:
                series = str(self._extract_value(row, ["series"], "")).strip()
                if series:
                    series_values.append(series)
            unique_series = sorted(set(series_values))

        self.available_series = unique_series

        current_text = self.series_filter_combo.currentText() or "全部"
        self.series_filter_combo.blockSignals(True)
        self.series_filter_combo.clear()
        self.series_filter_combo.addItem("全部")
        self.series_filter_combo.addItems(unique_series)
        index = self.series_filter_combo.findText(current_text)
        if index < 0:
            index = 0
        self.series_filter_combo.setCurrentIndex(index)
        self.series_filter_combo.blockSignals(False)

    def _refresh_view_from_selection(self):
        selected_series = self.series_filter_combo.currentText() or "全部"
        records = self._load_records(selected_series)
        loaded_records = len(records)

        total_records = len(records)
        if selected_series == "全部":
            try:
                gid_count = count_distinct_gids()
            except Exception:
                gid_count = self._get_gid_count(records)
        else:
            gid_count = self._get_gid_count(records)
        latest_date = self._get_latest_date(records)

        price_values = []
        for row in records:
            price = self._extract_value(row, ["price_cny"], None)
            parsed = self._parse_price(price)
            if parsed is not None:
                price_values.append(parsed)

        avg_price = None
        if price_values:
            avg_price = sum(price_values) / len(price_values)

        self.total_records_card.set_value(str(total_records))
        self.total_gid_card.set_value(str(gid_count))
        self.avg_price_card.set_value(self._format_price(avg_price))
        self.latest_date_card.set_value(latest_date)
        self.data_count_label.setText(f"共 {len(records)} 条数据")
        self._fill_table(records)
        self._draw_month_range_chart(records)

        if selected_series == "全部":
            if self.secondary_chart_group is not None:
                self.secondary_chart_group.setTitle("车系记录分布图")
            self._draw_series_count_chart(self.all_records_cache)
        else:
            if self.secondary_chart_group is not None:
                self.secondary_chart_group.setTitle("地域均价分布图")
            self._draw_location_count_chart(records)

        truncated_hint = ""
        if loaded_records >= self.max_chart_records:
            truncated_hint = f"（已达到图表加载上限 {self.max_chart_records} 条）"

        if selected_series == "全部":
            self.status_label.setText(f"状态：已加载全部数据，共 {len(records)} 条{truncated_hint}")
        else:
            self.status_label.setText(f"状态：已加载车系 {selected_series}，共 {len(records)} 条{truncated_hint}")

    def _draw_location_count_chart(self, records: list[Any]):
        location_prices = defaultdict(list)
        for row in records:
            location = self._normalized_location(self._extract_value(row, ["location"], ""))
            price = self._parse_price(self._extract_value(row, ["price_cny"], None))
            if price is not None:
                location_prices[location].append(price)

        if not location_prices:
            self.series_animation_labels = []
            self.series_animation_values = []
            self._draw_empty_chart(self.series_canvas.axes, self.series_canvas, "暂无地域数据")
            return

        avg_items = []
        for location, prices in location_prices.items():
            if prices:
                avg_items.append((location, sum(prices) / len(prices)))

        avg_items.sort(key=lambda item: item[1], reverse=True)
        top_items = avg_items[:20]

        self.series_animation_labels = [item[0] for item in top_items]
        self.series_animation_values = [item[1] for item in top_items]
        self.series_window_start = 0.0

        self._draw_series_frame(
            self.series_animation_labels,
            self.series_animation_values,
            self.series_window_start,
        )
        self.series_canvas.axes.set_title("地域平均价格分布")
        self.series_canvas.axes.set_ylabel("平均价格（元）")
        self.series_canvas.draw_idle()

    def _extract_value(self, row: Any, candidate_keys: list[str], default: Any = "") -> Any:
        if row is None:
            return default

        if isinstance(row, dict):
            for key in candidate_keys:
                if key in row and row[key] not in (None, ""):
                    return row[key]
            return default

        if hasattr(row, "keys"):
            try:
                keys = set(row.keys())
                for key in candidate_keys:
                    if key in keys and row[key] not in (None, ""):
                        return row[key]
            except Exception:
                pass

        for key in candidate_keys:
            if hasattr(row, key):
                value = getattr(row, key)
                if value not in (None, ""):
                    return value

        return default

    def _parse_price(self, value: Any) -> float | None:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None

        number = float(match.group())
        if "亿" in text:
            number *= 100000000
        elif "万" in text:
            number *= 10000
        return number

    def _format_price(self, value: float | None) -> str:
        if value is None:
            return "--"
        if value >= 10000:
            return f"{value / 10000:.2f} 万"
        return f"{value:.0f}"

    def _get_gid_count(self, records: list[Any]) -> int:
        try:
            gid_stats = self._safe_call(count_price_records_by_gid)
            if isinstance(gid_stats, dict):
                return len(gid_stats)
            if isinstance(gid_stats, Iterable) and not isinstance(gid_stats, (str, bytes)):
                return len(list(gid_stats))
        except Exception:
            pass

        gids = set()
        for row in records:
            gid = self._extract_value(row, ["gid", "post_gid", "thread_gid"], "")
            if gid:
                gids.add(str(gid))
        return len(gids)

    def _get_latest_date(self, records: list[Any]) -> str:
        latest = ""
        for row in records:
            value = self._extract_value(
                row,
                ["date", "fetched_at", "record_date", "publish_date", "post_date", "created_at"],
                "",
            )
            if value:
                text = str(value)
                if text > latest:
                    latest = text
        return latest[:10] if latest else "--"

    def _month_key(self, value: Any) -> str:
        if value in (None, ""):
            return ""
        text = str(value).strip()
        match = re.match(r"(\d{4})-(\d{2})", text)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        return text[:7] if len(text) >= 7 else ""

    def _fill_table(self, records: list[Any]):
        preview_rows = records[:100]
        self.data_table.setRowCount(len(preview_rows))

        for row_idx, row in enumerate(preview_rows):
            date_text = str(
                self._extract_value(
                    row,
                    ["date", "fetched_at", "record_date", "publish_date", "post_date", "created_at"],
                    "--",
                )
            )[:10]
            gid_text = str(self._extract_value(row, ["source_url"], "--"))
            series_text = str(self._extract_value(row, ["series"], "--"))
            location_text = self._normalized_location(self._extract_value(row, ["location"], ""))

            price_value = self._extract_value(row, ["price_cny"], None)
            parsed_price = self._parse_price(price_value)
            if parsed_price is not None:
                price_text = self._format_price(parsed_price)
            elif price_value not in (None, ""):
                price_text = str(price_value)
            else:
                price_text = "--"

            note_text = str(
                self._extract_value(
                    row,
                    ["evidence_content", "evidence_where", "remark", "remarks", "note", "model", "car_model", "title"],
                    "",
                )
            )

            values = [date_text, gid_text, series_text, location_text, price_text, note_text]
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(value)
                if col_idx == 1 and value and value != "--":
                    item.setToolTip(value)
                if col_idx == 5 and value and value != "--":
                    item.setToolTip(value)
                self.data_table.setItem(row_idx, col_idx, item)

    def _draw_empty_chart(self, ax, canvas, text: str):
        ax.clear()
        ax.set_facecolor("white")
        canvas.figure.patch.set_facecolor("white")
        ax.text(0.5, 0.5, text, ha="center", va="center", transform=ax.transAxes)
        ax.set_xticks([])
        ax.set_yticks([])
        canvas.figure.subplots_adjust(left=0.12, right=0.95, top=0.88, bottom=0.22)
        canvas.draw_idle()

    def _draw_trend_frame(self, months: list[str], min_values: list[float], max_values: list[float], window_start: float):
        ax = self.trend_canvas.axes
        ax.clear()
        ax.set_facecolor("white")
        self.trend_canvas.figure.patch.set_facecolor("white")

        if not months:
            self._draw_empty_chart(ax, self.trend_canvas, "暂无价格数据")
            return

        start = self._clamp_window_start(window_start, len(months))
        end = start + self.visible_units
        render_padding = self.edge_padding_units
        left_bound = start - (render_padding * 2)
        right_bound = end + render_padding

        visible_points = []
        for idx, month in enumerate(months):
            if left_bound <= idx <= right_bound:
                x = idx - start
                visible_points.append((x, month, min_values[idx], max_values[idx]))

        if not visible_points:
            self._draw_empty_chart(ax, self.trend_canvas, "暂无价格数据")
            return

        x_values = [item[0] for item in visible_points]
        visible_min = [item[2] for item in visible_points]
        visible_max = [item[3] for item in visible_points]
        
        tick_positions = []
        tick_labels = []
        for idx, month in enumerate(months):
            if start <= idx < end:
                tick_positions.append(idx - start)
                tick_labels.append(month)

        visible_left = 0.0
        visible_right = float(self.visible_units - 1)
        plot_left = visible_left - self.mask_padding_units
        plot_right = visible_right + self.mask_padding_units
        ax.set_xlim(plot_left, plot_right)

        ax.plot(x_values, visible_min, marker="o", linewidth=1.8, color="blue")
        ax.plot(x_values, visible_max, marker="o", linewidth=1.8, color="gold")
        ax.fill_between(x_values, visible_min, visible_max, alpha=0.22, color="gold")
        ax.set_title("月度最高/最低价格区间")
        ax.set_xlabel("月份")
        ax.set_ylabel("价格（元）")
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=0)
        ax.grid(True, axis="y", alpha=0.3)
        ax.margins(x=0.0)
        self.trend_canvas.figure.subplots_adjust(left=0.12, right=0.95, top=0.88, bottom=0.22)

        left_mask = ax.axvspan(
            plot_left - (self.mask_padding_units * 2),
            visible_left - (self.mask_padding_units * 2),
            color="white",
            zorder=5,
        )
        right_mask = ax.axvspan(
            visible_right + self.mask_padding_units,
            plot_right + self.mask_padding_units,
            color="white",
            zorder=5,
        )
        left_mask.set_clip_on(True)
        right_mask.set_clip_on(True)

        self.trend_canvas.draw_idle()

    # _advance_trend_animation method removed

    def _draw_series_frame(self, labels: list[str], values: list[int], window_start: float):
        ax = self.series_canvas.axes
        ax.clear()
        ax.set_facecolor("white")
        self.series_canvas.figure.patch.set_facecolor("white")

        if not labels:
            self._draw_empty_chart(ax, self.series_canvas, "暂无车系数据")
            return

        start = self._clamp_window_start(window_start, len(labels))
        end = start + self.visible_units
        render_padding = self.edge_padding_units
        left_bound = start - (render_padding * 2)
        right_bound = end + render_padding

        visible_points = []
        for idx, label in enumerate(labels):
            if left_bound <= idx <= right_bound:
                x = idx - start
                visible_points.append((x, label, values[idx]))

        if not visible_points:
            self._draw_empty_chart(ax, self.series_canvas, "暂无车系数据")
            return

        x_values = [item[0] for item in visible_points]
        visible_values = [item[2] for item in visible_points]

        tick_positions = []
        tick_labels = []
        for idx, label in enumerate(labels):
            if start <= idx < end:
                tick_positions.append(idx - start)
                tick_labels.append(label)

        visible_left = -0.5
        visible_right = float(self.visible_units - 0.5)
        plot_left = visible_left - self.mask_padding_units
        plot_right = visible_right + self.mask_padding_units
        ax.set_xlim(plot_left, plot_right)

        ax.bar(x_values, visible_values, width=0.6)
        ax.set_title("车系记录数量分布")
        ax.set_xlabel("series")
        ax.set_ylabel("记录数")
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=35, ha="right")
        ax.grid(True, axis="y", alpha=0.3)
        ax.margins(x=0.0)
        self.series_canvas.figure.subplots_adjust(left=0.12, right=0.95, top=0.88, bottom=0.28)

        left_mask = ax.axvspan(
            plot_left - (self.mask_padding_units * 2),
            visible_left - (self.mask_padding_units * 2),
            color="white",
            zorder=5,
        )
        right_mask = ax.axvspan(
            visible_right + self.mask_padding_units,
            plot_right + self.mask_padding_units,
            color="white",
            zorder=5,
        )
        left_mask.set_clip_on(True)
        right_mask.set_clip_on(True)

        self.series_canvas.draw_idle()

    # _advance_series_animation method removed

    def _draw_month_range_chart(self, records: list[Any]):
        monthly_prices = defaultdict(list)
        for row in records:
            month = self._month_key(self._extract_value(row, ["date"], ""))
            price = self._parse_price(self._extract_value(row, ["price_cny"], None))
            if month and price is not None:
                monthly_prices[month].append(price)

        if not monthly_prices:
            self.trend_animation_months = []
            self.trend_animation_min_values = []
            self.trend_animation_max_values = []
            self._draw_empty_chart(self.trend_canvas.axes, self.trend_canvas, "暂无价格数据")
            return

        months = sorted(monthly_prices.keys())
        self.trend_animation_months = months
        self.trend_animation_min_values = [min(monthly_prices[m]) for m in months]
        self.trend_animation_max_values = [max(monthly_prices[m]) for m in months]
        self.trend_window_start = 0.0

        self._draw_trend_frame(
            self.trend_animation_months,
            self.trend_animation_min_values,
            self.trend_animation_max_values,
            self.trend_window_start,
        )

    def _draw_series_count_chart(self, records: list[Any]):
        counter = Counter()
        for row in records:
            series = str(self._extract_value(row, ["series"], "")).strip()
            if series:
                counter[series] += 1

        if not counter:
            self.series_animation_labels = []
            self.series_animation_values = []
            self._draw_empty_chart(self.series_canvas.axes, self.series_canvas, "暂无车系数据")
            return

        top_items = counter.most_common(20)
        self.series_animation_labels = [item[0] for item in top_items]
        self.series_animation_values = [item[1] for item in top_items]
        self.series_window_start = 0.0

        self._draw_series_frame(
            self.series_animation_labels,
            self.series_animation_values,
            self.series_window_start,
        )
        self.series_canvas.axes.set_title("车系记录数量分布")
        self.series_canvas.axes.set_ylabel("记录数")
        self.series_canvas.draw_idle()

    def load_real_data(self):
        self.status_label.setText("状态：正在从数据库加载数据")

        try:
            all_records = self._load_records("全部")
            self.all_records_cache = all_records
            self._update_series_filter_options(all_records)
            self.pending_trend_delta = 0.0
            self.pending_series_delta = 0.0
            self.trend_scroll_timer.stop()
            self.series_scroll_timer.stop()
            self._refresh_view_from_selection()
        except Exception as e:
            self.total_records_card.set_value("--")
            self.total_gid_card.set_value("--")
            self.avg_price_card.set_value("--")
            self.latest_date_card.set_value("--")
            self.data_count_label.setText("共 0 条数据")
            self.data_table.setRowCount(0)
            self._draw_empty_chart(self.trend_canvas.axes, self.trend_canvas, "暂无价格数据")
            self._draw_empty_chart(self.series_canvas.axes, self.series_canvas, "暂无车系数据")
            self.status_label.setText(f"状态：数据库加载失败 - {e}")