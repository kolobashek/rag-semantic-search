"""
windows_app.py — Нативное Windows-приложение (PyQt6) для RAG Каталога.

Запуск:
    python windows_app.py
"""

import logging
import sys
from typing import Any, Dict, List, Optional

from PyQt6.QtCore import QCloseEvent, QThread, Qt, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenuBar,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from rag_core import RAGSearcher, load_config, save_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_TITLE = "📚 RAG Каталог — Поиск по документам"
APP_VERSION = "2.0.0"


# ════════════════════════════ SearchThread ══════════════════════════════

class SearchThread(QThread):
    """Фоновый поток поиска — не блокирует UI."""

    search_finished = pyqtSignal(list)
    search_error = pyqtSignal(str)

    def __init__(
        self,
        searcher: RAGSearcher,
        query: str,
        limit: int,
        file_type: Optional[str],
        content_only: bool,
    ) -> None:
        super().__init__()
        self.searcher = searcher
        self.query = query
        self.limit = limit
        self.file_type = file_type
        self.content_only = content_only

    def run(self) -> None:
        try:
            results = self.searcher.search(
                self.query,
                limit=self.limit,
                file_type=self.file_type,
                content_only=self.content_only,
            )
            self.search_finished.emit(results)
        except Exception as exc:
            self.search_error.emit(str(exc))


# ════════════════════════════ ResultCard ═══════════════════════════════

class ResultCard(QFrame):
    """Виджет для одного результата поиска."""

    def __init__(self, result: Dict[str, Any], index: int) -> None:
        super().__init__()
        self.result = result
        self.index = index
        self._build_ui()

    def _build_ui(self) -> None:
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet(
            "ResultCard { background: white; border: 1px solid #e0e0e0; "
            "border-radius: 5px; margin-bottom: 6px; }"
            "ResultCard:hover { border: 2px solid #1f77b4; }"
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(6)

        # ── заголовок ────────────────────────────────────────────────
        header = QHBoxLayout()

        title = QLabel(f"[{self.index}] {self.result.get('filename', '')}")
        f = QFont()
        f.setPointSize(10)
        f.setBold(True)
        title.setFont(f)
        header.addWidget(title)
        header.addStretch()

        score_lbl = QLabel(f"Score: {self.result.get('score', 0):.3f}")
        score_lbl.setStyleSheet(
            "background:#d4edda; color:#155724; padding:2px 8px; "
            "border-radius:3px; font-weight:bold; font-size:9pt;"
        )
        header.addWidget(score_lbl)

        ext = self.result.get("extension") or "?"
        ext_lbl = QLabel(ext)
        ext_lbl.setStyleSheet(
            "background:#cce5ff; color:#004085; padding:2px 8px; "
            "border-radius:3px; font-size:9pt;"
        )
        header.addWidget(ext_lbl)

        layout.addLayout(header)

        # ── путь ─────────────────────────────────────────────────────
        path_lbl = QLabel(f"📁  {self.result.get('path', '')}")
        path_lbl.setStyleSheet("color:#666; font-size:9pt;")
        path_lbl.setWordWrap(True)
        layout.addWidget(path_lbl)

        # ── детали ───────────────────────────────────────────────────
        details_parts = [f"Тип: {self.result.get('type', '')}"]
        if self.result.get("size_mb") is not None:
            details_parts.append(f"Размер: {self.result['size_mb']} МБ")
        if self.result.get("modified"):
            details_parts.append(f"Изменён: {str(self.result['modified'])[:10]}")
        details_lbl = QLabel("  |  ".join(details_parts))
        details_lbl.setStyleSheet("color:#999; font-size:8pt;")
        layout.addWidget(details_lbl)

        # ── превью текста ─────────────────────────────────────────────
        raw_text = self.result.get("text") or ""
        preview = raw_text[:400] + ("…" if len(raw_text) > 400 else "")
        preview_lbl = QLabel(preview)
        preview_lbl.setStyleSheet(
            "background:#f5f5f5; padding:7px 10px; border-radius:3px; "
            "color:#333; font-size:9pt;"
        )
        preview_lbl.setWordWrap(True)
        layout.addWidget(preview_lbl)


# ════════════════════════════ SettingsDialog ════════════════════════════

class SettingsDialog(QDialog):
    """Диалог редактирования config.json."""

    def __init__(self, config: Dict[str, Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.config = dict(config)
        self.setWindowTitle("Настройки")
        self.setMinimumWidth(520)
        self._build_ui()

    def _browse(self, line_edit: QLineEdit, is_file: bool = False) -> None:
        if is_file:
            path, _ = QFileDialog.getSaveFileName(self, "Выбрать файл", line_edit.text())
        else:
            path = QFileDialog.getExistingDirectory(self, "Выбрать папку", line_edit.text())
        if path:
            line_edit.setText(path)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        # Папка каталога
        row1 = QHBoxLayout()
        self.catalog_edit = QLineEdit(self.config.get("catalog_path", ""))
        btn1 = QPushButton("…")
        btn1.setFixedWidth(28)
        btn1.clicked.connect(lambda: self._browse(self.catalog_edit))
        row1.addWidget(self.catalog_edit)
        row1.addWidget(btn1)
        form.addRow("Папка каталога:", row1)

        # База Qdrant
        row2 = QHBoxLayout()
        self.db_edit = QLineEdit(self.config.get("qdrant_db_path", ""))
        btn2 = QPushButton("…")
        btn2.setFixedWidth(28)
        btn2.clicked.connect(lambda: self._browse(self.db_edit))
        row2.addWidget(self.db_edit)
        row2.addWidget(btn2)
        form.addRow("База Qdrant:", row2)

        # Лог файл
        row3 = QHBoxLayout()
        self.log_edit = QLineEdit(self.config.get("log_file", ""))
        btn3 = QPushButton("…")
        btn3.setFixedWidth(28)
        btn3.clicked.connect(lambda: self._browse(self.log_edit, is_file=True))
        row3.addWidget(self.log_edit)
        row3.addWidget(btn3)
        form.addRow("Лог файл:", row3)

        layout.addLayout(form)
        layout.addSpacing(12)

        note = QLabel("Изменения применятся после нажатия OK и перезапуска приложения.")
        note.setStyleSheet("color:#888; font-size:9pt;")
        note.setWordWrap(True)
        layout.addWidget(note)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_updated_config(self) -> Dict[str, Any]:
        self.config["catalog_path"]  = self.catalog_edit.text().strip()
        self.config["qdrant_db_path"] = self.db_edit.text().strip()
        self.config["log_file"]       = self.log_edit.text().strip()
        return self.config


# ════════════════════════════ RAGWindow ═════════════════════════════════

class RAGWindow(QMainWindow):
    """Главное окно приложения."""

    def __init__(self) -> None:
        super().__init__()
        self.cfg = load_config()
        self.searcher: Optional[RAGSearcher] = None
        self._search_thread: Optional[SearchThread] = None
        self._build_ui()
        self._build_menu()
        self._init_searcher()

    # ── UI ────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        self.setWindowTitle(APP_TITLE)
        self.setGeometry(100, 100, 1200, 820)

        self.setStyleSheet(
            """
            QMainWindow { background: #f5f5f5; }
            QLineEdit, QComboBox, QSpinBox {
                padding: 7px 10px; border: 1px solid #ccc; border-radius: 4px;
                background: white; font-size: 10pt;
            }
            QLineEdit:focus, QComboBox:focus { border: 2px solid #1f77b4; }
            QPushButton {
                background: #1f77b4; color: white; border: none;
                padding: 7px 16px; border-radius: 4px;
                font-weight: bold; font-size: 10pt;
            }
            QPushButton:hover    { background: #1563a0; }
            QPushButton:pressed  { background: #0d4080; }
            QPushButton:disabled { background: #aaa; }
            QLabel { color: #333; }
            """
        )

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(16, 14, 16, 8)
        root.setSpacing(10)

        # Заголовок
        title_lbl = QLabel(f"📚 RAG Каталог — Семантический поиск  v{APP_VERSION}")
        tf = QFont()
        tf.setPointSize(13)
        tf.setBold(True)
        title_lbl.setFont(tf)
        root.addWidget(title_lbl)

        # Поисковая строка
        search_row = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("🔍  Введите запрос: договоры, паспорта, счета…")
        self.search_input.returnPressed.connect(self._do_search)
        search_row.addWidget(self.search_input)

        self.search_btn = QPushButton("🔍 Найти")
        self.search_btn.setFixedWidth(110)
        self.search_btn.clicked.connect(self._do_search)
        search_row.addWidget(self.search_btn)
        root.addLayout(search_row)

        # Фильтры
        filters_row = QHBoxLayout()
        filters_row.addWidget(QLabel("Результатов:"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(5, 50)
        self.limit_spin.setValue(10)
        self.limit_spin.setSingleStep(5)
        self.limit_spin.setMaximumWidth(75)
        filters_row.addWidget(self.limit_spin)
        filters_row.addSpacing(16)

        filters_row.addWidget(QLabel("Тип файла:"))
        self.filetype_combo = QComboBox()
        self.filetype_combo.addItems(["Все", ".docx", ".xlsx", ".xls", ".pdf"])
        self.filetype_combo.setMaximumWidth(110)
        filters_row.addWidget(self.filetype_combo)
        filters_row.addSpacing(16)

        self.content_only_cb = QCheckBox("Только содержимое")
        filters_row.addWidget(self.content_only_cb)
        filters_row.addStretch()
        root.addLayout(filters_row)

        # Кнопки быстрого поиска
        quick_row = QHBoxLayout()
        quick_row.addWidget(QLabel("Быстрый поиск:"))
        quick_presets = [
            ("📄 Договоры",  "договоры"),
            ("👤 Паспорта",   "паспорта"),
            ("📋 Счета",      "счета на оплату"),
            ("📝 Записки",    "служебная записка"),
            ("💰 Финансовые", "финансовый отчёт"),
            ("⚖️ Юридические","юридический"),
        ]
        for label, qry in quick_presets:
            btn = QPushButton(label)
            btn.setMaximumWidth(130)
            btn.setStyleSheet(
                "QPushButton { background:#e8f4f8; color:#1f77b4; border:1px solid #1f77b4; }"
                "QPushButton:hover { background:#d0e8f0; }"
            )
            btn.clicked.connect(lambda _checked, q=qry: self._quick_search(q))
            quick_row.addWidget(btn)
        quick_row.addStretch()
        root.addLayout(quick_row)

        # Прогресс-бар (неопределённый режим пока идёт поиск)
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumHeight(4)
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(False)
        root.addWidget(self.progress_bar)

        # Область результатов
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("QScrollArea { border: none; }")
        self.results_container = QWidget()
        self.results_layout = QVBoxLayout(self.results_container)
        self.results_layout.setSpacing(4)
        self.scroll.setWidget(self.results_container)
        root.addWidget(self.scroll, stretch=1)

        # Статус-бар
        self.statusBar().showMessage("Инициализация…")
        self._conn_lbl = QLabel()
        self._conn_lbl.setStyleSheet("padding: 3px 8px;")
        self.statusBar().addPermanentWidget(self._conn_lbl)

    def _build_menu(self) -> None:
        bar: QMenuBar = self.menuBar()

        file_menu = bar.addMenu("Файл")
        settings_action = file_menu.addAction("⚙️  Настройки…")
        settings_action.triggered.connect(self._open_settings)
        file_menu.addSeparator()
        exit_action = file_menu.addAction("Выход")
        exit_action.triggered.connect(self.close)

        help_menu = bar.addMenu("Справка")
        about_action = help_menu.addAction("О программе")
        about_action.triggered.connect(self._show_about)

    # ── searcher init ─────────────────────────────────────────────────

    def _init_searcher(self) -> None:
        try:
            self.searcher = RAGSearcher(self.cfg)
            if self.searcher.connected:
                stats = self.searcher.get_collection_stats()
                pts = stats.get("points_count", 0)
                self.statusBar().showMessage(f"✅ Подключено к RAG  |  Точек: {pts:,}")
                self._conn_lbl.setText("✅ Подключено")
                self._conn_lbl.setStyleSheet(
                    "background:#d4edda; color:#155724; padding:3px 8px; border-radius:3px;"
                )
            else:
                self._set_disconnected()
                QMessageBox.warning(
                    self,
                    "Нет подключения",
                    "Не удалось подключиться к базе RAG.\n\n"
                    "Убедитесь что:\n"
                    f"1. Выполнено индексирование (index_rag.py)\n"
                    f"2. Папка '{self.cfg['qdrant_db_path']}' существует",
                )
        except Exception as exc:
            self.statusBar().showMessage(f"❌ Ошибка: {exc}")
            QMessageBox.critical(self, "Ошибка инициализации", str(exc))

    def _set_disconnected(self) -> None:
        self.statusBar().showMessage("❌ Нет подключения к RAG")
        self._conn_lbl.setText("❌ Отключено")
        self._conn_lbl.setStyleSheet(
            "background:#f8d7da; color:#721c24; padding:3px 8px; border-radius:3px;"
        )

    # ── search ────────────────────────────────────────────────────────

    def _quick_search(self, query: str) -> None:
        self.search_input.setText(query)
        self._do_search()

    def _do_search(self) -> None:
        query = self.search_input.text().strip()
        if not query:
            QMessageBox.warning(self, "Пустой запрос", "Введите поисковый запрос.")
            return
        if not self.searcher or not self.searcher.connected:
            QMessageBox.critical(self, "Ошибка", "RAG не подключена.")
            return

        limit = self.limit_spin.value()
        file_type: Optional[str] = self.filetype_combo.currentText()
        if file_type == "Все":
            file_type = None
        content_only = self.content_only_cb.isChecked()

        self._set_searching(True)
        self._clear_results()

        self._search_thread = SearchThread(
            self.searcher, query, limit, file_type, content_only
        )
        self._search_thread.search_finished.connect(self._on_results)
        self._search_thread.search_error.connect(self._on_search_error)
        self._search_thread.start()

    def _set_searching(self, active: bool) -> None:
        self.search_btn.setEnabled(not active)
        self.search_input.setEnabled(not active)
        if active:
            # Неопределённый прогресс (полоска бежит)
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setVisible(True)
        else:
            self.progress_bar.setRange(0, 1)
            self.progress_bar.setVisible(False)

    def _clear_results(self) -> None:
        while self.results_layout.count():
            item = self.results_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

    # ── slots ─────────────────────────────────────────────────────────

    def _on_results(self, results: List[Dict[str, Any]]) -> None:
        self._set_searching(False)
        self._search_thread = None  # освобождаем ссылку

        if not results:
            self.statusBar().showMessage("❌ Ничего не найдено")
            placeholder = QLabel("🔍  По вашему запросу ничего не найдено.")
            placeholder.setStyleSheet("color:#999; padding:20px; font-size:11pt;")
            self.results_layout.addWidget(placeholder)
        else:
            self.statusBar().showMessage(f"✅ Найдено результатов: {len(results)}")
            for i, r in enumerate(results, 1):
                self.results_layout.addWidget(ResultCard(r, i))
        self.results_layout.addStretch()

    def _on_search_error(self, error: str) -> None:
        self._set_searching(False)
        self._search_thread = None
        self.statusBar().showMessage(f"❌ Ошибка: {error}")
        QMessageBox.critical(self, "Ошибка поиска", error)

    # ── settings ──────────────────────────────────────────────────────

    def _open_settings(self) -> None:
        dlg = SettingsDialog(self.cfg, parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self.cfg = dlg.get_updated_config()
            save_config(self.cfg)
            # Переинициализировать searcher с новыми путями
            self._init_searcher()

    # ── close event ───────────────────────────────────────────────────

    def closeEvent(self, event: QCloseEvent) -> None:
        """Корректно завершить SearchThread перед закрытием окна.

        Порядок:
          1. quit()  — просим event-loop потока завершиться штатно.
          2. wait(3000) — даём 3 секунды на завершение.
          3. terminate() — принудительно убиваем, если поток всё ещё жив
             (предотвращает "QThread: Destroyed while thread is still running").
          4. wait() без таймаута — убеждаемся, что Qt освободил ресурсы потока.
        """
        if self._search_thread and self._search_thread.isRunning():
            self._search_thread.quit()
            if not self._search_thread.wait(3000):
                logger.warning(
                    "SearchThread не завершился за 3 сек — принудительное завершение"
                )
                self._search_thread.terminate()
                self._search_thread.wait()   # дождаться фактического завершения
        event.accept()

    # ── about ─────────────────────────────────────────────────────────

    def _show_about(self) -> None:
        QMessageBox.information(
            self,
            "О программе",
            f"RAG Каталог  v{APP_VERSION}\n\n"
            "Семантический поиск по DOCX / XLSX / PDF файлам.\n"
            "Движок: Qdrant + all-MiniLM-L6-v2 embeddings.\n",
        )


# ─────────────────────────── entry point ───────────────────────────────

def main() -> None:
    app = QApplication(sys.argv)
    window = RAGWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
