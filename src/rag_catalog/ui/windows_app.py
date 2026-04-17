"""
windows_app.py — Нативное Windows-приложение (PyQt6) для RAG Каталога.

Запуск:
    python windows_app.py
"""

import logging
import re
from collections import defaultdict
from difflib import get_close_matches
from pathlib import Path, PurePath
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from PyQt6.QtCore import QThread, QTimer, Qt, pyqtSignal
from PyQt6.QtGui import QCloseEvent, QFont, QIcon
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
    QStyle,
    QVBoxLayout,
    QWidget,
)

from rag_catalog.core.rag_core import RAGSearcher, load_config, save_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_TITLE = "RAG Каталог — Поиск по документам"
APP_VERSION = "2.0.0"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "icon.ico"


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
                source="windows_app",
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
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setStyleSheet(
            "ResultCard { background: #ffffff; border: 1px solid #d6e0ee; "
            "border-radius: 10px; margin-bottom: 8px; }"
            "ResultCard:hover { border: 1px solid #6ea8fe; background:#f8fbff; }"
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(6)

        # ── заголовок ────────────────────────────────────────────────
        header = QHBoxLayout()

        file_icon = QLabel()
        file_icon.setPixmap(self._file_icon().pixmap(20, 20))
        header.addWidget(file_icon)

        title = QLabel(f"{self.index}. {self.result.get('filename', '')}")
        f = QFont()
        f.setPointSize(11)
        f.setBold(True)
        title.setFont(f)
        title.setStyleSheet("color:#0f172a;")
        header.addWidget(title)
        header.addStretch()

        score_lbl = QLabel(f"Релевантность: {self.result.get('score', 0):.3f}")
        score_lbl.setStyleSheet(
            "background:#dcfce7; color:#166534; padding:3px 8px; "
            "border-radius:5px; font-weight:600; font-size:8.5pt;"
        )
        header.addWidget(score_lbl)

        ext = self.result.get("extension") or "?"
        ext_lbl = QLabel(ext.upper())
        ext_lbl.setStyleSheet(
            "background:#e0ecff; color:#1e3a8a; padding:3px 8px; "
            "border-radius:5px; font-size:8.5pt; font-weight:600;"
        )
        header.addWidget(ext_lbl)

        layout.addLayout(header)

        # ── путь ─────────────────────────────────────────────────────
        path_lbl = QLabel(f"Путь: {self.result.get('path', '')}")
        path_lbl.setStyleSheet("color:#475569; font-size:9pt;")
        path_lbl.setWordWrap(True)
        layout.addWidget(path_lbl)

        # ── детали ───────────────────────────────────────────────────
        details_parts = [f"Тип: {self.result.get('type', '')}"]
        if self.result.get("size_mb") is not None:
            details_parts.append(f"Размер: {self.result['size_mb']} МБ")
        if self.result.get("modified"):
            details_parts.append(f"Изменён: {str(self.result['modified'])[:10]}")
        details_lbl = QLabel("  |  ".join(details_parts))
        details_lbl.setStyleSheet("color:#64748b; font-size:8.5pt;")
        layout.addWidget(details_lbl)

        where_found = self._where_found_comment()
        clean_text = self._clean_display_text(self.result.get("text") or "")
        title, context = self._extract_context_bits(raw_text=clean_text)
        comment_lines = [f"Где найдено: {where_found}"]
        if title:
            comment_lines.append(f"Заголовок: {title}")
        if context:
            comment_lines.append(f"Контекст: {context}")
        comment_lbl = QLabel("\n".join(comment_lines))
        comment_lbl.setWordWrap(True)
        comment_lbl.setStyleSheet(
            "background:#eff6ff; border-left:3px solid #3b82f6; "
            "padding:7px 9px; color:#1e3a5f; font-size:8.5pt; border-radius:6px;"
        )
        layout.addWidget(comment_lbl)

        # ── превью текста ─────────────────────────────────────────────
        preview = clean_text[:400] + ("…" if len(clean_text) > 400 else "")
        preview_lbl = QLabel(preview)
        preview_lbl.setStyleSheet(
            "background:#f8fafc; border:1px solid #e2e8f0; padding:8px 10px; border-radius:6px; "
            "color:#0f172a; font-size:9pt;"
        )
        preview_lbl.setWordWrap(True)
        layout.addWidget(preview_lbl)

    def _where_found_comment(self) -> str:
        type_raw = str(self.result.get("type") or "").strip()
        chunk_index = self.result.get("chunk_index")
        ext = str(self.result.get("extension") or "").strip()
        parts: List[str] = []
        if type_raw:
            parts.append(type_raw)
        if chunk_index is not None:
            try:
                parts.append(f"фрагмент #{int(chunk_index) + 1}")
            except Exception:
                parts.append(f"фрагмент #{chunk_index}")
        if ext:
            parts.append(ext)
        return " | ".join(parts) if parts else "в тексте документа"

    def _file_icon(self) -> QIcon:
        ext = str(self.result.get("extension") or "").lower()
        if ext == ".pdf":
            return self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        if ext in {".xlsx", ".xls"}:
            return self.style().standardIcon(QStyle.StandardPixmap.SP_DriveHDIcon)
        if ext in {".docx", ".doc"}:
            return self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView)
        return self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)

    @staticmethod
    def _extract_context_bits(raw_text: str) -> tuple[str, str]:
        text = (raw_text or "").strip()
        if not text:
            return "", ""
        lines = [x.strip() for x in re.split(r"[\r\n]+", text) if x.strip()]
        title = ""
        for ln in lines[:6]:
            if 4 <= len(ln) <= 120:
                title = ln
                break
        sentence_parts = re.split(r"(?<=[.!?])\s+", text.replace("\n", " ").strip())
        context = sentence_parts[0] if sentence_parts else text[:200]
        return title[:160], context[:220]

    @staticmethod
    def _clean_display_text(raw_text: str) -> str:
        cleaned = re.sub(r"<[^>]{1,200}>", " ", str(raw_text or ""))
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\s*\n\s*", "\n", cleaned)
        return cleaned.strip()


class FolderCard(QFrame):
    """Карточка результата-каталога."""

    def __init__(self, folder_path: str, score: float = 0.0, sample: str = "") -> None:
        super().__init__()
        self.folder_path = folder_path
        self.score = score
        self.sample = sample
        self._build_ui()

    def _build_ui(self) -> None:
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setStyleSheet(
            "FolderCard { background: #fffdf0; border: 1px solid #f3e8a6; border-radius: 10px; }"
            "FolderCard:hover { border: 1px solid #e9d66b; background:#fffbe0; }"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        title = QLabel(f"Каталог: {self.folder_path}")
        title.setStyleSheet("color:#7a5300; font-size:10.5pt; font-weight:700;")
        title.setWordWrap(True)
        layout.addWidget(title)

        meta = QLabel(f"Релевантность группы: {self.score:.3f}")
        meta.setStyleSheet("color:#8a6a00; font-size:8.5pt;")
        layout.addWidget(meta)

        if self.sample:
            sample_lbl = QLabel(f"Пример: {self.sample}")
            sample_lbl.setStyleSheet("color:#6b7280; font-size:8.5pt;")
            sample_lbl.setWordWrap(True)
            layout.addWidget(sample_lbl)


class GroupSection(QFrame):
    """Сворачиваемая секция результатов."""

    def __init__(self, title: str, count: int) -> None:
        super().__init__()
        self._expanded = True
        self._content = QWidget()
        self._content_layout = QVBoxLayout(self._content)
        self._content_layout.setContentsMargins(8, 6, 8, 8)
        self._content_layout.setSpacing(8)
        self._toggle_btn = QPushButton()
        self._title = title
        self._count = count
        self._build_ui()

    def _build_ui(self) -> None:
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setStyleSheet(
            "GroupSection { background:#ecf2fb; border:1px solid #d7e2f2; border-radius:10px; }"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        self._toggle_btn.setCheckable(True)
        self._toggle_btn.setChecked(True)
        self._toggle_btn.setStyleSheet(
            "QPushButton { background:#dbeafe; color:#1e3a8a; text-align:left; "
            "padding:8px 10px; border:1px solid #bfdbfe; border-radius:8px; font-weight:700; }"
            "QPushButton:hover { background:#cfe2ff; }"
        )
        self._toggle_btn.clicked.connect(self._toggle)
        root.addWidget(self._toggle_btn)
        root.addWidget(self._content)
        self._refresh_title()

    def _refresh_title(self) -> None:
        marker = "▼" if self._expanded else "▶"
        self._toggle_btn.setText(f"{marker} {self._title} ({self._count})")

    def _toggle(self) -> None:
        self._expanded = not self._expanded
        self._content.setVisible(self._expanded)
        self._refresh_title()

    def add_widget(self, widget: QWidget) -> None:
        self._content_layout.addWidget(widget)

    def add_stretch(self) -> None:
        self._content_layout.addStretch()


# ════════════════════════════ SettingsDialog ════════════════════════════

class SettingsDialog(QDialog):
    """Диалог редактирования config.json."""

    def __init__(self, config: Dict[str, Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.config = dict(config)
        self.setWindowTitle("Настройки")
        self.setMinimumWidth(520)
        self.resize(680, 840)
        self._build_ui()

    def _browse(self, line_edit: QLineEdit, is_file: bool = False) -> None:
        if is_file:
            path, _ = QFileDialog.getSaveFileName(self, "Выбрать файл", line_edit.text())
        else:
            path = QFileDialog.getExistingDirectory(self, "Выбрать папку", line_edit.text())
        if path:
            line_edit.setText(path)

    def _build_ui(self) -> None:
        self.setStyleSheet(
            """
            QDialog {
                background: #f8fbff;
                color: #0f172a;
            }
            QDialog QLabel {
                color: #0f172a;
                font-size: 9.5pt;
            }
            QDialog QCheckBox {
                color: #0f172a;
                font-size: 9.5pt;
            }
            QDialog QLineEdit, QDialog QSpinBox, QDialog QComboBox {
                background: #ffffff;
                color: #0f172a;
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                padding: 7px 9px;
            }
            QDialog QLineEdit:focus, QDialog QSpinBox:focus, QDialog QComboBox:focus {
                border: 1px solid #3b82f6;
            }
            QDialog QPushButton {
                background: #2563eb;
                color: #ffffff;
                border: none;
                border-radius: 8px;
                padding: 7px 12px;
                font-weight: 600;
            }
            QDialog QPushButton:hover { background: #1d4ed8; }
            QDialogButtonBox QPushButton {
                min-width: 84px;
            }
            """
        )
        layout = QVBoxLayout(self)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(8)

        section_paths = QLabel("Пути и хранилище")
        section_paths.setStyleSheet("font-weight:700; color:#0b3b8f; padding:4px 0;")
        form.addRow(section_paths)

        # Папка каталога
        row1 = QHBoxLayout()
        self.catalog_edit = QLineEdit(self.config.get("catalog_path", ""))
        btn1 = QPushButton("…")
        btn1.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon))
        btn1.setFixedWidth(28)
        btn1.clicked.connect(lambda: self._browse(self.catalog_edit))
        row1.addWidget(self.catalog_edit)
        row1.addWidget(btn1)
        form.addRow("Папка каталога:", row1)

        # База Qdrant
        row2 = QHBoxLayout()
        self.db_edit = QLineEdit(self.config.get("qdrant_db_path", ""))
        btn2 = QPushButton("…")
        btn2.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon))
        btn2.setFixedWidth(28)
        btn2.clicked.connect(lambda: self._browse(self.db_edit))
        row2.addWidget(self.db_edit)
        row2.addWidget(btn2)
        form.addRow("База Qdrant:", row2)

        # Лог файл
        row3 = QHBoxLayout()
        self.log_edit = QLineEdit(self.config.get("log_file", ""))
        btn3 = QPushButton("…")
        btn3.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon))
        btn3.setFixedWidth(28)
        btn3.clicked.connect(lambda: self._browse(self.log_edit, is_file=True))
        row3.addWidget(self.log_edit)
        row3.addWidget(btn3)
        form.addRow("Лог файл:", row3)

        self.telemetry_db_edit = QLineEdit(self.config.get("telemetry_db_path", ""))
        self.telemetry_db_edit.setPlaceholderText("Опционально: путь к SQLite БД логов")
        form.addRow("SQLite лог БД:", self.telemetry_db_edit)
        self.telemetry_db_edit.setToolTip(
            "Кратко: куда сохранять логи запросов/индексации.\n"
            "Подробно: если пусто, используется <qdrant_db_path>\\rag_telemetry.db."
        )

        self.users_db_edit = QLineEdit(self.config.get("users_db_path", ""))
        self.users_db_edit.setPlaceholderText("Опционально: путь к SQLite БД пользователей")
        self.users_db_edit.setToolTip(
            "Кратко: где хранить пользователей и коды Telegram-подтверждения.\n"
            "Подробно: если пусто, используется <qdrant_db_path>\\rag_users.db."
        )
        form.addRow("SQLite users БД:", self.users_db_edit)

        # Индексация
        section_index = QLabel("Индексация")
        section_index.setStyleSheet("font-weight:700; color:#0b3b8f; padding:8px 0 4px 0;")
        form.addRow(section_index)

        self.chunk_size_spin = QSpinBox()
        self.chunk_size_spin.setRange(100, 5000)
        self.chunk_size_spin.setValue(int(self.config.get("chunk_size", 500)))
        self.chunk_size_spin.setToolTip(
            "Кратко: размер одного текстового чанка.\n"
            "Подробно: больше значение = меньше чанков и быстрее индексация, "
            "но ниже точность попадания в узкие фразы."
        )
        form.addRow("Chunk size:", self.chunk_size_spin)

        self.chunk_overlap_spin = QSpinBox()
        self.chunk_overlap_spin.setRange(0, 2000)
        self.chunk_overlap_spin.setValue(int(self.config.get("chunk_overlap", 100)))
        self.chunk_overlap_spin.setToolTip(
            "Кратко: перекрытие соседних чанков.\n"
            "Подробно: помогает не терять смысл на границах, но увеличивает объем индекса."
        )
        form.addRow("Chunk overlap:", self.chunk_overlap_spin)

        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(50, 20000)
        self.batch_size_spin.setSingleStep(50)
        self.batch_size_spin.setValue(int(self.config.get("batch_size", 1000)))
        self.batch_size_spin.setToolTip(
            "Кратко: размер батча записи векторов.\n"
            "Подробно: больше батч = меньше накладных расходов, но выше пиковая память."
        )
        form.addRow("Batch size:", self.batch_size_spin)

        self.index_workers_spin = QSpinBox()
        self.index_workers_spin.setRange(1, 32)
        self.index_workers_spin.setValue(int(self.config.get("index_read_workers", 4)))
        self.index_workers_spin.setToolTip(
            "Кратко: число потоков чтения файлов.\n"
            "Подробно: повышает скорость I/O; слишком много потоков может перегрузить сеть/диск."
        )
        form.addRow("Индекс. потоки:", self.index_workers_spin)

        self.index_max_chunks_spin = QSpinBox()
        self.index_max_chunks_spin.setRange(0, 20000)
        self.index_max_chunks_spin.setValue(int(self.config.get("index_max_chunks", 2000)))
        self.index_max_chunks_spin.setToolTip(
            "Кратко: максимум чанков на файл.\n"
            "Подробно: 0 = без лимита; ограничение защищает от огромных файлов и взрывного роста индекса."
        )
        form.addRow("Макс. чанков/файл:", self.index_max_chunks_spin)

        self.index_skip_ocr_cb = QCheckBox("Пропускать OCR для сканированных PDF")
        self.index_skip_ocr_cb.setChecked(bool(self.config.get("index_skip_ocr", False)))
        self.index_skip_ocr_cb.setToolTip(
            "Кратко: не выполнять OCR при отсутствии текстового слоя в PDF.\n"
            "Подробно: резко ускоряет индексацию, но текст из сканов не попадет в поиск."
        )
        form.addRow("OCR:", self.index_skip_ocr_cb)

        self.index_stage_combo = QComboBox()
        self.index_stage_combo.addItems(["all", "metadata", "small", "large"])
        stage_val = str(self.config.get("index_default_stage", "all")).strip().lower()
        stage_idx = self.index_stage_combo.findText(stage_val)
        self.index_stage_combo.setCurrentIndex(stage_idx if stage_idx >= 0 else 0)
        self.index_stage_combo.setToolTip(
            "Кратко: этап индексации по умолчанию.\n"
            "Подробно: all=полный пайплайн; metadata=только имена/пути; "
            "small=быстрый контент; large=крупные и тяжелые файлы."
        )
        form.addRow("Stage по умолчанию:", self.index_stage_combo)

        # Telegram
        section_tg = QLabel("Telegram")
        section_tg.setStyleSheet("font-weight:700; color:#0b3b8f; padding:8px 0 4px 0;")
        form.addRow(section_tg)

        self.tg_enabled_cb = QCheckBox("Включить Telegram-бота")
        self.tg_enabled_cb.setChecked(bool(self.config.get("telegram_enabled", False)))
        form.addRow("Telegram:", self.tg_enabled_cb)

        self.tg_token_edit = QLineEdit(self.config.get("telegram_bot_token", ""))
        self.tg_token_edit.setPlaceholderText("Токен от @BotFather")
        self.tg_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Bot token:", self.tg_token_edit)

        self.tg_chat_edit = QLineEdit(self.config.get("telegram_allowed_chat_id", ""))
        self.tg_chat_edit.setPlaceholderText("chat_id (опционально)")
        form.addRow("Allowed chat_id:", self.tg_chat_edit)

        self.tg_link_edit = QLineEdit(self.config.get("telegram_bot_link", ""))
        self.tg_link_edit.setPlaceholderText("https://t.me/your_bot")
        form.addRow("Bot link:", self.tg_link_edit)

        form_widget = QWidget()
        form_widget.setLayout(form)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(form_widget)
        layout.addWidget(scroll, 1)
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
        self.config["telemetry_db_path"] = self.telemetry_db_edit.text().strip()
        self.config["users_db_path"] = self.users_db_edit.text().strip()
        self.config["chunk_size"] = int(self.chunk_size_spin.value())
        self.config["chunk_overlap"] = int(self.chunk_overlap_spin.value())
        self.config["batch_size"] = int(self.batch_size_spin.value())
        self.config["index_read_workers"] = int(self.index_workers_spin.value())
        self.config["index_max_chunks"] = int(self.index_max_chunks_spin.value())
        self.config["index_skip_ocr"] = bool(self.index_skip_ocr_cb.isChecked())
        self.config["index_default_stage"] = self.index_stage_combo.currentText().strip()
        self.config["telegram_enabled"] = self.tg_enabled_cb.isChecked()
        self.config["telegram_bot_token"] = self.tg_token_edit.text().strip()
        self.config["telegram_allowed_chat_id"] = self.tg_chat_edit.text().strip()
        self.config["telegram_bot_link"] = self.tg_link_edit.text().strip()
        return self.config


# ════════════════════════════ RAGWindow ═════════════════════════════════

class RAGWindow(QMainWindow):
    """Главное окно приложения."""

    def __init__(self) -> None:
        super().__init__()
        self.cfg = load_config()
        self.searcher: Optional[RAGSearcher] = None
        self._search_thread: Optional[SearchThread] = None
        self._last_query: str = ""
        self._hint_items: List[Dict[str, str]] = []
        self._hint_terms: Set[str] = set()
        self._hints_loaded = False
        self._live_results_enabled = True
        self._live_timer = QTimer(self)
        self._live_timer.setSingleShot(True)
        self._live_timer.setInterval(250)
        self._live_timer.timeout.connect(self._run_live_filename_search)
        self._build_ui()
        self._build_menu()
        self._init_searcher()

    # ── UI ────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        self.setWindowTitle(APP_TITLE)
        if APP_ICON_PATH.exists():
            self.setWindowIcon(QIcon(str(APP_ICON_PATH)))
        self.setGeometry(90, 80, 1280, 860)

        self.setStyleSheet(
            """
            QMainWindow { background: #f3f6fb; }
            QWidget { color: #0f172a; font-family: 'Segoe UI'; }
            QLineEdit, QComboBox, QSpinBox {
                padding: 8px 10px;
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                background: #ffffff;
                color: #0f172a;
                font-size: 10pt;
            }
            QLineEdit:focus, QComboBox:focus, QSpinBox:focus {
                border: 1px solid #3b82f6;
            }
            QComboBox::drop-down {
                border: none;
                width: 22px;
            }
            QComboBox QAbstractItemView {
                background: #ffffff;
                color: #0f172a;
                selection-background-color: #dbeafe;
                selection-color: #0b3b8f;
                border: 1px solid #dbe2ee;
            }
            QPushButton {
                background: #2563eb;
                color: #ffffff;
                border: none;
                padding: 8px 14px;
                border-radius: 8px;
                font-weight: 600;
                font-size: 9.5pt;
            }
            QPushButton:hover    { background: #1d4ed8; }
            QPushButton:pressed  { background: #1e40af; }
            QPushButton:disabled { background: #94a3b8; color:#e2e8f0; }
            QPushButton#quickPreset {
                background: #e2e8f0;
                color: #1e293b;
                border: 1px solid #cbd5e1;
                padding: 6px 10px;
                font-weight: 500;
            }
            QPushButton#quickPreset:hover { background: #dbeafe; border:1px solid #93c5fd; }
            QLabel#titleLabel { color:#0b1220; font-size: 17pt; font-weight: 700; }
            QLabel#subtitleLabel { color:#475569; font-size: 9.5pt; }
            QScrollArea { border: none; background: transparent; }
            QCheckBox { color:#1e293b; font-size:9.5pt; }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
            }
            QStatusBar {
                background: #ffffff;
                color: #334155;
                border-top: 1px solid #dbe2ee;
            }
            QMenuBar {
                background: #ffffff;
                color: #0f172a;
                border-bottom: 1px solid #dbe2ee;
            }
            QMenuBar::item {
                background: transparent;
                color: #0f172a;
                padding: 6px 10px;
            }
            QMenuBar::item:selected {
                background: #eaf2ff;
                color: #0b3b8f;
                border-radius: 6px;
            }
            QMenu {
                background: #ffffff;
                color: #0f172a;
                border: 1px solid #dbe2ee;
                padding: 4px;
            }
            QMenu::item {
                padding: 7px 12px;
                border-radius: 6px;
            }
            QMenu::item:selected {
                background: #eaf2ff;
                color: #0b3b8f;
            }
            QMenu::separator {
                height: 1px;
                background: #e2e8f0;
                margin: 4px 6px;
            }
            """
        )

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(16, 14, 16, 8)
        root.setSpacing(10)

        # Заголовок
        title_row = QHBoxLayout()
        title_icon = QLabel()
        if APP_ICON_PATH.exists():
            title_icon.setPixmap(QIcon(str(APP_ICON_PATH)).pixmap(28, 28))
        title_row.addWidget(title_icon)
        title_lbl = QLabel("RAG Каталог")
        title_lbl.setObjectName("titleLabel")
        tf = QFont()
        tf.setPointSize(14)
        tf.setBold(True)
        title_lbl.setFont(tf)
        title_row.addWidget(title_lbl)
        title_row.addStretch()
        root.addLayout(title_row)
        subtitle_lbl = QLabel(
            "Найдите документ, контекст и нужный факт по естественному запросу."
        )
        subtitle_lbl.setObjectName("subtitleLabel")
        root.addWidget(subtitle_lbl)

        # Поисковая строка
        search_row = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Введите запрос: договоры, паспорта, счета, масса PC300…")
        self.search_input.returnPressed.connect(self._do_search)
        self.search_input.textChanged.connect(self._on_query_text_changed)
        search_row.addWidget(self.search_input)

        self.search_btn = QPushButton("Поиск")
        self.search_btn.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogContentsView))
        self.search_btn.setFixedWidth(120)
        self.search_btn.clicked.connect(self._do_search)
        search_row.addWidget(self.search_btn)
        root.addLayout(search_row)

        self.did_you_mean_lbl = QLabel("")
        self.did_you_mean_lbl.setVisible(False)
        self.did_you_mean_lbl.setStyleSheet(
            "background:#fff7ed; color:#9a3412; border:1px solid #fed7aa; "
            "padding:6px 10px; border-radius:8px; font-size:9pt;"
        )
        self.did_you_mean_lbl.setWordWrap(True)
        root.addWidget(self.did_you_mean_lbl)

        self.live_hint_lbl = QLabel("")
        self.live_hint_lbl.setVisible(False)
        self.live_hint_lbl.setStyleSheet(
            "background:#eef6ff; color:#1e3a8a; border:1px solid #cfe1ff; "
            "padding:6px 10px; border-radius:8px; font-size:9pt;"
        )
        self.live_hint_lbl.setWordWrap(True)
        root.addWidget(self.live_hint_lbl)

        # Фильтры
        filters_row = QHBoxLayout()
        filters_label = QLabel("Фильтры:")
        filters_label.setStyleSheet("color:#64748b; font-weight:600;")
        filters_row.addWidget(filters_label)
        filters_row.addSpacing(8)
        filters_row.addWidget(QLabel("Результатов"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(5, 50)
        self.limit_spin.setValue(10)
        self.limit_spin.setSingleStep(5)
        self.limit_spin.setMaximumWidth(75)
        filters_row.addWidget(self.limit_spin)
        filters_row.addSpacing(14)

        filters_row.addWidget(QLabel("Тип файла"))
        self.filetype_combo = QComboBox()
        self.filetype_combo.addItems(["Все", ".docx", ".xlsx", ".xls", ".pdf"])
        self.filetype_combo.setMaximumWidth(110)
        filters_row.addWidget(self.filetype_combo)
        filters_row.addSpacing(14)

        self.content_only_cb = QCheckBox("Только содержимое")
        filters_row.addWidget(self.content_only_cb)
        filters_row.addStretch()
        root.addLayout(filters_row)

        # Кнопки быстрого поиска
        quick_row = QHBoxLayout()
        quick_label = QLabel("Быстрый поиск:")
        quick_label.setStyleSheet("color:#64748b; font-weight:600;")
        quick_row.addWidget(quick_label)
        quick_presets = [
            ("Договоры",  "договоры", QStyle.StandardPixmap.SP_FileIcon),
            ("Паспорта",   "паспорта", QStyle.StandardPixmap.SP_FileDialogDetailedView),
            ("Счета",      "счета на оплату", QStyle.StandardPixmap.SP_FileDialogListView),
            ("Записки",    "служебная записка", QStyle.StandardPixmap.SP_FileIcon),
            ("Финансовые", "финансовый отчёт", QStyle.StandardPixmap.SP_DriveHDIcon),
            ("Юридические","юридический", QStyle.StandardPixmap.SP_DialogApplyButton),
        ]
        for label, qry, icon_id in quick_presets:
            btn = QPushButton(label)
            btn.setIcon(self.style().standardIcon(icon_id))
            btn.setObjectName("quickPreset")
            btn.setMaximumWidth(120)
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
        self.scroll.setStyleSheet(
            "QScrollArea { border: none; background: #f3f6fb; }"
            "QScrollArea > QWidget > QWidget { background: #f3f6fb; }"
        )
        self.results_container = QWidget()
        self.results_container.setStyleSheet("background:#f3f6fb;")
        self.results_layout = QVBoxLayout(self.results_container)
        self.results_layout.setSpacing(8)
        self.results_layout.setContentsMargins(2, 2, 2, 10)
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
        settings_action = file_menu.addAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView),
            "Настройки…",
        )
        settings_action.triggered.connect(self._open_settings)
        file_menu.addSeparator()
        exit_action = file_menu.addAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_DialogCloseButton),
            "Выход",
        )
        exit_action.triggered.connect(self.close)

        help_menu = bar.addMenu("Справка")
        about_action = help_menu.addAction(
            self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxInformation),
            "О программе",
        )
        about_action.triggered.connect(self._show_about)

    # ── searcher init ─────────────────────────────────────────────────

    def _init_searcher(self) -> None:
        try:
            self.searcher = RAGSearcher(self.cfg)
            if self.searcher.connected:
                stats = self.searcher.get_collection_stats()
                pts = stats.get("points_count", 0)
                self.statusBar().showMessage(f"Подключено к RAG  |  Точек: {pts:,}")
                self._conn_lbl.setText("Подключено")
                self._conn_lbl.setStyleSheet(
                    "background:#d4edda; color:#155724; padding:3px 8px; border-radius:3px;"
                )
                self._load_hint_cache()
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
            self.statusBar().showMessage(f"Ошибка: {exc}")
            QMessageBox.critical(self, "Ошибка инициализации", str(exc))

    def _set_disconnected(self) -> None:
        self.statusBar().showMessage("Нет подключения к RAG")
        self._conn_lbl.setText("Отключено")
        self._conn_lbl.setStyleSheet(
            "background:#f8d7da; color:#721c24; padding:3px 8px; border-radius:3px;"
        )

    def _split_terms(self, text: str) -> Set[str]:
        return {
            t.lower()
            for t in re.findall(r"[a-zа-я0-9\-]{3,}", text or "", flags=re.IGNORECASE)
            if any(ch.isalpha() for ch in t)
        }

    def _load_hint_cache(self, max_points: int = 15000) -> None:
        if self._hints_loaded or not self.searcher or not self.searcher.connected:
            return
        try:
            points_left = max_points
            offset = None
            seen: Set[str] = set()
            hint_items: List[Dict[str, str]] = []
            hint_terms: Set[str] = set()
            qdrant = self.searcher.qdrant
            while points_left > 0:
                batch_limit = 500 if points_left > 500 else points_left
                points, offset = qdrant.scroll(
                    collection_name=self.searcher.collection_name,
                    limit=batch_limit,
                    offset=offset,
                    with_payload=["filename", "path", "full_path", "type", "text"],
                    with_vectors=False,
                )
                if not points:
                    break
                for pt in points:
                    payload = pt.payload or {}
                    filename = str(payload.get("filename") or "").strip()
                    path = str(payload.get("path") or "").strip()
                    full_path = str(payload.get("full_path") or "").strip()
                    key = (full_path or f"{path}\\{filename}").lower().strip("\\")
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    source = full_path or path
                    folder = ""
                    if source:
                        try:
                            folder = str(PurePath(source).parent)
                        except Exception:
                            folder = source
                    item = {
                        "filename": filename,
                        "path": path,
                        "full_path": full_path,
                        "folder": folder,
                    }
                    hint_items.append(item)
                    bag = " ".join([filename, path, full_path, folder])
                    hint_terms.update(self._split_terms(bag))
                points_left -= len(points)
                if offset is None:
                    break
            self._hint_items = hint_items
            self._hint_terms = hint_terms
            self._hints_loaded = True
            logger.info(
                "Загружен кеш подсказок: files=%s terms=%s",
                len(self._hint_items),
                len(self._hint_terms),
            )
        except Exception as exc:
            logger.warning("Не удалось загрузить кеш подсказок: %s", exc)

    def _build_did_you_mean(self, query: str) -> List[str]:
        self._load_hint_cache()
        if not self._hint_terms:
            return []
        suggestions: List[str] = []
        for token in self._split_terms(query):
            if len(token) < 4 or token in self._hint_terms:
                continue
            for match in get_close_matches(token, list(self._hint_terms), n=2, cutoff=0.78):
                if match not in suggestions:
                    suggestions.append(match)
            if len(suggestions) >= 3:
                break
        return suggestions[:3]

    def _on_query_text_changed(self, _text: str) -> None:
        if not self._live_results_enabled:
            return
        self._live_timer.stop()
        q = self.search_input.text().strip()
        if len(q) < 2:
            self.live_hint_lbl.setVisible(False)
            return
        self._live_timer.start()

    def _run_live_filename_search(self) -> None:
        query = self.search_input.text().strip()
        if len(query) < 2:
            self.live_hint_lbl.setVisible(False)
            return
        self._load_hint_cache()
        if not self._hint_items:
            self.live_hint_lbl.setVisible(False)
            return

        q = query.lower()
        tokens = self._split_terms(q)
        scored: List[Tuple[int, Dict[str, str]]] = []
        for item in self._hint_items:
            filename = (item.get("filename") or "").lower()
            path = (item.get("path") or item.get("full_path") or "").lower()
            bag = f"{filename} {path}"
            score = 0
            if q in filename:
                score += 8
            if q in path:
                score += 4
            for t in tokens:
                if t in filename:
                    score += 4
                elif t in path:
                    score += 2
            if score > 0:
                scored.append((score, item))

        if not scored:
            self.live_hint_lbl.setVisible(False)
            return
        scored.sort(
            key=lambda pair: (
                -pair[0],
                (pair[1].get("filename") or "").lower(),
                (pair[1].get("path") or "").lower(),
            )
        )
        top = scored[:6]
        preview_parts: List[str] = []
        for _, item in top:
            fn = item.get("filename") or "(без имени)"
            p = item.get("path") or item.get("full_path") or ""
            preview_parts.append(f"{fn} [{p}]")
        self.live_hint_lbl.setText("Мгновенно по именам: " + "  •  ".join(preview_parts))
        self.live_hint_lbl.setVisible(True)

    def _classify_result(self, result: Dict[str, Any]) -> str:
        filename = str(result.get("filename") or "").lower()
        path = str(result.get("path") or result.get("full_path") or "").lower()
        text = str(result.get("text") or "").lower()[:3000]
        bag = " ".join([filename, path, text])

        if any(k in bag for k in ("птс", "псм", "техпаспорт", "тех паспорт", "паспорт тс")):
            return "Техпаспорта ТС (ПТС/ПСМ)"
        if "паспорт" in bag or "пасп " in f"{bag} " or "пасп\\" in bag:
            return "Личные паспорта и удостоверения"
        if any(k in bag for k in ("договор", "соглашени", "контракт", "акт")):
            return "Договоры и соглашения"
        if any(k in bag for k in ("счет", "инвойс", "накладн")):
            return "Счета и платежные документы"
        if any(k in bag for k in ("отчет", "баланс", "финанс")):
            return "Финансовые документы"
        if any(k in bag for k in ("приказ", "доверен", "заявлен", "юрист", "протокол")):
            return "Юридические и служебные"
        ext = str(result.get("extension") or "").lower()
        if ext in {".xlsx", ".xls"}:
            return "Таблицы и реестры"
        if ext == ".pdf":
            return "PDF документы"
        return "Прочие документы"

    def _group_by_parent_catalog(self, path: str) -> str:
        raw = (path or "").replace("/", "\\").strip("\\")
        if not raw:
            return "Без каталога"
        parts = [p for p in raw.split("\\") if p]
        if not parts:
            return "Без каталога"
        lowered = [p.lower() for p in parts]
        for idx, p in enumerate(lowered):
            if "пасп" in p:
                return parts[idx]
            if any(x in p for x in ("птс", "псм", "техпас", "транспорт")):
                return parts[idx]
        return parts[0]

    def _extract_folder_hits(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        folder_map: Dict[str, Dict[str, Any]] = {}
        for r in results:
            src = str(r.get("full_path") or r.get("path") or "").strip()
            if not src:
                continue
            try:
                folder = str(PurePath(src).parent)
            except Exception:
                folder = src
            if not folder or folder == ".":
                continue
            score = float(r.get("score") or 0.0)
            if folder not in folder_map:
                folder_map[folder] = {
                    "folder_path": folder,
                    "score": score,
                    "sample": str(r.get("filename") or ""),
                }
            else:
                folder_map[folder]["score"] = max(folder_map[folder]["score"], score)
        query = self._last_query.lower().strip()
        if query:
            self._load_hint_cache()
            for item in self._hint_items:
                folder = str(item.get("folder") or "").strip()
                if not folder:
                    continue
                bag = folder.lower()
                if query in bag and folder not in folder_map:
                    folder_map[folder] = {
                        "folder_path": folder,
                        "score": 0.4,
                        "sample": str(item.get("filename") or ""),
                    }
        rows = list(folder_map.values())
        rows.sort(key=lambda x: (-float(x.get("score") or 0.0), str(x.get("folder_path") or "")))
        return rows[:8]

    def _group_results(self, results: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for r in results:
            group = self._classify_result(r)
            path = str(r.get("path") or r.get("full_path") or "")
            parent_catalog = self._group_by_parent_catalog(path)
            grouped[group][parent_catalog].append(r)
        return grouped

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
        self._last_query = query

        self._set_searching(True)
        self._clear_results()
        self.did_you_mean_lbl.setVisible(False)

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
        self._run_live_filename_search()

        did_you_mean = self._build_did_you_mean(self._last_query)
        if did_you_mean:
            self.did_you_mean_lbl.setText(
                "Возможно, вы имели в виду: " + ", ".join(did_you_mean)
            )
            self.did_you_mean_lbl.setVisible(True)
        else:
            self.did_you_mean_lbl.setVisible(False)

        if not results:
            self.statusBar().showMessage("Ничего не найдено")
            folder_hits = self._extract_folder_hits([])
            if folder_hits:
                placeholder = QLabel("По документам совпадений нет, но найдены каталоги:")
                placeholder.setStyleSheet("color:#475569; padding:10px 4px; font-size:10pt;")
                self.results_layout.addWidget(placeholder)
                folder_section = GroupSection("Каталоги", len(folder_hits))
                for row in folder_hits:
                    folder_section.add_widget(
                        FolderCard(
                            folder_path=str(row.get("folder_path") or ""),
                            score=float(row.get("score") or 0.0),
                            sample=str(row.get("sample") or ""),
                        )
                    )
                self.results_layout.addWidget(folder_section)
            else:
                placeholder = QLabel("По вашему запросу ничего не найдено.")
                placeholder.setStyleSheet("color:#999; padding:20px; font-size:11pt;")
                self.results_layout.addWidget(placeholder)
        else:
            self.statusBar().showMessage(f"Найдено результатов: {len(results)}")
            folder_hits = self._extract_folder_hits(results)
            if folder_hits:
                folder_section = GroupSection("Каталоги", len(folder_hits))
                for row in folder_hits:
                    folder_section.add_widget(
                        FolderCard(
                            folder_path=str(row.get("folder_path") or ""),
                            score=float(row.get("score") or 0.0),
                            sample=str(row.get("sample") or ""),
                        )
                    )
                self.results_layout.addWidget(folder_section)

            grouped = self._group_results(results)
            group_order = [
                "Техпаспорта ТС (ПТС/ПСМ)",
                "Личные паспорта и удостоверения",
                "Договоры и соглашения",
                "Счета и платежные документы",
                "Финансовые документы",
                "Юридические и служебные",
                "Таблицы и реестры",
                "PDF документы",
                "Прочие документы",
            ]
            rank = {name: i for i, name in enumerate(group_order)}
            sorted_groups = sorted(
                grouped.items(),
                key=lambda kv: rank.get(kv[0], 999),
            )
            idx = 1
            for group_name, by_catalog in sorted_groups:
                total = sum(len(items) for items in by_catalog.values())
                section = GroupSection(group_name, total)
                for catalog, items in sorted(
                    by_catalog.items(),
                    key=lambda kv: (-len(kv[1]), kv[0].lower()),
                ):
                    catalog_lbl = QLabel(f"Каталог: {catalog}")
                    catalog_lbl.setStyleSheet(
                        "color:#334155; font-size:9pt; font-weight:600; padding:2px 2px;"
                    )
                    section.add_widget(catalog_lbl)
                    for r in sorted(
                        items,
                        key=lambda x: float(x.get("score") or 0.0),
                        reverse=True,
                    ):
                        section.add_widget(ResultCard(r, idx))
                        idx += 1
                self.results_layout.addWidget(section)
        self.results_layout.addStretch()

    def _on_search_error(self, error: str) -> None:
        self._set_searching(False)
        self._search_thread = None
        self.statusBar().showMessage(f"Ошибка: {error}")
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
    if APP_ICON_PATH.exists():
        app.setWindowIcon(QIcon(str(APP_ICON_PATH)))
    window = RAGWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
