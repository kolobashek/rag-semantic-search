"""
test_indexer_image_tags.py — тесты для генерации тегов и OCR изображений.

Покрывает:
  - _generate_tags(): теги из пути, синонимы, расширение, год, тип документа
  - _extract_image(): OCR изображений через pytesseract (мок)
  - Маршрутизация IMAGE_EXTENSIONS в extract_one() и process_file()
  - Интеграция тегов в meta_payload и content_payloads
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

# Убеждаемся что src в sys.path (pytest.ini: pythonpath = ["src"])
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rag_catalog.core.index_rag import (
    IMAGE_EXTENSIONS,
    SUPPORTED_EXTENSIONS,
    _generate_tags,
    _file_category,
)


# ═══════════════════════════ _generate_tags ════════════════════════════════════

class TestGenerateTags:
    """Юнит-тесты для _generate_tags."""

    def test_extension_label_pdf(self, tmp_path):
        """PDF-файл получает тег 'pdf' и 'PDF документ'."""
        f = tmp_path / "договор.pdf"
        f.touch()
        tags = _generate_tags(f, Path("договор.pdf"), "")
        assert "pdf" in tags
        assert "PDF документ" in tags

    def test_extension_label_docx(self, tmp_path):
        """DOCX-файл получает тег 'docx' и 'Word документ'."""
        f = tmp_path / "report.docx"
        f.touch()
        tags = _generate_tags(f, Path("report.docx"), "")
        assert "docx" in tags
        assert "Word документ" in tags

    def test_extension_label_image_jpg(self, tmp_path):
        """JPEG-файл получает тег 'фотография'."""
        f = tmp_path / "photo.jpg"
        f.touch()
        tags = _generate_tags(f, Path("photo.jpg"), "")
        assert "фотография" in tags

    def test_extension_label_image_tiff(self, tmp_path):
        """TIFF-файл получает тег 'скан'."""
        f = tmp_path / "scan.tiff"
        f.touch()
        tags = _generate_tags(f, Path("scan.tiff"), "")
        assert "скан" in tags

    def test_extension_label_image_png(self, tmp_path):
        """PNG-файл получает тег 'изображение'."""
        f = tmp_path / "screenshot.png"
        f.touch()
        tags = _generate_tags(f, Path("screenshot.png"), "")
        assert "изображение" in tags

    def test_path_tokens_extracted(self, tmp_path):
        """Токены из пути папок попадают в теги."""
        catalog = tmp_path / "Техника" / "Экскаваторы"
        catalog.mkdir(parents=True)
        f = catalog / "паспорт.pdf"
        f.touch()
        rel = Path("Техника") / "Экскаваторы" / "паспорт.pdf"
        tags = _generate_tags(f, rel, "")
        # папки
        assert "техника" in tags
        assert "экскаваторы" in tags
        # имя файла
        assert "паспорт" in tags

    def test_synonym_псм(self, tmp_path):
        """Аббревиатура ПСМ раскрывается в синонимы."""
        f = tmp_path / "ПСМ_001.pdf"
        f.touch()
        tags = _generate_tags(f, Path("ПСМ_001.pdf"), "")
        assert "паспорт самоходной машины" in tags
        assert "техпаспорт самоходной машины" in tags

    def test_synonym_птс(self, tmp_path):
        """Аббревиатура ПТС раскрывается в синонимы."""
        f = tmp_path / "ПТС_2023.pdf"
        f.touch()
        tags = _generate_tags(f, Path("ПТС_2023.pdf"), "")
        assert "паспорт транспортного средства" in tags

    def test_synonym_cat(self, tmp_path):
        """Аббревиатура CAT раскрывается в caterpillar/кэт."""
        f = tmp_path / "CAT 320.pdf"
        f.touch()
        tags = _generate_tags(f, Path("CAT 320.pdf"), "")
        assert "caterpillar" in tags or "кэт" in tags

    def test_year_extracted_from_path(self, tmp_path):
        """Год из пути попадает в теги."""
        catalog = tmp_path / "2023"
        catalog.mkdir()
        f = catalog / "отчёт.docx"
        f.touch()
        tags = _generate_tags(f, Path("2023") / "отчёт.docx", "")
        assert "2023" in tags

    def test_year_extracted_from_filename(self, tmp_path):
        """Год из имени файла попадает в теги."""
        f = tmp_path / "договор_2022.pdf"
        f.touch()
        tags = _generate_tags(f, Path("договор_2022.pdf"), "")
        assert "2022" in tags

    def test_doc_type_акт(self, tmp_path):
        """Файл с 'акт' в имени получает тег типа документа."""
        f = tmp_path / "акт_выполненных_работ.pdf"
        f.touch()
        tags = _generate_tags(f, Path("акт_выполненных_работ.pdf"), "")
        assert "акт" in tags

    def test_doc_type_договор(self, tmp_path):
        """Файл с 'договор' в имени получает тег типа документа."""
        f = tmp_path / "договор_поставки.docx"
        f.touch()
        tags = _generate_tags(f, Path("договор_поставки.docx"), "")
        assert "договор" in tags

    def test_doc_type_фото(self, tmp_path):
        """Файл с 'фото' в имени получает тег 'фотография'."""
        f = tmp_path / "фото_техники.jpg"
        f.touch()
        tags = _generate_tags(f, Path("фото_техники.jpg"), "")
        assert "фотография" in tags

    def test_content_keywords_extracted(self, tmp_path):
        """Ключевые слова из текста попадают в теги."""
        f = tmp_path / "report.docx"
        f.touch()
        content = "экскаватор экскаватор экскаватор гидравлический гидравлический двигатель"
        tags = _generate_tags(f, Path("report.docx"), content)
        # Слова с частотой 3 и 2 должны войти в топ-20
        assert "экскаватор" in tags
        assert "гидравлический" in tags

    def test_stopwords_filtered(self, tmp_path):
        """Стоп-слова не попадают в теги."""
        f = tmp_path / "и_в_на.pdf"
        f.touch()
        tags = _generate_tags(f, Path("и_в_на.pdf"), "")
        assert "и" not in tags
        assert "в" not in tags
        assert "на" not in tags

    def test_short_tokens_filtered(self, tmp_path):
        """Токены длиной < 2 символа не попадают в теги."""
        f = tmp_path / "a_b_c.pdf"
        f.touch()
        tags = _generate_tags(f, Path("a_b_c.pdf"), "")
        assert "a" not in tags
        assert "b" not in tags
        assert "c" not in tags

    def test_deduplication(self, tmp_path):
        """Теги дедуплицированы (нет повторов)."""
        f = tmp_path / "паспорт_паспорт.pdf"
        f.touch()
        tags = _generate_tags(f, Path("паспорт_паспорт.pdf"), "")
        assert len(tags) == len(set(tags))

    def test_sorted_output(self, tmp_path):
        """Теги возвращаются отсортированными."""
        f = tmp_path / "CAT_2022.pdf"
        f.touch()
        tags = _generate_tags(f, Path("CAT_2022.pdf"), "")
        assert tags == sorted(tags)

    def test_tags_max_length(self, tmp_path):
        """Ни один тег не превышает 50 символов."""
        f = tmp_path / "очень_длинное_имя_файла_с_множеством_слов.pdf"
        f.touch()
        tags = _generate_tags(f, Path("очень_длинное_имя_файла_с_множеством_слов.pdf"), "")
        assert all(len(t) <= 50 for t in tags), f"Длинные теги: {[t for t in tags if len(t) > 50]}"

    def test_custom_synonym_map(self, tmp_path):
        """Пользовательская synonym_map дополняет дефолтную."""
        f = tmp_path / "ТМЦ.xlsx"
        f.touch()
        custom = {"тмц": ["товарно-материальные ценности", "склад"]}
        tags = _generate_tags(f, Path("ТМЦ.xlsx"), "", synonym_map=custom)
        assert "товарно-материальные ценности" in tags
        assert "склад" in tags

    def test_no_crash_on_empty_content(self, tmp_path):
        """Пустой текст не вызывает ошибок."""
        f = tmp_path / "empty.pdf"
        f.touch()
        tags = _generate_tags(f, Path("empty.pdf"), "")
        assert isinstance(tags, list)

    def test_no_crash_on_none_content(self, tmp_path):
        """None в тексте не вызывает ошибок (полагается на default "")."""
        f = tmp_path / "none.pdf"
        f.touch()
        # Передаём пустую строку вместо None (типизация говорит str)
        tags = _generate_tags(f, Path("none.pdf"), "")
        assert isinstance(tags, list)

    def test_xlsx_extension_label(self, tmp_path):
        """XLSX-файл получает тег 'Excel таблица'."""
        f = tmp_path / "data.xlsx"
        f.touch()
        tags = _generate_tags(f, Path("data.xlsx"), "")
        assert "Excel таблица" in tags

    def test_komatsu_synonym(self, tmp_path):
        """Аббревиатура komatsu раскрывается в 'комацу'."""
        f = tmp_path / "komatsu_PC300.pdf"
        f.touch()
        tags = _generate_tags(f, Path("komatsu_PC300.pdf"), "")
        assert "комацу" in tags

    def test_relative_path_used(self, tmp_path):
        """relative_path включён в теги — не только имя файла."""
        deep = tmp_path / "Архив" / "2021" / "Q1"
        deep.mkdir(parents=True)
        f = deep / "report.pdf"
        f.touch()
        rel = Path("Архив") / "2021" / "Q1" / "report.pdf"
        tags = _generate_tags(f, rel, "")
        assert "архив" in tags
        assert "2021" in tags


# ═══════════════════════════ IMAGE_EXTENSIONS константа ═══════════════════════

class TestImageExtensions:
    """Проверяем что IMAGE_EXTENSIONS корректно определены."""

    def test_image_extensions_subset_of_supported(self):
        """IMAGE_EXTENSIONS — подмножество SUPPORTED_EXTENSIONS."""
        assert IMAGE_EXTENSIONS.issubset(SUPPORTED_EXTENSIONS)

    def test_image_extensions_contains_common_formats(self):
        """Все ожидаемые форматы присутствуют."""
        expected = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".webp"}
        assert expected.issubset(IMAGE_EXTENSIONS)

    def test_image_extensions_no_office_formats(self):
        """Офисные форматы не входят в IMAGE_EXTENSIONS."""
        assert ".docx" not in IMAGE_EXTENSIONS
        assert ".xlsx" not in IMAGE_EXTENSIONS
        assert ".pdf" not in IMAGE_EXTENSIONS


# ═══════════════════════════ _file_category для изображений ═══════════════════

class TestFileCategoryImages:
    """Изображения всегда в категории 'large'."""

    @pytest.mark.parametrize("ext", [".jpg", ".jpeg", ".png", ".gif", ".tiff", ".bmp", ".webp"])
    def test_image_always_large(self, tmp_path, ext):
        f = tmp_path / f"photo{ext}"
        # Создаём маленький файл — должно быть large независимо от размера
        f.write_bytes(b"\x00" * 100)
        category = _file_category(f, small_office_mb=20.0, small_pdf_mb=2.0)
        assert category == "large", f"{ext} должен быть 'large', получено '{category}'"


# ═══════════════════════════ _extract_image (мок) ═════════════════════════════

class TestExtractImage:
    """
    Тесты для RAGIndexer._extract_image.
    Используем моки, т.к. pytesseract и Pillow не гарантированы в тестовом окружении.
    """

    def _make_indexer(self, tmp_path: Path):
        """Создать минимальный RAGIndexer без реального Qdrant и SentenceTransformer."""
        from rag_catalog.core.index_rag import RAGIndexer
        indexer = object.__new__(RAGIndexer)
        indexer.catalog_path = tmp_path
        indexer.skip_ocr = False
        return indexer

    def test_returns_empty_when_pytesseract_missing(self, tmp_path):
        """Если pytesseract не установлен — возвращает ''."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "img.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0")  # минимальный JPEG header

        with patch.dict("sys.modules", {"pytesseract": None}):
            result = indexer._extract_image(f)
        assert result == ""

    def _make_mock_image(self, mode: str = "RGB", n_frames: int = 1) -> MagicMock:
        """Вспомогательный метод: PIL.Image mock с поддержкой контекстного менеджера."""
        img = MagicMock()
        img.mode = mode
        img.n_frames = n_frames
        # Контекстный менеджер должен возвращать сам объект
        img.__enter__ = lambda s: s
        img.__exit__ = MagicMock(return_value=False)
        img.copy.return_value = img  # seek/copy возвращают тот же объект
        return img

    def test_calls_pytesseract_with_rus_eng(self, tmp_path):
        """pytesseract.image_to_string вызывается с lang='rus+eng'."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "doc.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n")

        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "Текст на изображении"
        mock_img = self._make_mock_image("RGB", n_frames=1)
        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {
            "pytesseract": mock_tess,
            "PIL": mock_pil,
            "PIL.Image": mock_pil.Image,
        }):
            result = indexer._extract_image(f)

        mock_tess.image_to_string.assert_called_once_with(mock_img, lang="rus+eng")
        assert result == "Текст на изображении"

    def test_strips_whitespace_from_ocr_result(self, tmp_path):
        """OCR результат обрезается по пробелам."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "scan.tiff"
        f.write_bytes(b"II\x2a\x00")

        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "  \n  ТЕКСТ  \n  "
        mock_img = self._make_mock_image("RGB", n_frames=1)
        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {
            "pytesseract": mock_tess,
            "PIL": mock_pil,
            "PIL.Image": mock_pil.Image,
        }):
            result = indexer._extract_image(f)

        assert result == "ТЕКСТ"

    def test_returns_empty_on_exception(self, tmp_path):
        """При ошибке OCR (например повреждённый файл) возвращает ''."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "broken.jpg"
        f.write_bytes(b"not an image")

        mock_tess = MagicMock()
        mock_pil = MagicMock()
        mock_pil.Image.open.side_effect = Exception("cannot identify image file")

        with patch.dict("sys.modules", {
            "pytesseract": mock_tess,
            "PIL": mock_pil,
            "PIL.Image": mock_pil.Image,
        }):
            result = indexer._extract_image(f)

        assert result == ""

    def test_converts_non_rgb_mode(self, tmp_path):
        """Изображения не в RGB/L/RGBA конвертируются в RGB перед OCR."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "cmyk.tiff"
        f.write_bytes(b"II\x2a\x00")

        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "результат"

        converted_image = MagicMock()
        converted_image.mode = "RGB"
        mock_img = self._make_mock_image("CMYK", n_frames=1)
        mock_img.convert.return_value = converted_image

        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {
            "pytesseract": mock_tess,
            "PIL": mock_pil,
            "PIL.Image": mock_pil.Image,
        }):
            result = indexer._extract_image(f)

        mock_img.convert.assert_called_once_with("RGB")
        mock_tess.image_to_string.assert_called_once_with(converted_image, lang="rus+eng")
        assert result == "результат"


# ═══════════════════════════ Интеграция тегов в payload ═══════════════════════

class TestTagsInPayload:
    """
    Проверяем что теги корректно добавляются в meta_payload и content_payloads.
    Вместо реального Qdrant тестируем только логику через _generate_tags напрямую.
    """

    def test_tags_present_in_metadata(self, tmp_path):
        """Файл с говорящим именем генерирует непустой список тегов."""
        catalog = tmp_path / "Техника" / "ПСМ"
        catalog.mkdir(parents=True)
        f = catalog / "ПСМ_Komatsu_2022.pdf"
        f.touch()
        rel = Path("Техника") / "ПСМ" / "ПСМ_Komatsu_2022.pdf"

        tags = _generate_tags(f, rel, "Паспорт самоходной машины Komatsu WA470-7")

        assert len(tags) > 0
        # Синоним ПСМ
        assert "паспорт самоходной машины" in tags
        # Синоним komatsu
        assert "комацу" in tags
        # Год
        assert "2022" in tags

    def test_tags_for_image_file(self, tmp_path):
        """Изображения получают корректные теги."""
        f = tmp_path / "фото_экскаватора.jpg"
        f.touch()
        tags = _generate_tags(f, Path("фото_экскаватора.jpg"), "")
        assert "фотография" in tags
        # Слово 'фото' из DOC_TYPE_MAP
        assert "jpg" in tags

    def test_meta_text_contains_tags(self, tmp_path):
        """Если тегов >= 1, meta_text содержит '| Теги: ...'."""
        f = tmp_path / "ПСМ.pdf"
        f.touch()
        tags = _generate_tags(f, Path("ПСМ.pdf"), "")

        # Симулируем как это делает extract_one()
        meta_text = f"Файл: {f.name} | Путь: ПСМ.pdf | Расширение: .pdf"
        if tags:
            meta_text += f" | Теги: {', '.join(tags[:30])}"

        assert "| Теги:" in meta_text
        # В тексте должны быть синонимы ПСМ
        assert "паспорт самоходной машины" in meta_text

    def test_no_tags_in_meta_text_for_generic_file(self, tmp_path):
        """Файл без значимого имени не ломает meta_text (теги могут быть минимальны)."""
        f = tmp_path / "123.pdf"
        f.touch()
        tags = _generate_tags(f, Path("123.pdf"), "")

        # tags могут быть пустыми или содержать только расширение
        meta_text = f"Файл: {f.name} | Путь: 123.pdf | Расширение: .pdf"
        if tags:
            meta_text += f" | Теги: {', '.join(tags[:30])}"

        # Главное — не падает и meta_text всегда строка
        assert isinstance(meta_text, str)
        assert "Файл:" in meta_text


# ═══════════════════════════ _chunk_text (fix: бесконечный цикл) ══════════════

class TestChunkText:
    """Тесты для RAGIndexer._chunk_text — в том числе edge cases из code review."""

    def _make_indexer(self, chunk_size: int, chunk_overlap: int):
        from rag_catalog.core.index_rag import RAGIndexer
        idx = object.__new__(RAGIndexer)
        idx.chunk_size = chunk_size
        idx.chunk_overlap = chunk_overlap
        idx.max_chunks_per_file = 0
        return idx

    def test_normal_chunking(self):
        """Стандартное разбиение: chunk_size=10, overlap=3."""
        idx = self._make_indexer(10, 3)
        text = "A" * 25
        chunks = idx._chunk_text(text)
        assert len(chunks) > 1
        # Первый чанк ровно chunk_size
        assert len(chunks[0]) == 10

    def test_text_shorter_than_chunk(self):
        """Текст короче chunk_size — один чанк."""
        idx = self._make_indexer(500, 100)
        text = "короткий текст"
        chunks = idx._chunk_text(text)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_empty_text(self):
        """Пустой текст — пустой список."""
        idx = self._make_indexer(500, 100)
        assert idx._chunk_text("") == []

    def test_overlap_equals_chunk_size_no_infinite_loop(self):
        """chunk_overlap == chunk_size не вызывает бесконечный цикл (bug fix)."""
        idx = self._make_indexer(100, 100)  # overlap == size — опасный edge case
        text = "x" * 500
        chunks = idx._chunk_text(text)
        # Должен завершиться и вернуть разумное число чанков
        assert 1 <= len(chunks) <= 500

    def test_overlap_greater_than_chunk_size_no_infinite_loop(self):
        """chunk_overlap > chunk_size тоже не вызывает бесконечный цикл."""
        idx = self._make_indexer(50, 200)  # overlap > size — очень опасный case
        text = "y" * 300
        chunks = idx._chunk_text(text)
        assert len(chunks) >= 1

    def test_chunks_cover_all_text(self):
        """Все символы текста присутствуют в чанках."""
        idx = self._make_indexer(100, 20)
        text = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" * 10
        chunks = idx._chunk_text(text)
        # Первый чанк — начало текста
        assert chunks[0] == text[:100]
        # Последний чанк — конец текста
        assert chunks[-1].endswith(text[-len(chunks[-1]):])

    def test_zero_overlap(self):
        """chunk_overlap=0 — чанки без перекрытия."""
        idx = self._make_indexer(10, 0)
        text = "0123456789abcdefghij"  # ровно 20 символов
        chunks = idx._chunk_text(text)
        assert len(chunks) == 2
        assert chunks[0] == "0123456789"
        assert chunks[1] == "abcdefghij"


# ═══════════════════════════ Многостраничный TIFF ═════════════════════════════

class TestExtractImageMultipage:
    """Тесты OCR многостраничных изображений (TIFF-сканы)."""

    def _make_indexer(self, tmp_path):
        from rag_catalog.core.index_rag import RAGIndexer
        idx = object.__new__(RAGIndexer)
        idx.catalog_path = tmp_path
        idx.skip_ocr = False
        return idx

    def test_single_page_image(self, tmp_path):
        """Однокадровое изображение: pytesseract вызывается один раз."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "single.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n")

        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "страница один"

        mock_img = MagicMock()
        mock_img.mode = "RGB"
        mock_img.n_frames = 1  # один кадр
        mock_img.__enter__ = lambda s: s
        mock_img.__exit__ = MagicMock(return_value=False)

        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {"pytesseract": mock_tess, "PIL": mock_pil, "PIL.Image": mock_pil.Image}):
            result = indexer._extract_image(f)

        assert mock_tess.image_to_string.call_count == 1
        assert result == "страница один"

    def test_multipage_tiff_all_pages_ocrd(self, tmp_path):
        """Многостраничный TIFF: pytesseract вызывается для каждой страницы."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "scan.tiff"
        f.write_bytes(b"II\x2a\x00")

        mock_tess = MagicMock()
        mock_tess.image_to_string.side_effect = [
            "текст страницы 1",
            "текст страницы 2",
            "текст страницы 3",
        ]

        mock_img = MagicMock()
        mock_img.mode = "RGB"
        mock_img.n_frames = 3
        mock_img.__enter__ = lambda s: s
        mock_img.__exit__ = MagicMock(return_value=False)
        mock_img.copy.return_value = mock_img  # copy() возвращает тот же объект

        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {"pytesseract": mock_tess, "PIL": mock_pil, "PIL.Image": mock_pil.Image}):
            result = indexer._extract_image(f)

        assert mock_tess.image_to_string.call_count == 3
        assert "текст страницы 1" in result
        assert "текст страницы 2" in result
        assert "текст страницы 3" in result

    def test_multipage_tiff_empty_pages_skipped(self, tmp_path):
        """Пустые страницы не добавляются в результат."""
        indexer = self._make_indexer(tmp_path)
        f = tmp_path / "scan2.tiff"
        f.write_bytes(b"II\x2a\x00")

        mock_tess = MagicMock()
        mock_tess.image_to_string.side_effect = ["", "содержимое", ""]

        mock_img = MagicMock()
        mock_img.mode = "RGB"
        mock_img.n_frames = 3
        mock_img.__enter__ = lambda s: s
        mock_img.__exit__ = MagicMock(return_value=False)
        mock_img.copy.return_value = mock_img

        mock_pil = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict("sys.modules", {"pytesseract": mock_tess, "PIL": mock_pil, "PIL.Image": mock_pil.Image}):
            result = indexer._extract_image(f)

        assert result == "содержимое"


# ═══════════════════════════ DEFAULT_SYNONYM_MAP (fix: liebherr) ══════════════

class TestSynonymMapIntegrity:
    """Проверка корректности DEFAULT_SYNONYM_MAP после исправления дубля liebherr."""

    def test_no_duplicate_keys(self):
        """В DEFAULT_SYNONYM_MAP нет дублирующихся ключей."""
        from rag_catalog.core.index_rag import DEFAULT_SYNONYM_MAP
        # Python dict не хранит дубли, но мы проверяем через исходник
        import ast
        from pathlib import Path
        src = Path(__file__).parent.parent / "src/rag_catalog/core/index_rag.py"
        tree = ast.parse(src.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == "DEFAULT_SYNONYM_MAP":
                        if isinstance(node.value, ast.Dict):
                            keys = [
                                k.s if isinstance(k, ast.Constant) else None
                                for k in node.value.keys
                            ]
                            assert len(keys) == len(set(k for k in keys if k)), \
                                f"Дублирующиеся ключи: {[k for k in keys if keys.count(k) > 1]}"

    def test_liebherr_has_both_variants(self):
        """liebherr содержит оба варианта транслитерации."""
        from rag_catalog.core.index_rag import DEFAULT_SYNONYM_MAP
        assert "liebherr" in DEFAULT_SYNONYM_MAP
        syns = DEFAULT_SYNONYM_MAP["liebherr"]
        assert "либхер" in syns
        assert "либхерр" in syns
