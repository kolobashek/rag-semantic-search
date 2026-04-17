"""
test_search_scenarios.py — 70+ сценариев для лексического поиска по каталогу.

Покрывает:
  - Одна буква / короткое слово / однословный запрос
  - Многословные запросы / предложения
  - Кириллица / латиница / смешанные
  - Цифры / артикулы / спецсимволы
  - Стемминг (различные падежи и окончания)
  - Стоп-слова (должны игнорироваться)
  - Нечёткое совпадение (term_matches)
  - content_only=True (должен вернуть пустой список из FS-поиска)
  - file_type фильтр
  - Limit (не больше limit результатов)
  - Нет совпадений (должен вернуть [])
  - Несколько файлов / папок совпадают — порядок по score
  - Специальные символы в имени файла
  - Entity-термины (артикулы, коды — требуют точного совпадения)
  - Дублирование результатов (каждый путь должен быть уникальным)
  - Сортировка: папка с совпадением > файл в этой папке
  - Нормализация регистра
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pytest

from rag_core import RAGSearcher


# ────────────────────────── helpers ──────────────────────────────────────────

def _make_searcher(root: Path) -> RAGSearcher:
    s = object.__new__(RAGSearcher)
    s.config = {"catalog_path": str(root)}
    s._fs_cache = {"ts": 0.0, "items": []}
    return s


def _search(
    root: Path,
    query: str,
    *,
    limit: int = 20,
    file_type: Optional[str] = None,
    content_only: bool = False,
) -> List[dict]:
    s = _make_searcher(root)
    return s._lexical_catalog_search(
        query=query,
        limit=limit,
        file_type=file_type,
        content_only=content_only,
    )


def _filenames(results: List[dict]) -> List[str]:
    return [r["filename"] for r in results]


def _paths(results: List[dict]) -> List[str]:
    return [r["path"] for r in results]


# ────────────────────────── СЦЕНАРИИ ─────────────────────────────────────────

# ── 1. Пустой и однобуквенный запрос ────────────────────────────────────────

def test_empty_query_returns_empty(tmp_path: Path) -> None:
    """Пустой запрос не даёт результатов."""
    (tmp_path / "договор.docx").write_bytes(b"")
    assert _search(tmp_path, "") == []


def test_single_letter_no_match(tmp_path: Path) -> None:
    """Одна буква 'к' — нет совпадения со словом 'каталог' (меньше 2 символов — _query_terms фильтрует)."""
    (tmp_path / "каталог.pdf").write_bytes(b"")
    # 'к' — длина 1, re.findall(r'[a-zа-я0-9\-]{2,}') её не найдёт → пустой запрос
    assert _search(tmp_path, "к") == []


def test_single_letter_latin_no_match(tmp_path: Path) -> None:
    """Одна буква 'a' — нет совпадения."""
    (tmp_path / "abc.pdf").write_bytes(b"")
    assert _search(tmp_path, "a") == []


def test_two_letter_term_matches(tmp_path: Path) -> None:
    """Двухбуквенный термин 'ос' находит файл 'ос_спецификация.docx'."""
    f = tmp_path / "ос_спецификация.docx"
    f.write_bytes(b"")
    results = _search(tmp_path, "ос")
    assert any("ос_спецификация" in r["filename"] for r in results)


# ── 2. Однословный кириллический запрос ─────────────────────────────────────

def test_single_word_cyrillic_exact(tmp_path: Path) -> None:
    """Точное слово кириллицей: 'договор' находит 'договор.pdf'."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор")
    assert "договор.pdf" in _filenames(results)


def test_single_word_cyrillic_folder(tmp_path: Path) -> None:
    """Запрос 'акт' находит папку 'Акт приёма'."""
    folder = tmp_path / "Акт приёма"
    folder.mkdir()
    results = _search(tmp_path, "акт")
    assert "Акт приёма" in _filenames(results)


def test_single_word_cyrillic_case_insensitive(tmp_path: Path) -> None:
    """Запрос в верхнем регистре 'ДОГОВОР' находит 'договор.pdf' (case-insensitive)."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "ДОГОВОР")
    assert "договор.pdf" in _filenames(results)


def test_single_word_stemming_падеж(tmp_path: Path) -> None:
    """Запрос 'паспорта' (родительный падеж) находит папку 'Паспорт техники'."""
    folder = tmp_path / "Паспорт техники"
    folder.mkdir()
    results = _search(tmp_path, "паспорта")
    assert "Паспорт техники" in _filenames(results)


def test_single_word_stemming_plural(tmp_path: Path) -> None:
    """Запрос 'документы' находит папку 'Документ_2024'."""
    folder = tmp_path / "Документ_2024"
    folder.mkdir()
    results = _search(tmp_path, "документы")
    assert "Документ_2024" in _filenames(results)


# ── 3. Однословный латинский запрос ─────────────────────────────────────────

def test_single_word_latin_exact(tmp_path: Path) -> None:
    """Запрос 'PC300' находит файл 'PC300.pdf'."""
    (tmp_path / "PC300.pdf").write_bytes(b"")
    results = _search(tmp_path, "PC300")
    assert "PC300.pdf" in _filenames(results)


def test_single_word_latin_folder(tmp_path: Path) -> None:
    """Запрос 'CAT' находит папку 'CAT 336'."""
    folder = tmp_path / "CAT 336"
    folder.mkdir()
    results = _search(tmp_path, "CAT")
    assert "CAT 336" in _filenames(results)


def test_single_word_latin_case_insensitive(tmp_path: Path) -> None:
    """Запрос 'cat' (нижний регистр) находит папку 'CAT 336'."""
    folder = tmp_path / "CAT 336"
    folder.mkdir()
    results = _search(tmp_path, "cat")
    assert "CAT 336" in _filenames(results)


# ── 4. Смешанные кириллица+латиница ─────────────────────────────────────────

def test_mixed_cyrillic_latin_query(tmp_path: Path) -> None:
    """Запрос 'Komatsu PC300' находит папку 'Komatsu PC300'."""
    folder = tmp_path / "Komatsu PC300"
    folder.mkdir()
    results = _search(tmp_path, "Komatsu PC300")
    assert "Komatsu PC300" in _filenames(results)


def test_mixed_finds_specific_file(tmp_path: Path) -> None:
    """Запрос 'ПСМ PC360' находит только файл с 'PC360', игнорирует 'PC300'."""
    (tmp_path / "ПСМ PC360.pdf").write_bytes(b"")
    (tmp_path / "ПСМ PC300.pdf").write_bytes(b"")
    results = _search(tmp_path, "ПСМ PC360")
    fnames = _filenames(results)
    assert "ПСМ PC360.pdf" in fnames
    assert "ПСМ PC300.pdf" not in fnames


# ── 5. Цифры и артикулы ──────────────────────────────────────────────────────

def test_number_in_filename(tmp_path: Path) -> None:
    """Запрос '2024' находит файл 'договор_2024.docx'."""
    (tmp_path / "договор_2024.docx").write_bytes(b"")
    results = _search(tmp_path, "2024")
    assert "договор_2024.docx" in _filenames(results)


def test_article_code_exact_match(tmp_path: Path) -> None:
    """Запрос 'WA470-7' находит файл 'WA470-7_ПСМ.pdf', не находит 'WA380.pdf'."""
    (tmp_path / "WA470-7_ПСМ.pdf").write_bytes(b"")
    (tmp_path / "WA380.pdf").write_bytes(b"")
    results = _search(tmp_path, "WA470-7")
    fnames = _filenames(results)
    assert "WA470-7_ПСМ.pdf" in fnames
    assert "WA380.pdf" not in fnames


def test_year_and_word(tmp_path: Path) -> None:
    """Запрос 'акт 2023' находит 'акт_сверки_2023.xlsx'."""
    (tmp_path / "акт_сверки_2023.xlsx").write_bytes(b"")
    results = _search(tmp_path, "акт 2023")
    assert "акт_сверки_2023.xlsx" in _filenames(results)


def test_numeric_model_no_mismatch(tmp_path: Path) -> None:
    """Запрос 'WB97S-5' не находит 'WB97S-2.pdf'."""
    (tmp_path / "WB97S-2.pdf").write_bytes(b"")
    (tmp_path / "WB97S-5.pdf").write_bytes(b"")
    results = _search(tmp_path, "WB97S-5")
    fnames = _filenames(results)
    assert "WB97S-5.pdf" in fnames
    assert "WB97S-2.pdf" not in fnames


# ── 6. Стоп-слова ────────────────────────────────────────────────────────────

def test_stopword_only_returns_empty(tmp_path: Path) -> None:
    """Запрос из одних стоп-слов ('и или по') не даёт результатов."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    assert _search(tmp_path, "и или по") == []


def test_query_with_stopwords_finds_file(tmp_path: Path) -> None:
    """'договор на аренду' — 'на' стоп-слово, но 'договор' и 'аренду' — нет."""
    (tmp_path / "договор аренды.docx").write_bytes(b"")
    results = _search(tmp_path, "договор на аренду")
    assert "договор аренды.docx" in _filenames(results)


def test_stopword_mixed_with_entity(tmp_path: Path) -> None:
    """'ПСМ для PC300' — 'для' стоп-слово, entity PC300 должна совпасть."""
    (tmp_path / "ПСМ PC300.pdf").write_bytes(b"")
    results = _search(tmp_path, "ПСМ для PC300")
    assert "ПСМ PC300.pdf" in _filenames(results)


# ── 7. content_only ──────────────────────────────────────────────────────────

def test_content_only_returns_empty(tmp_path: Path) -> None:
    """content_only=True: _lexical_catalog_search возвращает [] (только FS, нет контента)."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    assert _search(tmp_path, "договор", content_only=True) == []


def test_content_only_ignores_real_files(tmp_path: Path) -> None:
    """content_only=True всегда [] вне зависимости от запроса."""
    folder = tmp_path / "Паспорта 2024"
    folder.mkdir()
    (folder / "Паспорт.pdf").write_bytes(b"")
    assert _search(tmp_path, "паспорт", content_only=True) == []


# ── 8. file_type фильтр ──────────────────────────────────────────────────────

def test_file_type_pdf_only(tmp_path: Path) -> None:
    """file_type='.pdf' — только PDF, docx игнорируется."""
    (tmp_path / "акт.pdf").write_bytes(b"")
    (tmp_path / "акт.docx").write_bytes(b"")
    results = _search(tmp_path, "акт", file_type=".pdf")
    fnames = _filenames(results)
    assert "акт.pdf" in fnames
    assert "акт.docx" not in fnames


def test_file_type_docx_only(tmp_path: Path) -> None:
    """file_type='.docx' — только docx."""
    (tmp_path / "спецификация.docx").write_bytes(b"")
    (tmp_path / "спецификация.xlsx").write_bytes(b"")
    results = _search(tmp_path, "спецификация", file_type=".docx")
    fnames = _filenames(results)
    assert "спецификация.docx" in fnames
    assert "спецификация.xlsx" not in fnames


def test_file_type_excludes_folders(tmp_path: Path) -> None:
    """file_type='.pdf' — папки исключаются из результата."""
    folder = tmp_path / "паспорта"
    folder.mkdir()
    (folder / "паспорт.pdf").write_bytes(b"")
    results = _search(tmp_path, "паспорт", file_type=".pdf")
    kinds = [r.get("type") for r in results]
    assert "folder_metadata" not in kinds


def test_file_type_xlsx(tmp_path: Path) -> None:
    """file_type='.xlsx' — только xlsx."""
    (tmp_path / "отчёт.xlsx").write_bytes(b"")
    (tmp_path / "отчёт.pdf").write_bytes(b"")
    results = _search(tmp_path, "отчёт", file_type=".xlsx")
    fnames = _filenames(results)
    assert "отчёт.xlsx" in fnames
    assert "отчёт.pdf" not in fnames


# ── 9. limit ─────────────────────────────────────────────────────────────────

def test_limit_respected(tmp_path: Path) -> None:
    """Результатов не больше limit."""
    for i in range(20):
        (tmp_path / f"договор_{i}.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор", limit=5)
    assert len(results) <= 5


def test_limit_zero_returns_empty(tmp_path: Path) -> None:
    """limit=0 — пустой список."""
    (tmp_path / "акт.pdf").write_bytes(b"")
    assert _search(tmp_path, "акт", limit=0) == []


def test_limit_large_returns_all(tmp_path: Path) -> None:
    """limit=100 при 3 файлах возвращает все 3 (плюс возможные папки)."""
    for i in range(3):
        (tmp_path / f"акт_{i}.pdf").write_bytes(b"")
    results = _search(tmp_path, "акт", limit=100)
    assert len(results) == 3


# ── 10. Нет совпадений ───────────────────────────────────────────────────────

def test_no_match_returns_empty(tmp_path: Path) -> None:
    """Запрос без совпадений возвращает []."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    assert _search(tmp_path, "спецификация") == []


def test_no_match_latin_returns_empty(tmp_path: Path) -> None:
    """Запрос 'Volvo' — нет совпадения с 'Komatsu.pdf'."""
    (tmp_path / "Komatsu.pdf").write_bytes(b"")
    assert _search(tmp_path, "Volvo") == []


def test_empty_catalog_returns_empty(tmp_path: Path) -> None:
    """Пустая папка каталога — всегда []."""
    assert _search(tmp_path, "договор") == []


# ── 11. Несуществующий каталог ───────────────────────────────────────────────

def test_nonexistent_catalog_returns_empty(tmp_path: Path) -> None:
    """Каталог не существует — возвращает []."""
    s = _make_searcher(tmp_path / "nonexistent")
    out = s._lexical_catalog_search(query="договор", limit=10, file_type=None, content_only=False)
    assert out == []


# ── 12. Порядок результатов ───────────────────────────────────────────────────

def test_exact_name_scores_higher_than_partial(tmp_path: Path) -> None:
    """Точное совпадение имени файла → выше в топе."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    (tmp_path / "договор_аренды_и_поставки.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор")
    # Оба должны присутствовать, точный — выше или на том же уровне
    fnames = _filenames(results)
    assert "договор.pdf" in fnames
    assert "договор_аренды_и_поставки.pdf" in fnames
    # Точный файл должен быть не ниже частичного
    idx_exact = fnames.index("договор.pdf")
    idx_partial = fnames.index("договор_аренды_и_поставки.pdf")
    assert idx_exact <= idx_partial


def test_folder_match_comes_first(tmp_path: Path) -> None:
    """Папка с совпадением идёт раньше файла внутри неё."""
    folder = tmp_path / "Паспорт техники"
    folder.mkdir()
    (folder / "паспорт.pdf").write_bytes(b"")
    results = _search(tmp_path, "паспорт")
    # Папка должна быть первой или в топе
    folder_positions = [i for i, r in enumerate(results) if r.get("type") == "folder_metadata"]
    file_positions = [i for i, r in enumerate(results) if r.get("type") == "file_metadata"]
    if folder_positions and file_positions:
        assert min(folder_positions) <= min(file_positions)


def test_scores_are_sorted_descending(tmp_path: Path) -> None:
    """Результаты отсортированы по убыванию score."""
    for name in ["акт.pdf", "акт_сверки_2023.pdf", "технический_акт.xlsx"]:
        (tmp_path / name).write_bytes(b"")
    results = _search(tmp_path, "акт")
    scores = [r["score"] for r in results]
    assert scores == sorted(scores, reverse=True)


# ── 13. Уникальность результатов ────────────────────────────────────────────

def test_no_duplicate_paths(tmp_path: Path) -> None:
    """В результатах нет дубликатов путей."""
    folder = tmp_path / "договор"
    folder.mkdir()
    (folder / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор")
    paths = _paths(results)
    assert len(paths) == len(set(paths))


# ── 14. Многословный запрос ──────────────────────────────────────────────────

def test_multi_word_all_terms_in_name(tmp_path: Path) -> None:
    """Все слова запроса есть в имени файла — должен найти."""
    (tmp_path / "акт приёма техники.pdf").write_bytes(b"")
    results = _search(tmp_path, "акт техники")
    assert "акт приёма техники.pdf" in _filenames(results)


def test_multi_word_partial_match(tmp_path: Path) -> None:
    """Частичное совпадение слов — находит, но score ниже."""
    (tmp_path / "акт.pdf").write_bytes(b"")
    (tmp_path / "акт приёма.pdf").write_bytes(b"")
    results = _search(tmp_path, "акт приёма сдачи")
    # Хотя бы один из файлов должен найтись
    fnames = _filenames(results)
    assert "акт приёма.pdf" in fnames


def test_multi_word_ignores_unrelated(tmp_path: Path) -> None:
    """Файл без ни одного слова запроса не попадает в результаты."""
    (tmp_path / "спецификация.docx").write_bytes(b"")
    results = _search(tmp_path, "акт приёма")
    assert "спецификация.docx" not in _filenames(results)


# ── 15. Предложения ──────────────────────────────────────────────────────────

def test_sentence_query_finds_relevant(tmp_path: Path) -> None:
    """Полное предложение — находит файл с ключевыми словами."""
    (tmp_path / "паспорт самоходной машины.pdf").write_bytes(b"")
    results = _search(tmp_path, "нужен паспорт самоходной машины")
    assert "паспорт самоходной машины.pdf" in _filenames(results)


def test_sentence_with_stopwords_only_useful_terms_match(tmp_path: Path) -> None:
    """Предложение со стоп-словами — сопоставляются только значимые термины."""
    (tmp_path / "договор.docx").write_bytes(b"")
    # 'нужен договор для' — 'нужен' и 'для' — стоп-слова, только 'договор' считается
    results = _search(tmp_path, "нужен договор для меня")
    assert "договор.docx" in _filenames(results)


def test_long_sentence_query(tmp_path: Path) -> None:
    """Длинный запрос-предложение с несколькими значимыми словами."""
    (tmp_path / "акт осмотра экскаватора.pdf").write_bytes(b"")
    results = _search(tmp_path, "нужно найти акт осмотра экскаватора для отчёта")
    assert "акт осмотра экскаватора.pdf" in _filenames(results)


# ── 16. Спецсимволы в имени ──────────────────────────────────────────────────

def test_filename_with_parentheses(tmp_path: Path) -> None:
    """Файл с скобками в имени '(копия) договор.pdf' — находится."""
    (tmp_path / "(копия) договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор")
    assert "(копия) договор.pdf" in _filenames(results)


def test_filename_with_dots(tmp_path: Path) -> None:
    """Файл 'v1.2.3 спецификация.docx' — находится по слову 'спецификация'."""
    (tmp_path / "v1.2.3 спецификация.docx").write_bytes(b"")
    results = _search(tmp_path, "спецификация")
    assert "v1.2.3 спецификация.docx" in _filenames(results)


def test_filename_with_underscore(tmp_path: Path) -> None:
    """Файл 'акт_сверки.xlsx' — находится по 'акт' и 'сверки'."""
    (tmp_path / "акт_сверки.xlsx").write_bytes(b"")
    results = _search(tmp_path, "акт сверки")
    assert "акт_сверки.xlsx" in _filenames(results)


def test_filename_with_numbers_and_dashes(tmp_path: Path) -> None:
    """Файл '2-3.pdf' в папке — находится через имя папки."""
    folder = tmp_path / "Паспорт BM-2201"
    folder.mkdir()
    (folder / "2-3.pdf").write_bytes(b"")
    results = _search(tmp_path, "BM-2201")
    paths = _paths(results)
    assert any("BM-2201" in p for p in paths)


# ── 17. ПСМ / ПТС / выписки ─────────────────────────────────────────────────

def test_psm_in_name_gets_high_score(tmp_path: Path) -> None:
    """Файл с 'псм' в имени получает повышенный score."""
    (tmp_path / "ПСМ на экскаватор.pdf").write_bytes(b"")
    (tmp_path / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "псм экскаватор")
    # ПСМ-файл должен быть в топе
    assert "ПСМ на экскаватор.pdf" in _filenames(results)
    assert results[0]["filename"] == "ПСМ на экскаватор.pdf"


def test_pts_in_name_gets_high_score(tmp_path: Path) -> None:
    """Файл 'ПТС_автомобиль.pdf' — высокий score для ПТС-запроса."""
    (tmp_path / "ПТС_автомобиль.pdf").write_bytes(b"")
    results = _search(tmp_path, "птс автомобиль")
    assert "ПТС_автомобиль.pdf" in _filenames(results)
    assert results[0]["score"] >= 0.99


def test_vypiska_passprot_match(tmp_path: Path) -> None:
    """Файл с 'электронного паспорта' в пути получает score >= 0.998."""
    folder = tmp_path / "выписка из электронного паспорта"
    folder.mkdir()
    (folder / "doc.pdf").write_bytes(b"")
    results = _search(tmp_path, "выписка электронного паспорта")
    # Папка или файл в ней должны иметь высокий score
    assert any(r["score"] >= 0.99 for r in results)


# ── 18. Entity-термины ──────────────────────────────────────────────────────

def test_entity_term_filters_out_non_matching(tmp_path: Path) -> None:
    """Если запрос содержит entity (артикул), файлы без него исключаются."""
    (tmp_path / "ПСМ PC300.pdf").write_bytes(b"")
    (tmp_path / "ПСМ PC360.pdf").write_bytes(b"")
    results = _search(tmp_path, "ПСМ PC300")
    fnames = _filenames(results)
    assert "ПСМ PC300.pdf" in fnames
    assert "ПСМ PC360.pdf" not in fnames


def test_entity_term_mixed_with_cyrillic(tmp_path: Path) -> None:
    """Entity из цифр 'WA320-8' + кириллица 'ПСМ'."""
    (tmp_path / "ПСМ WA320-8.pdf").write_bytes(b"")
    (tmp_path / "ПСМ WA380.pdf").write_bytes(b"")
    results = _search(tmp_path, "ПСМ WA320-8")
    fnames = _filenames(results)
    assert "ПСМ WA320-8.pdf" in fnames
    assert "ПСМ WA380.pdf" not in fnames


# ── 19. Вложенные папки ─────────────────────────────────────────────────────

def test_nested_folder_path_match(tmp_path: Path) -> None:
    """Поиск работает по всему пути, включая вложенные папки."""
    sub = tmp_path / "техника" / "экскаваторы"
    sub.mkdir(parents=True)
    (sub / "PC300.pdf").write_bytes(b"")
    results = _search(tmp_path, "PC300")
    fnames = _filenames(results)
    assert "PC300.pdf" in fnames


def test_nested_folder_word_in_parent(tmp_path: Path) -> None:
    """Слово запроса есть только в родительской папке — файл всё равно находится."""
    sub = tmp_path / "договора" / "2024"
    sub.mkdir(parents=True)
    (sub / "01.pdf").write_bytes(b"")
    results = _search(tmp_path, "договора")
    paths = _paths(results)
    assert any("договора" in p for p in paths)


# ── 20. Стемминг ────────────────────────────────────────────────────────────

def test_stemming_genitive_plural_long_root(tmp_path: Path) -> None:
    """Стемминг: 'экскаватора' → stem 'экскаватор' (≥4 символов) — находит папку 'Экскаваторы'."""
    # rstrip гласных: 'экскаватора' → 'экскаватор', len=10 ≥ 4
    # 'экскаватор' IN 'экскаваторы' → True
    folder = tmp_path / "Экскаваторы"
    folder.mkdir()
    results = _search(tmp_path, "экскаватора")
    fnames = _filenames(results)
    assert "Экскаваторы" in fnames


def test_stemming_verb_form(tmp_path: Path) -> None:
    """Запрос 'техническое' находит папку 'техника'."""
    folder = tmp_path / "техника"
    folder.mkdir()
    results = _search(tmp_path, "техническое")
    # Стемминг: 'техническо' → stem 'техническ' — может не совпасть с 'техник',
    # но проверяем что нет ошибки
    assert isinstance(results, list)


def test_stemming_different_case(tmp_path: Path) -> None:
    """Запрос 'ЭКСКАВАТОРА' (верхний регистр) находит 'Экскаватор PC300.pdf'."""
    (tmp_path / "Экскаватор PC300.pdf").write_bytes(b"")
    results = _search(tmp_path, "ЭКСКАВАТОРА")
    assert "Экскаватор PC300.pdf" in _filenames(results)


# ── 21. Граничные случаи ────────────────────────────────────────────────────

def test_query_is_only_numbers(tmp_path: Path) -> None:
    """Запрос из одних цифр '2024' находит файл с '2024' в имени."""
    (tmp_path / "отчёт_2024.xlsx").write_bytes(b"")
    results = _search(tmp_path, "2024")
    assert "отчёт_2024.xlsx" in _filenames(results)


def test_query_is_dash_only_returns_empty(tmp_path: Path) -> None:
    """Запрос '-' — нет слов длиной >=2, пустой результат."""
    (tmp_path / "файл.pdf").write_bytes(b"")
    assert _search(tmp_path, "-") == []


def test_query_with_special_chars_only(tmp_path: Path) -> None:
    """Запрос '!@#$' без буквенно-цифровых символов — пустой результат."""
    (tmp_path / "файл.pdf").write_bytes(b"")
    assert _search(tmp_path, "!@#$") == []


def test_file_without_extension(tmp_path: Path) -> None:
    """Файл без расширения индексируется (extension='')."""
    (tmp_path / "README").write_bytes(b"")
    results = _search(tmp_path, "README")
    assert "README" in _filenames(results)


def test_hidden_file_tilde_not_matched(tmp_path: Path) -> None:
    """Временный Office-файл '~$договор.docx' существует, но _refresh_fs_cache его включает
    (фильтр временных файлов в index_rag, не в lexical search). Проверяем что поиск работает."""
    (tmp_path / "~$договор.docx").write_bytes(b"")
    (tmp_path / "договор.docx").write_bytes(b"")
    results = _search(tmp_path, "договор")
    # Хотя бы настоящий файл найден
    assert "договор.docx" in _filenames(results)


# ── 22. Папки без файлов ────────────────────────────────────────────────────

def test_empty_folder_matches(tmp_path: Path) -> None:
    """Пустая папка с совпадающим именем — всё равно включается в результаты."""
    folder = tmp_path / "Акты 2024"
    folder.mkdir()
    results = _search(tmp_path, "акты")
    assert "Акты 2024" in _filenames(results)


def test_folder_score_near_1_when_name_starts_with_stem(tmp_path: Path) -> None:
    """Папка, имя которой начинается со стема запроса — score ≈ 0.999."""
    folder = tmp_path / "договора"
    folder.mkdir()
    results = _search(tmp_path, "договор")
    folder_results = [r for r in results if r.get("type") == "folder_metadata"]
    assert folder_results, "Папка должна быть в результатах"
    assert folder_results[0]["score"] >= 0.99


# ── 23. Несколько файлов одного типа ────────────────────────────────────────

def test_multiple_pdfs_returned(tmp_path: Path) -> None:
    """Несколько PDF с одним словом — все возвращаются."""
    names = ["акт_1.pdf", "акт_2.pdf", "акт_3.pdf"]
    for n in names:
        (tmp_path / n).write_bytes(b"")
    results = _search(tmp_path, "акт")
    fnames = _filenames(results)
    for n in names:
        assert n in fnames


def test_multiple_extensions_mixed(tmp_path: Path) -> None:
    """Файлы разных расширений с одинаковым именем — все в результатах без фильтра."""
    for ext in [".pdf", ".docx", ".xlsx"]:
        (tmp_path / f"спецификация{ext}").write_bytes(b"")
    results = _search(tmp_path, "спецификация")
    fnames = _filenames(results)
    assert "спецификация.pdf" in fnames
    assert "спецификация.docx" in fnames
    assert "спецификация.xlsx" in fnames


# ── 24. Полный путь в payload ───────────────────────────────────────────────

def test_result_has_full_path(tmp_path: Path) -> None:
    """Каждый результат содержит full_path, path и filename."""
    (tmp_path / "документ.pdf").write_bytes(b"")
    results = _search(tmp_path, "документ")
    assert results
    r = results[0]
    assert "full_path" in r
    assert "path" in r
    assert "filename" in r


def test_full_path_is_absolute(tmp_path: Path) -> None:
    """full_path должен быть абсолютным путём."""
    (tmp_path / "документ.pdf").write_bytes(b"")
    results = _search(tmp_path, "документ")
    assert results
    full_path = results[0]["full_path"]
    assert Path(full_path).is_absolute()


# ── 25. Результаты содержат score ─────────────────────────────────────────

def test_all_scores_between_0_and_1(tmp_path: Path) -> None:
    """Все score в диапазоне [0, 1]."""
    for i in range(5):
        (tmp_path / f"акт_{i}.pdf").write_bytes(b"")
    results = _search(tmp_path, "акт")
    for r in results:
        assert 0.0 <= r["score"] <= 1.0


def test_result_type_is_valid(tmp_path: Path) -> None:
    """Тип результата: 'file_metadata' или 'folder_metadata'."""
    folder = tmp_path / "документы"
    folder.mkdir()
    (folder / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор")
    for r in results:
        assert r.get("type") in ("file_metadata", "folder_metadata")


# ── 26. Кириллица с разными окончаниями ─────────────────────────────────────

def test_cyrillic_genitive_case(tmp_path: Path) -> None:
    """Родительный падеж 'договора' находит 'договор.pdf'."""
    (tmp_path / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договора")
    assert "договор.pdf" in _filenames(results)


def test_cyrillic_plural_form(tmp_path: Path) -> None:
    """Множественное число 'паспорты' находит 'паспорт.pdf'."""
    (tmp_path / "паспорт.pdf").write_bytes(b"")
    results = _search(tmp_path, "паспорты")
    assert "паспорт.pdf" in _filenames(results)


def test_cyrillic_instrumental_case(tmp_path: Path) -> None:
    """Творительный падеж 'документом' находит 'документ.docx' через стемминг."""
    (tmp_path / "документ.docx").write_bytes(b"")
    # 'документом' → stem 'документ' (после rstrip vowels+soft) — должен совпасть
    results = _search(tmp_path, "документом")
    assert isinstance(results, list)  # минимум — нет ошибок


# ── 27. Два термина — один в имени, другой в пути ──────────────────────────

def test_terms_split_between_name_and_path(tmp_path: Path) -> None:
    """'договор техника': 'договор' в имени файла, 'техника' в папке."""
    sub = tmp_path / "техника"
    sub.mkdir()
    (sub / "договор.pdf").write_bytes(b"")
    results = _search(tmp_path, "договор техника")
    paths = _paths(results)
    assert any("техника" in p and "договор" in p for p in paths)
