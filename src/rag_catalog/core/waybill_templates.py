"""Import a dispatcher’s printable, single-sheet XLSX template into the forms book."""

import re
from copy import copy
from io import BytesIO
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

from openpyxl import Workbook, load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils.cell import range_boundaries
from openpyxl.utils.exceptions import InvalidFileException

MARKER = re.compile(r"\{\{([a-zа-яё][a-zа-яё0-9_]{0,59})\}\}", re.IGNORECASE)
MAX_UPLOAD = 4 * 1024 * 1024
_SHEET_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def workbook_bytes(workbook):
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


def _check_extent(ref):
    left, top, right, bottom = range_boundaries(ref)
    if None in (left, top, right, bottom) or left < 1 or top < 1 or right > 1024 or bottom > 2000:
        raise ValueError("Макет бланка должен помещаться в 2000 строк и 1024 столбца")
    return (right - left + 1) * (bottom - top + 1)


def _check_package(content):
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_UPLOAD:
        raise ValueError("Загрузите книгу XLSX размером до 4 МБ")
    with ZipFile(BytesIO(content)) as package:
        parts = package.infolist()
        if len(parts) > 256 or sum(i.file_size for i in parts) > 16 * 1024 * 1024:
            raise ValueError("Слишком большая распакованная книга")
        names = package.namelist()
        if any(n.startswith(("xl/drawings/", "xl/media/", "xl/embeddings/", "xl/activeX/", "xl/externalLinks/",
                             "xl/pivot", "xl/tables/", "xl/charts/")) or "vbaproject" in n.lower() for n in names):
            raise ValueError("Загрузите макет из ячеек без картинок, макросов, внешних ссылок и внедрённых объектов")
        sheets = [n for n in names if re.fullmatch(r"xl/worksheets/[^/]+\.xml", n)]
        if len(sheets) != 1:
            raise ValueError("В книге должен быть один видимый лист с бланком")
        sheet = ElementTree.fromstring(package.read(sheets[0]))
        if any(sheet.find(f".//{_SHEET_NS}{tag}") is not None for tag in ("f", "conditionalFormatting", "hyperlinks")):
            raise ValueError("Для загрузки сохраните бланк без формул, гиперссылок и условного форматирования")
        for dim in sheet.iter(_SHEET_NS + "dimension"):
            _check_extent(dim.attrib["ref"])
        cells = list(sheet.iter(_SHEET_NS + "c"))
        for cell in cells:
            _check_extent(cell.attrib["r"])
        if len(cells) + sum(_check_extent(m.attrib["ref"]) for m in sheet.iter(_SHEET_NS + "mergeCell")) > 100000:
            raise ValueError("Слишком много ячеек в бланке")


def _copy_style(source, target):
    if source.has_style:
        for attr in ("font", "border", "fill", "alignment", "number_format", "protection"):
            setattr(target, attr, copy(getattr(source, attr)))


def append_sheet(book, source, name):
    """Copy only the printable cells, styles and page layout between workbooks."""
    target = book.create_sheet(name)
    for cell in source._cells.values():
        if isinstance(cell, MergedCell):
            continue
        dest = target.cell(cell.row, cell.column, cell.value)
        dest.data_type = cell.data_type
        _copy_style(cell, dest)
    for key, dimension in source.row_dimensions.items():
        dest = target.row_dimensions[key]
        for attr in ("height", "hidden", "outlineLevel", "collapsed"):
            setattr(dest, attr, getattr(dimension, attr))
        _copy_style(dimension, dest)
    for key, dimension in source.column_dimensions.items():
        dest = target.column_dimensions[key]
        for attr in ("min", "max", "width", "hidden", "outlineLevel", "collapsed", "bestFit"):
            setattr(dest, attr, getattr(dimension, attr))
        _copy_style(dimension, dest)
    for merged in source.merged_cells.ranges:
        target.merge_cells(str(merged))
    for attr in ("sheet_format", "sheet_properties", "page_setup", "page_margins", "print_options",
                 "row_breaks", "col_breaks", "HeaderFooter"):
        setattr(target, attr, copy(getattr(source, attr)))
    target.page_setup.id = None
    if source.print_area:
        target.print_area = [str(area) for area in source._print_area.ranges]
    target.print_title_rows = source.print_title_rows
    target.print_title_cols = source.print_title_cols
    target.sheet_view.showGridLines = source.sheet_view.showGridLines
    return target


def import_template(content, name):
    try:
        _check_package(content)
        source = load_workbook(BytesIO(content), keep_links=False).active
        if source is None or source.sheet_state != "visible":
            raise ValueError("В книге должен быть один видимый лист с бланком")
        if source.print_area:
            for area in source._print_area.ranges:
                _check_extent(str(area))
        keys = {}
        for cell in source._cells.values():
            if isinstance(cell.value, str):
                for key in MARKER.findall(cell.value):
                    keys[key] = key.replace("_", " ").capitalize()
                remainder = MARKER.sub("", cell.value)
                if "{{" in remainder or "}}" in remainder:
                    raise ValueError("Проверьте маркеры полей, например {{маршрут}}")
        if len(keys) > 100:
            raise ValueError("Допускается до 100 полей в одном бланке")
        book = Workbook()
        book.remove(book.active)
        append_sheet(book, source, name)
        return workbook_bytes(book), keys
    except (BadZipFile, KeyError, IndexError, TypeError, InvalidFileException,
            ElementTree.ParseError, OSError, RuntimeError) as exc:
        raise ValueError("Не удалось прочитать книгу XLSX") from exc


def fill_sheet(sheet, values):
    for cell in sheet._cells.values():
        if cell.data_type == "s" and isinstance(cell.value, str) and "{{" in cell.value:
            cell.value = MARKER.sub(lambda match: str(values.get(match[1], "")), cell.value)
            cell.data_type = "s"


def new_template(name, fields):
    book = Workbook()
    sheet = book.active
    sheet.title = name
    sheet.sheet_view.showGridLines = False
    sheet.sheet_format.defaultRowHeight = 28
    sheet.merge_cells("A1:B1")
    heading = sheet.cell(1, 1, name)
    heading.data_type = "s"
    heading.font = Font(name="Arial", size=16, bold=True)
    heading.alignment = Alignment(horizontal="center", vertical="center")
    sheet.row_dimensions[1].height = 38
    sheet.column_dimensions["A"].width = 32
    sheet.column_dimensions["B"].width = 57
    line = Side(style="thin", color="7F7F7F")
    for row, (key, label) in enumerate(fields.items(), start=3):
        caption = sheet.cell(row, 1, label)
        caption.data_type = "s"
        caption.font = Font(name="Arial", size=11, bold=True)
        entry = sheet.cell(row, 2, "{{" + key + "}}")
        entry.font = Font(name="Arial", size=11)
        for cell in (caption, entry):
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            cell.border = Border(bottom=line)
    last = len(fields) + 5
    sheet.cell(last, 1, "Подпись")
    sheet.cell(last, 2).border = Border(bottom=line)
    sheet.print_area = f"A1:B{last}"
    sheet.page_setup.paperSize = sheet.PAPERSIZE_A4
    sheet.page_setup.orientation = "portrait"
    sheet.sheet_properties.pageSetUpPr.fitToPage = True
    sheet.page_setup.fitToWidth = 1
    sheet.page_setup.fitToHeight = 0
    sheet.print_title_rows = "1:1"
    return workbook_bytes(book)
