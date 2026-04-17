from telegram_bot import _clean_tg_text, _file_uri, format_fact_answer, process_message, process_query


class _FakeSearcher:
    def __init__(self, fact_result=None, fact_exc=None, search_result=None, search_exc=None):
        self._fact_result = fact_result
        self._fact_exc = fact_exc
        self._search_result = search_result if search_result is not None else []
        self._search_exc = search_exc

    def answer_fact_question(self, _q, limit=30):
        if self._fact_exc:
            raise self._fact_exc
        return self._fact_result if self._fact_result is not None else {"ok": False}

    def search(self, *_args, **_kwargs):
        if self._search_exc:
            raise self._search_exc
        return self._search_result


class _FakeAuthDB:
    def __init__(self, out):
        self.out = out

    def confirm_verification(self, *, telegram_chat_id: str, code: str):
        return self.out(telegram_chat_id, code) if callable(self.out) else self.out

    def confirm_password_reset(self, *, telegram_chat_id: str, code: str):
        return self.out(telegram_chat_id, code) if callable(self.out) else self.out


def test_process_query_reports_infra_error_from_fact() -> None:
    s = _FakeSearcher(fact_exc=ConnectionError("qdrant down"))
    out = process_query(s, "сколько весит pc300")
    assert "Ошибка инфраструктуры поиска" in out


def test_process_query_returns_fact_answer() -> None:
    s = _FakeSearcher(
        fact_result={
            "ok": True,
            "answer": "3400 кг согласно ПСМ",
            "source": {
                "filename": "ПСМ PC300.pdf",
                "full_path": r"O:\Обмен\ПСМ\PC300.pdf",
                "text_excerpt": "Масса: 3400 кг",
            },
        }
    )
    out = process_query(s, "сколько весит pc300")
    assert "3400 кг согласно ПСМ" in out
    assert "ПСМ PC300.pdf" in out


def test_process_query_fallback_results() -> None:
    s = _FakeSearcher(
        fact_result={"ok": False},
        search_result=[{"filename": "паспорт.docx", "score": 0.8, "full_path": r"O:\Обмен\паспорт.docx"}],
    )
    out = process_query(s, "паспорт")
    assert "Точный факт не извлечён" in out
    assert "паспорт.docx" in out


def test_process_query_no_results() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    out = process_query(s, "нечто редкое")
    assert out == "Ничего не найдено."


def test_process_message_verify_success() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": True, "username": "ivan"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/verify 123456",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "подтверждён" in out
    assert "ivan" in out


def test_process_message_verify_not_found() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": False, "reason": "not_found"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/verify 000000",
        chat_id="777",
        allowed_chat_id="123",
    )
    assert "Код не найден" in out


def test_process_message_recover_success() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": True, "username": "ivan"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/recover 123456",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Код восстановления подтверждён" in out


# ── sanitization helpers ──────────────────────────────────────────────────

def test_clean_tg_text_strips_control_chars() -> None:
    assert _clean_tg_text("hello\x00world\x1f!") == "helloworld!"


def test_clean_tg_text_normalizes_crlf() -> None:
    assert "\r" not in _clean_tg_text("line1\r\nline2\rline3")


def test_clean_tg_text_truncates() -> None:
    long = "а" * 2000
    result = _clean_tg_text(long, max_len=100)
    assert len(result) <= 101  # 100 chars + "…"
    assert result.endswith("…")


def test_file_uri_windows_cyrillic() -> None:
    uri = _file_uri(r"O:\Обмен\Договоры\файл.pdf")
    assert uri.startswith("file:///O:/")
    assert "%D0" in uri  # кириллица закодирована


def test_file_uri_windows_spaces() -> None:
    uri = _file_uri(r"C:\My Documents\file name.pdf")
    assert "%20" in uri or "My%20Documents" in uri


def test_file_uri_empty_returns_empty() -> None:
    assert _file_uri("") == ""
    assert _file_uri(None) == ""  # type: ignore[arg-type]


def test_format_fact_answer_no_newlines_in_path() -> None:
    result = format_fact_answer({
        "answer": "3400 кг",
        "source": {
            "filename": "file\nwith\nnewlines.pdf",
            "full_path": r"O:\Обмен\file.pdf",
            "text_excerpt": "Масса:\n3400 кг",
        },
    })
    # Перевод строки допустим между полями, но не внутри имени файла
    lines = result.split("\n")
    file_line = next(l for l in lines if l.startswith("Файл:"))
    assert "\n" not in file_line
