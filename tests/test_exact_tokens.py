from rag_catalog.core.exact_tokens import (
    numeric_exact_tokens,
    numeric_query_has_trusted_context,
    repair_mojibake_text,
)


def test_numeric_exact_tokens_join_adjacent_identifier_groups() -> None:
    tokens = numeric_exact_tokens("СТС 9941 210904")

    assert "9941" in tokens
    assert "210904" in tokens
    assert "9941210904" in tokens


def test_numeric_exact_tokens_tolerate_replacement_separator() -> None:
    tokens = numeric_exact_tokens("9941�210904")

    assert "9941210904" in tokens


def test_numeric_query_context_rejects_unrelated_words() -> None:
    assert numeric_query_has_trusted_context("СТС 9941 210904") is True
    assert numeric_query_has_trusted_context("договор 847291") is True
    assert numeric_query_has_trusted_context("847291") is True
    assert numeric_query_has_trusted_context("qzxv-несуществующий-документ-999999") is False


def test_repair_mojibake_text_recovers_cp866_zip_names() -> None:
    broken = "Счет.pdf".encode("cp866").decode("cp437")

    assert repair_mojibake_text(broken) == "Счет.pdf"
