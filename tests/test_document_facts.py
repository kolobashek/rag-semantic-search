import pytest

from rag_catalog.core.document_facts import (
    answer_from_facts,
    compact_facts,
    extract_facts,
    facts_for_result,
    matches_query,
    parse_fact_query,
)
from rag_catalog.core.rag_core import RAGSearcher


def result(text, filename='Решение о назначении директора.pdf', **kwargs):
    return dict(text=text, filename=filename, path=filename, full_path='catalog/' + filename,
                type='pdf_content', chunk_index=0, **kwargs)


def test_all_dates_are_preserved_with_distinct_roles_and_offsets():
    text = 'Решение № 5 от 15.05.2024\nДата рождения: 10.04.1980\nДата выдачи: 01.06.2020'
    facts = extract_facts(text)['mentions']
    dates = [f for f in facts if f['kind'] == 'date']
    assert [(f['role'], f['normalized']) for f in dates] == [
        ('document_date', '2024-05-15'), ('birth_date', '1980-04-10'), ('issue_date', '2020-06-01')]
    for fact in dates:
        assert text[fact['start']:fact['end']] == fact['value']


@pytest.mark.parametrize('spelling', ['15.05.2024', '15/05/2024', '2024-05-15', '«15» мая 2024', '15 МАЯ 2024'])
def test_dates_normalize_without_losing_quote(spelling):
    fact = extract_facts(spelling)['mentions'][0]
    assert fact['normalized'] == '2024-05-15'
    assert fact['value'] == spelling


def test_invalid_dates_do_not_become_facts():
    assert extract_facts('31.02.2024; 29.02.2023; 99.20.2024')['mentions'] == []


def test_general_date_query_matches_birth_date_but_specific_issue_query_does_not():
    item = result('Дата рождения: 15.05.1980\nДата выдачи: 01.06.2020', filename='Паспорт Иванов.pdf')
    facts = facts_for_result(item)
    assert matches_query(parse_fact_query('документы за 15.05.1980'), item, facts)
    assert not matches_query(parse_fact_query('дата выдачи паспорта 15.05.1980'), item, facts)


def test_file_modified_date_is_not_document_evidence():
    item = result('Нет даты в тексте', modified='2024-05-15')
    assert not matches_query(parse_fact_query('документ за 15.05.2024'), item, facts_for_result(item))
    metadata = result('Файл: 15.05.2024.txt')
    metadata['type'] = 'file_metadata'
    assert facts_for_result(metadata)['mentions'] == []


def test_date_range_matches_any_mention_including_middle_and_edges():
    plan = parse_fact_query('документы с 01.01.2024 по 31.12.2024')
    assert plan.date_range
    for date in ['01.01.2024', '15.05.2024', '31.12.2024']:
        item = result('Ссылка на документ от ' + date)
        assert matches_query(plan, item, facts_for_result(item))
    item = result('Дата выдачи: 01.01.2025')
    assert not matches_query(plan, item, facts_for_result(item))


def test_roles_are_not_interchangeable_and_owner_answer_is_source_bound():
    text = ('СТС 99 80 123456\nVIN: XTA210990Y1234567\n'
            'Собственник: ООО «Альфа»\nАрендатор: ООО «Бета»\nВодитель: Иванов Иван Иванович')
    item = result(text, filename='СТС 9980123456.pdf')
    facts = extract_facts(text)
    assert ('owner', 'ООО «Альфа»') in [(f['role'], f['value']) for f in facts['mentions']]
    assert ('tenant', 'ООО «Бета»') in [(f['role'], f['value']) for f in facts['mentions']]
    answer = answer_from_facts('чей номер СТС 99 80 123456', [item])
    assert answer['ok']
    assert 'Альфа' in answer['answer'] and 'Бета' not in answer['answer']
    assert 'не проверка актуальности' in answer['answer']
    assert answer['sources'][0]['full_path'] == item['full_path']


def test_lease_is_not_purchase_or_ownership():
    item = result('Договор аренды автомобиля\nАрендатор: ООО «Тест»', filename='Аренда.pdf')
    assert not matches_query(parse_fact_query('договор покупки автомобиля'), item, facts_for_result(item))
    assert not answer_from_facts('кому принадлежит автомобиль', [item])['ok']


def test_real_estate_and_address_share_the_same_model():
    text = 'Адрес объекта: г. Новокузнецк, ул. Ленина, д. 15\nКадастровый номер: 42:30:0101001:5'
    facts = extract_facts(text)['mentions']
    assert {'address', 'city', 'street', 'cadastre'} <= {f['role'] for f in facts}
    assert next(f['normalized'] for f in facts if f['role'] == 'cadastre') == '42:30:0101001:5'
    assert next(f['context_role'] for f in facts if f['role'] == 'address') == 'object'


@pytest.mark.parametrize('city', ['Киселевск', 'Новосибирск', 'Новокузнецк'])
def test_location_mentions_keep_city_street_and_address_role(city):
    text = (f'Адрес регистрации: г. {city}, ул. Ленина, д. 15\n'
            f'Адрес организации: город {city}, проспект Мира, д. 20')
    facts = extract_facts(text)['mentions']
    assert [f['normalized'] for f in facts if f['role'] == 'city'] == [city.lower(), city.lower()]
    assert [f['normalized'] for f in facts if f['role'] == 'street'] == ['ленина', 'мира']
    assert [f['context_role'] for f in facts if f['role'] == 'address'] == ['registration', 'organization']
    for fact in facts:
        assert text[fact['start']:fact['end']] == fact['value']


def test_fact_limit_does_not_attach_address_role_to_unrelated_mention():
    text = '01.01.2020\n' * 128 + 'Адрес объекта: г. Киселевск, ул. Ленина'
    facts = extract_facts(text)
    assert facts['truncated']
    assert len(facts['mentions']) == 128
    assert all('context_role' not in f for f in facts['mentions'])


def test_invoice_direction_is_not_just_two_organization_mentions():
    query = parse_fact_query('счет-фактуры от ООО «Альфа» к ООО «Бета»')
    valid = result('Счет-фактура № 7 от 01.01.2024\nПродавец: ООО «Альфа»\nПокупатель: ООО «Бета»', filename='invoice.pdf')
    reverse = result('Счет-фактура № 8 от 01.01.2024\nПродавец: ООО «Бета»\nПокупатель: ООО «Альфа»', filename='reverse.pdf')
    mention = result('Счет-фактура\nООО «Бета»\nООО «Альфа»', filename='unknown.pdf')
    assert matches_query(query, valid, facts_for_result(valid))
    assert not matches_query(query, reverse, facts_for_result(reverse))
    assert not matches_query(query, mention, facts_for_result(mention))


def test_date_on_next_line_retains_explicit_label():
    facts = extract_facts('Дата выдачи:\n15.05.2024')['mentions']
    assert facts[0]['role'] == 'issue_date'


def test_compact_payload_rehydrates_evidence_and_rejects_stale_text():
    item = result('Дата выдачи: 15.05.2024')
    item['document_facts'] = compact_facts(item['text'], filename=item['filename'])
    assert 'evidence' not in item['document_facts']['mentions'][0]
    assert '15.05.2024' in facts_for_result(item)['mentions'][0]['evidence']
    item['text'] = 'Дата выдачи: 01.01.2025'
    assert facts_for_result(item)['mentions'][0]['normalized'] == '2025-01-01'


def test_multiple_sources_are_not_silently_reduced_to_one_current_fact():
    rows = [result('Решение № 1 от 01.01.2020'), result('Решение № 2 от 02.02.2025')]
    answer = answer_from_facts('дата решения о назначении директора', rows)
    assert len(answer['sources']) == 2
    assert '[S1]' in answer['answer'] and '[S2]' in answer['answer']


def test_unlabelled_date_is_searchable_but_not_a_confirmed_issue_date():
    item = result('г. Новосибирск 01.01.2025\nНазначить директора.')
    assert matches_query(parse_fact_query('документ за 01.01.2025'), item, facts_for_result(item))
    assert not answer_from_facts('дата выдачи', [item])['ok']


def test_decision_city_dateline_does_not_turn_year_suffix_into_a_city():
    text = 'РЕШЕНИЕ\nединственного участника\nг. Новосибирск "07" февраля 2019 г.\nВремя принятия 10 ч.'
    facts = extract_facts(text, filename='Решение о назначении директора.pdf')['mentions']
    assert [(f['role'], f['normalized']) for f in facts if f['kind'] == 'date'] == [('document_date', '2019-02-07')]
    assert [f['value'] for f in facts if f['role'] == 'city'] == ['Новосибирск']


def test_bounded_fact_lookup_filters_acl_before_chunk_expansion_and_answer():
    searcher = object.__new__(RAGSearcher)
    searcher.connected = True
    searcher.config = {}
    searcher._lexical_catalog_search = lambda **kw: [
        {'full_path': 'allowed', 'type': 'file_metadata'}, {'full_path': 'denied', 'type': 'file_metadata'}]
    calls = []
    def chunks(paths, max_chunks, **kwargs):
        assert kwargs['header_first'] is True
        calls.append(paths)
        return [result('Решение № 1 от 01.01.2025'), result('Решение № 2 от 02.02.2025', filename='private.pdf')]
    searcher._content_chunks_for_paths = chunks
    searcher._fulltext_content_search = lambda **kw: []
    def acl(rows):
        return [r for r in rows if r.get('full_path') != 'denied' and r.get('filename') != 'private.pdf']
    out = searcher.search_document_facts('дата решения о назначении директора', result_filter=acl)
    assert calls == [['allowed']]
    assert len(out) == 1
    assert '02.02.2025' not in answer_from_facts('дата решения о назначении директора', out)['answer']


def test_fact_lookup_does_not_expand_files_after_acl_failure():
    s = object.__new__(RAGSearcher)
    s.connected = True
    s.config = {}
    s._lexical_catalog_search = lambda **kw: [{'full_path': 'denied', 'type': 'file_metadata'}]
    s._content_chunks_for_paths = lambda paths, **kw: [] if not paths else pytest.fail('ACL leak')
    s._fulltext_content_search = lambda **kw: []
    def fail(rows):
        raise RuntimeError('ACL unavailable')
    assert s.search_document_facts('дата решения о назначении директора', result_filter=fail) == []
