"""Source-bound document mentions, not a master register of people or ownership.

Offsets refer to the original chunk. Unlabelled dates remain searchable but
are never silently promoted to a document's issue date. No external model,
OCR, filesystem timestamps or cross-document identity inference is used.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date

from .retrieval.terms import term_matches, tokenize

SCHEMA_VERSION = 1
MAX_TEXT = 64_000
MAX_FACTS = 128
_MONTHS = {name: i for i, name in enumerate(
    'января февраля марта апреля мая июня июля августа сентября октября ноября декабря'.split(), 1)}
_DATE = re.compile(
    r'(?<!\d)(?:(?P<y1>20\d{2}|19\d{2})-(?P<m1>\d{2})-(?P<d1>\d{2})'
    r'|(?P<d2>\d{1,2})[./-](?P<m2>\d{1,2})[./-](?P<y2>20\d{2}|19\d{2})'
    r'|[«"“]?(?P<d3>\d{1,2})[»"”]?\s+(?P<m3>' + '|'.join(_MONTHS) +
    r')\s+(?P<y3>20\d{2}|19\d{2}))(?!\d)', re.I)
_ORG = re.compile(r'\b(?:ООО|АО|ПАО|ОАО|ЗАО|общество\s+с\s+ограниченной\s+ответственностью|'
                  r'общества\s+с\s+ограниченной\s+ответственностью)\s*[«"]([^»"\r\n]{2,100})[»"]', re.I)
_FIO = r'[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+'
_PERSON = re.compile(r'\b(?:' + _FIO + r'\b|[А-ЯЁ][а-яё-]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.)')
_KINDS = (
    ('appointment_decision', r'решени\w*[\s\S]{0,140}(?:назначени\w*|полномочи\w*|смен\w*)[\s\S]{0,60}директор'),
    ('invoice', r'(?:сч[её]т[ -]?)?фактур'),
    ('sale_contract', r'договор[\s\S]{0,45}(?:купли[ -]продажи|покупк|продаж)'),
    ('service_contract', r'договор[\s\S]{0,65}(?:оказани\w*\s+услуг|услуг|подряд)'),
    ('lease_contract', r'договор[\s\S]{0,45}аренд'),
    ('vehicle_registration', r'\bстс\b|свидетельство\s+о\s+регистрации\s+(?:транспортного|тс)'),
    ('vehicle_passport', r'\bптс\b|паспорт\s+транспортного\s+средства'),
    ('personal_passport', r'паспорт\s+(?:гражданина|рф)|дата\s+рождения'),
    ('contract', r'\bдоговор'),
)
_ROLES = (
    ('birth_date', r'(?:дат\w*\s+)?рождени\w*'),
    ('issue_date', r'(?:дат\w*\s+выдачи|выдан\w*)'),
    ('appointment_date', r'(?:назначить|назначен\w*|приступить)[^\n]{0,70}\sс'),
    ('expiry_date', r'(?:действителен\s+до|срок[^\n]{0,25}до)'),
    ('document_date', r'(?:дат\w*\s+(?:документ\w*|решени\w*|договор\w*|сч[её]т\S*|составления)|(?:решение|договор|сч[её]т[ -]фактура)[^\n]{0,60}\sот)'),
)
_PARTY_ROLES = (
    ('owner', r'собственник|владелец'), ('buyer', r'покупатель'),
    ('seller', r'продавец'), ('tenant', r'арендатор'), ('landlord', r'арендодатель'),
    ('customer', r'заказчик'), ('contractor', r'исполнитель|подрядчик'),
    ('director', r'директор'), ('recipient', r'получатель'), ('sender', r'отправитель'),
)
_LABELS = {
    'mention': 'Упоминание', 'document_date': 'Дата документа', 'issue_date': 'Дата выдачи',
    'birth_date': 'Дата рождения', 'appointment_date': 'Начало полномочий', 'expiry_date': 'Срок действия',
    'owner': 'Собственник по документу', 'buyer': 'Покупатель', 'seller': 'Продавец',
    'tenant': 'Арендатор', 'landlord': 'Арендодатель', 'director': 'Директор',
    'customer': 'Заказчик', 'contractor': 'Исполнитель', 'document_number': 'Номер документа',
    'passport_number': 'Серия и номер паспорта', 'registration_number': 'Номер СТС',
    'vin': 'VIN', 'plate': 'Госномер', 'cadastre': 'Кадастровый номер', 'address': 'Адрес',
}


def normalize(value: str) -> str:
    return ' '.join(str(value or '').casefold().replace('ё', 'е').split())


def date_mentions(text: str):
    for match in _DATE.finditer(text):
        for suffix in ('1', '2', '3'):
            if match[f'y{suffix}']:
                month = _MONTHS.get(match[f'm{suffix}'].lower()) if suffix == '3' else int(match[f'm{suffix}'])
                try:
                    value = date(int(match[f'y{suffix}']), month, int(match[f'd{suffix}'])).isoformat()
                except ValueError:
                    break
                yield match, value
                break


def document_kind(text: str, filename: str = '') -> str:
    bag = filename + '\n' + text[:2000]
    return next((kind for kind, pattern in _KINDS if re.search(pattern, bag, re.I)), '')


def extract_facts(text: str, *, filename: str = '') -> dict:
    original = str(text or '')
    text = original[:MAX_TEXT]
    kind = document_kind(text, filename)
    facts = []
    seen = set()

    def add(category, role, value, start, end, canonical=None):
        key = (category, role, canonical or normalize(value), start)
        if key in seen or len(facts) >= MAX_FACTS:
            return
        seen.add(key)
        facts.append(dict(kind=category, role=role, value=value, normalized=canonical or normalize(value),
                          start=start, end=end, evidence=text[max(0, start - 100):min(len(text), end + 140)],
                          method='explicit_text', certainty='mention' if role == 'mention' else 'labelled'))
        return facts[-1]

    for match, value in date_mentions(text):
        before = text[max(text.rfind('\n', 0, match.start()) + 1, match.start() - 110):match.start()]
        if not before.strip():
            before = text[:match.start()].rstrip().rsplit('\n', 1)[-1][-110:]
        role = next((role for role, pattern in _ROLES if re.search(pattern + r'\s*[:—\-]?\s*$', before, re.I)), 'mention')
        if (role == 'mention' and match.start() < 700
                and re.match(r'\s*(?:решение|договор|сч[её]т[ -]?фактура)\b', text, re.I)
                and re.fullmatch(r'\s*(?:г\.|город)\s+[А-ЯЁа-яё -]+\s*', before)):
            role = 'document_date'
        add('date', role, match.group(), match.start(), match.end(), value)

    for pattern, category in ((_ORG, 'organization'), (_PERSON, 'person')):
        for match in pattern.finditer(text):
            before = text[max(text.rfind('\n', 0, match.start()) + 1, match.start() - 60):match.start()]
            role = next((role for role, label in _PARTY_ROLES if re.search(r'(?:' + label + r')\s*[:—\-]?\s*(?:ИП\s*)?$', before, re.I)), 'mention')
            add(category, role, match.group(), match.start(), match.end())

    fields = (
        ('identifier', 'vin', r'\bVIN\s*[:№\-]?\s*([A-HJ-NPR-Z0-9]{17})\b'),
        ('identifier', 'cadastre', r'\b(\d{2}:\d{2}:\d{6,7}:\d{1,9})\b'),
        ('identifier', 'inn', r'\bИНН\s*[:№]?\s*(\d{10}|\d{12})\b'),
        ('identifier', 'document_number', r'(?:решение|договор|сч[её]т[ -]фактура)\s*№\s*([\w/.-]{1,40})'),
        ('identifier', 'registration_number', r'(?:\bСТС|свидетельство\s+о\s+регистрации(?:\s+ТС)?)\s*(?:№|номер)?\s*[:\-]?\s*(\d{2}\s?\d{2}\s?\d{6})\b'),
        ('identifier', 'passport_number', r'паспорт\s*(?:серия|серии|№)?\s*[:\-]?\s*(\d{2}\s?\d{2}\s?\d{6})\b'),
        ('identifier', 'plate', r'(?:гос\S*\s*номер|регистрационный\s+знак)\s*[:№]?\s*([АВЕКМНОРСТУХABEKMHOPCTYX]\s?\d{3}\s?[АВЕКМНОРСТУХABEKMHOPCTYX]{2}\s?\d{2,3})\b'),
        ('address', 'address', r'(?:адрес(?:\s+(?:объекта|регистрации|организации|местонахождения))?)\s*:\s*([^\r\n;]{5,200})'),
        ('location', 'city', r'(?:\bг\.|\bгород)[ \t]*([А-ЯЁ][а-яё-]+(?:[ -]на-[А-ЯЁ][а-яё-]+)?)'),
        ('location', 'street', r'(?:\bул\.|\bулица|\bпроспект|\bпр-т)\s*([А-ЯЁа-яё0-9][^,;\r\n]{1,70})'),
    )
    for category, role, pattern in fields:
        for match in re.finditer(pattern, text, re.I):
            if role == 'city' and re.search(r'\d{4}\s*$', text[max(0, match.start() - 8):match.start()]):
                continue
            value = match[1].strip()
            canonical = re.sub(r'\s+', '', normalize(value)) if category == 'identifier' else normalize(value)
            fact = add(category, role, value, match.start(1), match.end(1), canonical)
            if role == 'address' and fact is not None:
                label = normalize(match.group().split(':', 1)[0])
                fact['context_role'] = ('object' if 'объект' in label else
                                        'organization' if 'организац' in label else
                                        'registration' if 'регистрац' in label else 'unspecified')
    return dict(schema_version=SCHEMA_VERSION, document_kind=kind, mentions=facts,
                text_sha256=hashlib.sha256(original.encode('utf-8')).hexdigest(),
                truncated=len(original) > MAX_TEXT or len(facts) >= MAX_FACTS)


def compact_facts(text: str, *, filename: str = '') -> dict:
    facts = extract_facts(text, filename=filename)
    facts['mentions'] = [{key: value for key, value in fact.items() if key != 'evidence'} for fact in facts['mentions']]
    return facts


@dataclass(frozen=True)
class FactQuery:
    kind: str = ''
    dates: tuple[str, ...] = ()
    date_range: bool = False
    role: str = ''
    qualifiers: tuple[str, ...] = ()
    lookup: str = ''
    parties: tuple[tuple[str, str], ...] = ()

    @property
    def active(self):
        return bool(self.kind or self.dates or self.role)


def parse_fact_query(query: str) -> FactQuery:
    q = normalize(query)
    dates = tuple(value for _, value in date_mentions(q))
    kind = document_kind(q)
    role = ''
    if re.search(r'\b(?:дата|дату|когда|какого числа)\b', q):
        if 'рождени' in q:
            role = 'birth_date'
        elif 'выдач' in q or 'выдан' in q:
            role = 'issue_date'
        elif kind:
            role = 'document_date'
        else:
            role = 'date'
    if re.search(r'кому принадлежит|чей\b|собственник|владелец', q):
        role = 'owner'
    if re.search(r'\b(?:адрес|местоположение)\b', q):
        role = 'address'
    if re.search(r'\b(?:номер|номера|данные)\s+паспорта', q):
        role = 'passport_number'
    clean = _DATE.sub(' ', q)
    clean = re.sub(r'\b(?:дата|дату|даты|когда|какого|числа|период|кому|принадлежит|чей|собственник|владелец|'
                   r'найти|найди|покажи|документы?|документа|документе|такое|это|этот|какой|какая|открыть|'
                   r'выдачи|выдан|рождения|номер|номера|данные|паспорт[а-я]*|машин[а-я]*|автомобил[а-я]*|адрес|местоположение)\b', ' ', clean)
    lookup_words = {'appointment_decision': 'решение директор', 'invoice': 'счет фактура',
                    'sale_contract': 'договор купли продажи', 'service_contract': 'договор услуги',
                    'lease_contract': 'договор аренды', 'contract': 'договор',
                    'vehicle_registration': 'стс', 'vehicle_passport': 'птс', 'personal_passport': 'паспорт'}
    qualifiers = tuple(t for t in tokenize(clean) if t not in {'за', 'до', 'со', 'такого', 'такой', 'года', 'число'})
    if kind:
        clean = re.sub(r'решени\w*|назначени\w*|полномочи\w*|директор\w*|сч[её]т[ -]?фактур\w*|'
                       r'договор\w*|купли|продажи|покупк\w*|оказани\w*|услуг\w*|аренд\w*', ' ', clean)
        qualifiers = tuple(t for t in tokenize(clean) if t not in {'за', 'до', 'со', 'такого', 'такой', 'года', 'число'})
    lookup = ' '.join([lookup_words.get(kind, ''), *qualifiers]).strip()
    parties = []
    for org in _ORG.finditer(q):
        before = q[max(0, org.start() - 30):org.start()]
        party_role = next((r for r, label in _PARTY_ROLES if re.search(r'(?:' + label + r')\s*:?\s*$', before)), '')
        if not party_role and kind == 'invoice':
            if re.search(r'\bот\s*$', before):
                party_role = 'seller'
            elif re.search(r'\b(?:к|для|в)\s*$', before):
                party_role = 'buyer'
        if party_role:
            parties.append((party_role, _party_key(org.group())))
    return FactQuery(kind, dates, len(dates) == 2 and bool(re.search(r'\b(?:с|от)\b.+\b(?:до|по)\b', q)), role, qualifiers, lookup, tuple(parties))


def _party_key(value: str) -> str:
    return normalize(re.sub(r'[«»"“”]', '', value))


def facts_for_result(item: dict) -> dict:
    stored = item.get('document_facts')
    text = str(item.get('text') or '')
    if (isinstance(stored, dict) and stored.get('schema_version') == SCHEMA_VERSION
            and stored.get('text_sha256') == hashlib.sha256(text.encode('utf-8')).hexdigest()):
        return {**stored, 'mentions': [{**fact, 'evidence': text[max(0, fact['start'] - 100):fact['end'] + 140]}
                                      for fact in stored['mentions']]}
    if str(item.get('type') or '').endswith('metadata'):
        return extract_facts('', filename=str(item.get('filename') or ''))
    return extract_facts(str(item.get('text') or ''), filename=str(item.get('filename') or ''))


def matches_query(plan: FactQuery, item: dict, facts: dict) -> bool:
    kind = facts.get('document_kind', '')
    if plan.kind and not (kind == plan.kind or (plan.kind == 'contract' and kind.endswith('_contract'))):
        return False
    bag = normalize(' '.join(str(item.get(key) or '') for key in ('filename', 'path', 'text')))
    if any(not term_matches(bag, term) for term in plan.qualifiers):
        return False
    if any(not any(f['role'] == role and _party_key(f['value']) == name for f in facts['mentions'])
           for role, name in plan.parties):
        return False
    if plan.dates:
        dates = [fact['normalized'] for fact in facts['mentions'] if fact['kind'] == 'date'
                 and (not plan.role or plan.role == 'date' or fact['role'] == plan.role)]
        if plan.date_range:
            if not any(plan.dates[0] <= value <= plan.dates[1] for value in dates):
                return False
        elif not all(value in dates for value in plan.dates):
            return False
    return True


def answer_from_facts(question: str, results: list[dict]) -> dict:
    plan = parse_fact_query(question)
    if not plan.role:
        return {}
    sources, lines, seen = [], [], set()
    for item in results:
        facts = facts_for_result(item)
        if not matches_query(plan, item, facts):
            continue
        for fact in facts['mentions']:
            if not (fact['role'] == plan.role or plan.role == 'date' and fact['kind'] == 'date'):
                continue
            key = (str(item.get('full_path') or item.get('path')), fact['role'], fact['normalized'])
            if key in seen:
                continue
            seen.add(key)
            source_id = f'S{len(sources) + 1}'
            sources.append({**item, 'source_id': source_id, 'excerpt': fact['evidence'],
                            'citation': f'[{source_id}] {item.get("filename") or item.get("path")}'})
            lines.append(f'{_LABELS.get(fact["role"], fact["role"])}: {fact["value"]} [{source_id}]')
            if len(sources) >= 5:
                break
        if len(sources) >= 5:
            break
    if not sources:
        return {'ok': False, 'answer': 'В найденных фрагментах нет явно указанного факта. Это не означает, что документа нет.',
                'sources': [], 'error': 'no_labelled_evidence'}
    prefix = 'В найденных документах указано (это не проверка актуальности сведений):\n'
    return {'ok': True, 'answer': prefix + '\n'.join(lines), 'sources': sources,
            'results': results, 'method': 'explicit_document_facts', 'error': ''}
