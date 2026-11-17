from rag_catalog.core import llm


def test_rag_generation_bounds_context_and_idle_lifetime(monkeypatch):
    calls = []

    def post(url, payload, timeout):
        calls.append(payload)
        return {'response': 'answer'}

    monkeypatch.setattr(llm, '_ollama_post', post)
    assert llm.expand_query('query') == 'answer'
    assert llm.rag_answer('question', [{'filename': 'sample', 'text': 'evidence'}]) == 'answer'
    assert len(calls) == 2
    for payload in calls:
        assert payload['options']['num_ctx'] == 4096
        assert payload['keep_alive'] == '1m'


def test_explicit_generation_policy_is_preserved(monkeypatch):
    calls = []
    monkeypatch.setattr(llm, '_ollama_post', lambda url, payload, timeout: calls.append(payload) or {})
    llm.rag_answer('question', [{'filename': 'sample', 'text': 'evidence'}],
                   context_length=8192, keep_alive='30s')
    assert calls[0]['options']['num_ctx'] == 8192
    assert calls[0]['keep_alive'] == '30s'
