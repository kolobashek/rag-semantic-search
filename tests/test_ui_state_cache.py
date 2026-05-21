from rag_catalog.ui.state import PageState, capture_screen_state, restore_screen_state


def test_search_screen_state_cache_restores_query_filters_and_results() -> None:
    state = PageState(cfg={})
    state.screen = "search"
    state.query = "паспорт"
    state.file_type = ".pdf"
    state.limit = 25
    state.content_only = True
    state.results = [{"path": "O:/a.pdf", "score": 0.9}]
    state.searched_query = "паспорт"
    state.active_type_filter = "PDF"
    capture_screen_state(state)

    state.query = ""
    state.file_type = None
    state.limit = 50
    state.content_only = False
    state.results = []
    state.searched_query = ""
    state.active_type_filter = None

    assert restore_screen_state(state, "search") is True
    assert state.query == "паспорт"
    assert state.file_type == ".pdf"
    assert state.limit == 25
    assert state.content_only is True
    assert state.results == [{"path": "O:/a.pdf", "score": 0.9}]
    assert state.searched_query == "паспорт"
    assert state.active_type_filter == "PDF"


def test_explorer_and_cloud_screen_state_cache_restore_independently() -> None:
    state = PageState(cfg={})
    state.screen = "explorer"
    state.explorer_path = "O:/Обмен"
    state.explorer_filter = "акт"
    state.explorer_view = "Мелкие значки"
    state.explorer_page = 3
    capture_screen_state(state)

    state.screen = "cloud"
    state.cloud_tab = "sync"
    capture_screen_state(state)

    state.explorer_path = None
    state.explorer_filter = ""
    state.explorer_view = "Таблица"
    state.explorer_page = 0
    state.cloud_tab = "files"

    assert restore_screen_state(state, "explorer") is True
    assert state.explorer_path == "O:/Обмен"
    assert state.explorer_filter == "акт"
    assert state.explorer_view == "Мелкие значки"
    assert state.explorer_page == 3

    assert restore_screen_state(state, "cloud") is True
    assert state.cloud_tab == "sync"
