"""MAX account linking and browser login in the catalog."""

from collections.abc import Awaitable, Callable

from nicegui import run, ui

from rag_catalog.integrations.max_settings import public_bot_link

from .state import PageState, _get_auth_db


def _bot_link(label: str, base: str, purpose: str, token: str) -> None:
    ui.link(label, f"{base}?start={purpose}_{token}", new_tab=True).props("rel='noopener noreferrer'")


async def open_max_account(state: PageState) -> None:
    db = _get_auth_db(state)
    session = state.auth_token
    user = await run.io_bound(db.get_user_by_session, session)
    if not user or user.get("must_change_password") or session != state.auth_token:
        ui.notify("Войдите в кабинет с действующим паролем.", type="warning")
        return
    bot = public_bot_link(state.cfg)
    if not user.get("max_user_id") and not bot:
        ui.notify("Сотрудникам пока недоступно подключение MAX.", type="warning")
        return

    async def unlink() -> None:
        removed = await run.io_bound(db.unlink_max_for_session, session_token=session)
        ui.notify("MAX отвязан." if removed else "Сессия истекла.")
        dialog.close()

    async def check_link() -> None:
        current = await run.io_bound(db.get_user_by_session, session)
        if current and current.get("max_user_id"):
            ui.notify("MAX привязан.", type="positive")
            dialog.close()
        else:
            ui.notify("Подтвердите привязку в MAX.", type="info")

    if not user.get("max_user_id"):
        result = await run.io_bound(db.create_max_link_token, session_token=session)
        if not result["ok"]:
            ui.notify("Откройте подключение MAX ещё раз.", type="warning")
            return
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-sm"):
        ui.label("Аккаунт MAX").classes("text-lg font-semibold")
        if user.get("max_user_id"):
            ui.label(f"MAX привязан к сотруднику {user['username']}.")
            ui.button("Отвязать MAX", icon="link_off", on_click=unlink).props("outline")
        else:
            ui.label(f"Откройте бота со своего аккаунта MAX, чтобы привязать его к {user['username']}. Ссылка действует 10 минут.")
            _bot_link("Открыть бота MAX", bot, "link", result["token"])
            ui.button("Проверить привязку", icon="refresh", on_click=check_link).props("outline")
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


def render_max_login_button(state: PageState, complete_login: Callable[..., Awaitable[None]]) -> None:
    bot = public_bot_link(state.cfg)
    if not bot:
        return
    db = _get_auth_db(state)

    async def request_login() -> None:
        if state.current_user:
            return
        button.disable()
        try:
            challenge = await run.io_bound(db.create_max_login_challenge)
        finally:
            if not button.is_deleted:
                button.enable()
        if button.is_deleted or state.current_user:
            return

        async def poll() -> None:
            if state.current_user or not dialog.value or button.is_deleted:
                timer.deactivate()
                return
            out = await run.io_bound(db.consume_confirmed_max_login, verifier=challenge["verifier"])
            if dialog.is_deleted or not dialog.value or state.current_user or button.is_deleted:
                return
            if out.get("reason") == "pending":
                return
            timer.deactivate()
            if out["ok"]:
                await complete_login(out["user"], event_type="max_web_login")
            else:
                ui.notify("Ссылка входа истекла. Запросите новую.", type="warning")
            if not dialog.is_deleted:
                dialog.close()

        with ui.dialog() as dialog, ui.card().classes("w-full max-w-sm"):
            ui.label("Вход через MAX").classes("text-lg font-semibold")
            ui.label("Откройте бота и подтвердите вход в этот браузер. Ссылка действует 5 минут.")
            _bot_link("Подтвердить в MAX", bot, "login", challenge["token"])
            ui.button("Отмена", on_click=dialog.close).props("flat")
            timer = ui.timer(2, poll)
        dialog.on("hide", dialog.delete)
        dialog.open()

    button = ui.button("Войти через MAX", icon="chat", on_click=request_login).props("outline").classes("w-full")
