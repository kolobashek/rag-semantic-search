"""Account linking and the organization administrator's employee desk."""

import re
from inspect import isawaitable
from io import BytesIO

from nicegui import run, ui

from rag_catalog.core.roles import DEFAULT_EMPLOYEE_ROLE, ROLE_LABELS, manages_employees
from rag_catalog.integrations.employee_portal import dispatch_notice, employee_office_url
from rag_catalog.integrations.max_settings import public_bot_link

from .helpers import _telegram_deeplink
from .state import _get_auth_db


def employee_roles_select():
    return ui.select(ROLE_LABELS, value=[DEFAULT_EMPLOYEE_ROLE], label="Роли сотрудника",
                     multiple=True).props("outlined dense use-chips").classes("w-full")


async def confirm_role_grant(roles, submit):
    if set(roles or []) - {DEFAULT_EMPLOYEE_ROLE}:
        with ui.dialog() as dialog, ui.card():
            ui.label("Назначить дополнительные права?").classes("text-lg font-semibold")
            ui.label(", ".join(ROLE_LABELS[role] for role in roles))
            with ui.row():
                ui.button("Вернуться", on_click=lambda: dialog.submit(False)).props("flat")
                ui.button("Назначить выбранные права", on_click=lambda: dialog.submit(True))
        confirmed = await dialog
        dialog.delete()
        if not confirmed:
            return
    result = submit()
    if isawaitable(result):
        await result


async def open_telegram_account(state):
    db = _get_auth_db(state)
    session = state.auth_token
    employee = await run.io_bound(db.get_user_by_session, session)
    if not employee or employee.get("must_change_password"):
        ui.notify("Войдите в рабочий кабинет с действующим паролем.", type="warning")
        return
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-sm"):
        ui.label("Аккаунт Telegram").classes("text-lg font-semibold")
        if employee.get("telegram_chat_id"):
            ui.label(f"Telegram привязан к сотруднику {employee['username']}.")
            def unlink():
                if db.unlink_telegram_for_session(session_token=session):
                    ui.notify("Telegram отвязан.")
                dialog.close()
            ui.button("Отвязать Telegram", on_click=unlink).props("outline")
        else:
            result = db.create_telegram_link_token(session_token=session)
            base = state.cfg.get("telegram_bot_link") or ""
            if result.get("ok") and base:
                ui.label(f"Откройте бота из того аккаунта Telegram, который хотите привязать к {employee['username']}.")
                ui.link("Открыть бота Telegram", _telegram_deeplink(base, "link", result["token"]),
                        new_tab=True).props("rel='noopener noreferrer'")
            else:
                ui.label("Привязка сейчас недоступна.")
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


def _personal_link(url):
    with ui.context.client.content, ui.dialog() as dialog, ui.card().classes("w-full max-w-md"):
        ui.label("Передайте ссылку сотруднику лично").classes("text-lg font-semibold")
        field = ui.input("Одноразовая ссылка", value=url).props("outlined readonly").classes("w-full")
        ui.button("Выделить ссылку", on_click=lambda: field.run_method("select")).props("outline")
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


def open_employee_desk(state):
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl"):
        ui.label("Сотрудники").classes("text-xl font-semibold")
        render_employee_desk(state)
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


def render_employee_desk(state):
    db = _get_auth_db(state)

    def actor():
        user = db.get_user_by_session(state.auth_token)
        if user and not user.get("must_change_password") and manages_employees(user):
            return user["username"]
        return ""

    @ui.refreshable
    def desk():
        admin = actor()
        if not admin:
            ui.label("Для управления сотрудниками требуется действующий доступ администратора.")
            return
        orgs = {row["id"]: row["name"] for row in db.list_employee_organizations(admin_username=admin)}
        selected = next(iter(orgs)) if len(orgs) == 1 else None
        portal = employee_office_url(state.cfg)
        channels = {"web": "Ссылка в кабинет"}
        if state.cfg.get("telegram_bot_link"):
            channels["telegram"] = "Telegram"
        if public_bot_link(state.cfg):
            channels["max"] = "MAX"
        if portal:
            def download_qr():
                import qrcode
                png = BytesIO()
                qrcode.make(portal).save(png, format="PNG")
                ui.download.content(png.getvalue(), "employee-access.png", media_type="image/png")

            ui.button("Скачать QR-код кабинета", icon="qr_code",
                      on_click=download_qr).props("flat")
        with ui.expansion("Пригласить сотрудника", icon="person_add").classes("w-full"):
            login = ui.input("Логин").props("outlined dense")
            name = ui.input("ФИО").props("outlined dense")
            org = ui.select(orgs, label="Организация", value=selected).classes("w-full")
            roles = employee_roles_select()
            channel = ui.select(channels, label="Куда ведёт приглашение", value="web").classes("w-full")

            def invite():
                if not actor() or not org.value or not roles.value or not re.fullmatch(
                        r"[a-z][a-z0-9._-]{2,63}", (login.value or "").strip().lower()):
                    ui.notify("Укажите организацию, роли и логин латиницей длиной от 3 до 64 символов.", type="warning")
                    return
                provider = channel.value
                if provider == "web":
                    if not portal:
                        ui.notify("Укажите публичный адрес кабинета в настройках сервера.", type="warning")
                        return
                    result = db.create_account_activation(created_by=actor(), username=login.value,
                        display_name=name.value or "", organization_id=org.value, roles=list(roles.value))
                    base = portal + "/auth/activate#token="
                else:
                    result = db.create_employee_invite(admin_username=actor(), username=login.value,
                        display_name=name.value or "", organization_id=org.value, roles=list(roles.value),
                        provider=provider)
                    bot = state.cfg.get("telegram_bot_link") if provider == "telegram" else public_bot_link(state.cfg)
                    base = str(bot).rstrip("/") + "?start=invite_"
                if result.get("ok"):
                    _personal_link(base + result["token"])
                    desk.refresh()
                else:
                    ui.notify("Проверьте логин и полномочия. Для действующего сотрудника привяжите мессенджер из его кабинета.", type="warning")

            ui.button("Создать приглашение", icon="link", on_click=lambda: confirm_role_grant(roles.value, invite))

        ui.label("Заявки на доступ").classes("text-lg font-semibold")
        pending = db.list_employee_requests(admin_username=admin)
        if not pending:
            ui.label("Ожидающих заявок нет.")
        rows = db.list_managed_employees(admin_username=admin)
        for request in pending:
            with ui.card().classes("w-full"):
                ui.label(request.get("display_name") or request.get("username") or "Сотрудник").classes("font-semibold")
                org_id = request.get("organization_id") or "default"
                provider = request.get("provider") or request.get("source") or "web"
                ui.label(f"{orgs.get(org_id, org_id)} · {channels.get(provider, provider)} · №{request['id']}")
                if request.get("phone"):
                    ui.label("Телефон из заявки: " + request["phone"])
                role_field = employee_roles_select()

                async def review(approve, req_id=request["id"], role_field=role_field):
                    if not actor() or (approve and (not role_field.value or not portal)):
                        ui.notify("Проверьте полномочия, роли и публичный адрес кабинета.", type="warning")
                        return
                    result = db.review_employee_request(admin_username=actor(), request_id=req_id,
                        approve=approve, roles=list(role_field.value or []))
                    if not result.get("ok"):
                        ui.notify("Заявка уже обработана, логин занят или действие недоступно. Для существующей записи используйте личную привязку.", type="warning")
                        return
                    token = result.get("activation_token")
                    url = portal + "/auth/activate#token=" + token if token else ""
                    text = ("Доступ к кабинету одобрен. Откройте ссылку и задайте пароль.\n" + url
                            if approve else "Ваша заявка на доступ отклонена.")
                    sent = await run.io_bound(dispatch_notice, state.cfg, {**result, "text": text})
                    if approve and url and not sent:
                        _personal_link(url)
                    desk.refresh()
                    ui.notify("Заявка обработана." + (" Сотруднику отправлено уведомление." if sent else ""))

                async def accept(review=review, role_field=role_field):
                    await confirm_role_grant(role_field.value, lambda: review(True))

                async def reject(review=review):
                    await review(False)

                with ui.row():
                    ui.button("Одобрить", icon="check", on_click=accept)
                    ui.button("Отклонить", on_click=reject).props("flat")
                matching = {u["username"]: f"{u.get('display_name') or u['username']} ({u['username']})"
                            for u in rows if u.get("status") == "active" and org_id in u.get("organization_ids", [])}
                if matching:
                    with ui.expansion("У сотрудника уже есть кабинет").classes("w-full"):
                        account = ui.select(matching, label="Существующая учётная запись", with_input=True).classes("w-full")

                        async def refer(field=account, req_id=request["id"]):
                            if not field.value:
                                ui.notify("Выберите сотрудника.", type="warning")
                                return
                            out = db.refer_employee_request(admin_username=actor(), request_id=req_id, username=field.value)
                            if not out.get("ok"):
                                ui.notify("Проверьте организацию сотрудника и состояние заявки.", type="warning")
                                return
                            message = "У вас уже есть рабочий кабинет. Войдите привычным способом и подключите мессенджер вверху страницы."
                            if portal:
                                message += "\n" + portal
                            sent = await run.io_bound(dispatch_notice, state.cfg, {**out, "text": message})
                            desk.refresh()
                            ui.notify("Инструкция отправлена." if sent else "Заявка обработана. Передайте сотруднику инструкцию по входу лично.")
                        ui.button("Предложить привязку", on_click=refer).props("outline")

        with ui.expansion("Организации и права сотрудников", icon="groups").classes("w-full"):
            me = db.get_user_by_session(state.auth_token) or {}
            if me.get("is_platform_admin"):
                org_name = ui.input("Название новой организации").classes("w-full")

                def add_org():
                    out = db.create_employee_organization(admin_username=actor(), name=org_name.value or "")
                    if out.get("ok"):
                        desk.refresh()
                    else:
                        ui.notify("Проверьте название и полномочия.", type="warning")

                ui.button("Добавить организацию", on_click=add_org).props("outline")
            for employee in rows:
                employee_login = employee["username"]
                with ui.expansion(employee.get("display_name") or employee_login).classes("w-full"):
                    memberships = employee.get("organization_roles", {})
                    ui.label(employee_login)
                    for key, label in orgs.items():
                        values = memberships.get(key, [])
                        ui.label(f"{label} · " + (", ".join(ROLE_LABELS[r] for r in values) or "Доступ не назначен"))
                    membership = ui.select(orgs, label="Организация", value=selected).classes("w-full")
                    edit_roles = employee_roles_select()

                    def load_roles(key, field=edit_roles, current=memberships):
                        field.set_value(list(current.get(key) or [DEFAULT_EMPLOYEE_ROLE]))

                    if selected:
                        load_roles(selected)
                    membership.on_value_change(lambda event, load=load_roles: load(event.value))

                    def save(target=employee_login, org_field=membership, role_input=edit_roles):
                        if not org_field.value or not role_input.value:
                            ui.notify("Выберите организацию и роли.", type="warning")
                            return
                        out = db.set_employee_org_roles(admin_username=actor(), username=target,
                            organization_id=org_field.value, roles=list(role_input.value))
                        if out.get("ok"):
                            desk.refresh()
                        else:
                            ui.notify("Изменение недоступно.", type="warning")

                    ui.button("Сохранить права", on_click=lambda field=edit_roles, save=save:
                              confirm_role_grant(field.value, save)).props("outline")

                    def remove_membership(target=employee_login, org_field=membership):
                        if not org_field.value:
                            ui.notify("Выберите организацию.", type="warning")
                            return
                        out = db.set_employee_org_roles(admin_username=actor(), username=target,
                            organization_id=org_field.value, roles=[])
                        if out.get("ok"):
                            desk.refresh()
                        else:
                            ui.notify("Нельзя снять последний доступ или изменить эту запись.", type="warning")

                    ui.button("Снять доступ к выбранной организации", on_click=remove_membership).props("flat")
                    if portal and employee.get("status") == "pending":
                        def renew(target=employee_login, org_field=membership):
                            out = db.create_account_activation(created_by=actor(), username=target, display_name="",
                                organization_id=org_field.value, renew_pending=True)
                            if out.get("ok"):
                                _personal_link(portal + "/auth/activate#token=" + out["token"])
                            else:
                                ui.notify("Выберите организацию. Приглашение в мессенджер обновляется в том же боте.", type="warning")
                        ui.button("Обновить ссылку в кабинет", on_click=renew).props("outline")
                    if portal and employee.get("status") == "active" and db.can_manage_employee(admin_username=admin, username=employee_login):
                        async def recover(target=employee_login, org_field=membership):
                            if not org_field.value:
                                ui.notify("Выберите организацию сотрудника.", type="warning")
                                return
                            with ui.dialog() as confirmation, ui.card():
                                ui.label(f"Восстановить доступ для {target}?")
                                ui.label("Текущие сеансы завершатся, привязки мессенджеров и старый пароль будут отозваны.")
                                ui.button("Отмена", on_click=lambda: confirmation.submit(False)).props("flat")
                                ui.button("Выдать ссылку восстановления", on_click=lambda: confirmation.submit(True))
                            accepted = await confirmation
                            confirmation.delete()
                            if accepted:
                                out = db.create_account_activation(created_by=actor(), username=target, display_name="",
                                    organization_id=org_field.value, reset_existing=True)
                                if out.get("ok"):
                                    _personal_link(portal + "/auth/activate#token=" + out["token"])
                                    desk.refresh()
                                else:
                                    ui.notify("Проверьте организацию и полномочия.", type="warning")
                        ui.button("Восстановить доступ", on_click=recover).props("outline")
                    for key, field in (("telegram", "telegram_chat_id"), ("max", "max_user_id")):
                        if employee.get(field):
                            def remove_channel(provider=key, target=employee_login):
                                out = db.reset_employee_channel(admin_username=actor(), username=target, provider=provider)
                                if out.get("ok"):
                                    desk.refresh()
                                else:
                                    ui.notify("Для отвязки недостаточно полномочий.", type="warning")
                            ui.button("Отвязать " + ("MAX" if key == "max" else "Telegram"),
                                      on_click=remove_channel).props("outline")
    desk()
