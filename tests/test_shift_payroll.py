import json

import pytest

from rag_catalog.core.shift_payroll import MonthlyPayroll
from rag_catalog.core.work_shifts import WorkShiftJournal
from test_shift_roles import accounts, data


def test_personal_union_percent_of_gross_before_deductions(payroll):
    j, p, t = payroll
    a = t["cloud"]
    p.add_item(a, "default", "2026-10", "driver", "bonus", "10000.50", "Приказ")
    p.add_item(a, "default", "2026-10", "both", "bonus", "10000.50", "Приказ")
    p.add_item(a, "default", "2026-10", "driver", "advance", 4000, "Аванс")
    with pytest.raises(ValueError):
        p.add_item(a, "default", "2026-10", "driver", "union", None, "Заявление", percent=1)
    key = p.add_item(
        a, "default", "2026-10", "driver", "union", None, "Заявление", percent=1, consent=True, monthly=True
    )
    with pytest.raises(ValueError):
        p.add_item(a, "default", "2026-11", "driver", "union", None, "Второй взнос", percent=1, consent=True)
    s = p.sheet(a, "default", "2026-10")
    line = next(r for r in s["items"] if r["id"] == key)
    assert (line["earnings"], line["basis_points"], line["calculated"]) == (1000050, 100, 10001)
    assert s["people"]["driver"]["estimate"] == 590049
    assert s["people"]["both"]["voluntary"] == 0
    p.stop_item(a, "default", "2026-11", key)
    assert p.add_item(
        a, "default", "2026-11", "driver", "union", None, "Новое заявление", percent="0,5", consent=True
    )
    assert p.sheet(a, "default", "2026-10")["people"]["driver"]["voluntary"] == 10001


@pytest.fixture
def payroll(accounts, tmp_path):
    auth, tokens = accounts
    auth.set_payroll_owner("cloud")
    journal = WorkShiftJournal(tmp_path / "journal.db", auth)
    return journal, MonthlyPayroll(journal), tokens


def test_owner_is_unique_and_checked_again_for_existing_sessions(accounts,tmp_path):
    auth, t = accounts
    j = WorkShiftJournal(tmp_path / "shifts.db", auth)
    p = MonthlyPayroll(j)
    for token in t.values():
        with pytest.raises(PermissionError):
            p.organizations(token)
    auth.set_payroll_owner("cloud")
    auth.set_payroll_owner("cloud")
    assert "default" in p.organizations(t["cloud"])
    with pytest.raises(PermissionError):
        j.organizations(t["cloud"], manage=True)
    with pytest.raises(ValueError):
        auth.set_payroll_owner("both")
    for name in ("admin","dispatch","driver","both"):
        tok=t[name]
        for f,args,kwargs in [
            (p.sheet,("default","2026-10"),{}), (p.employees,("default",),{}), (p.rate_options,("default",),{}),
            (p.set_salary,("default","2026-10","driver",1),{}), (p.set_norm,("default","2026-10",[]),{"revision":None}),
            (p.add_type,("default","X","credit"),{}), (p.add_item,("default","2026-10","driver","bonus",1,"X"),{}),
            (p.stop_item,("default","2026-10",1),{}), (j.add_rate,(),dict(organization_id="default",work="X",unit="hour",price=1,since="2026-01-01"))
        ]:
            with pytest.raises(PermissionError):
                f(tok,*args,**kwargs)
    auth.set_payroll_owner("both",expected_previous="cloud")
    assert p.organizations(t["both"])
    with pytest.raises(PermissionError):
        p.sheet(t["cloud"],"default","2026-10")


def test_journal_surfaces_never_return_pay_and_drivers_only_see_their_hours(payroll):
    j,p,t = payroll
    rate = j.add_rate(t["cloud"],organization_id="default",work="Work",unit="hour",price="8765.43",since="2026-01-01")
    filters = dict(date_from="2026-10-01",date_to="2026-10-31")
    payload = {**data(),"employee":"driver","work_lines":[dict(rate_id=rate,quantity=2)]}
    r=j.save(t["driver"],payload,source_event_id="test-request")
    replay=j.save(t["driver"],payload,source_event_id="test-request")
    d=j.save(t["dispatch"],{**data(),"employee":"both"})
    approved=j.transition(t["dispatch"],r["id"],1,"approved")
    for public in (r,replay,approved,j.audit(t["dispatch"]),j.month_sheet(t["dispatch"],"2026-10")):
        assert all(term not in json.dumps(public) for term in ("kopecks","amount","876543","1753086"))
    for name in ("driver","dispatch","admin"):
        token=t[name]
        for public in (j.get(token,r["id"]),j.history(token,r["id"]),j.list(token,**filters),
                       j.rates(token,organization_id="default",work_date="2026-10-07")):
            assert all(term not in json.dumps(public) for term in ("kopecks","amount","876543","1753086"))
        csv=j.export_csv(token,**filters).decode("utf-8-sig")
        assert "руб" not in csv and "17530.86" not in csv
    assert j.list(t["driver"],employee="both",**filters)["work_minutes"]==480
    with pytest.raises(PermissionError):
        j.get(t["driver"],d["id"])
    assert p.sheet(t["cloud"],"default","2026-10")["people"]["driver"]["piece"]==1753086


def test_calendar_salary_and_approved_minutes_only(payroll):
    j, p, t = payroll
    actor = t["cloud"]
    key = j.save_employee(
        t["dispatch"], offline=True, create=True, display_name="Office", categories=["office", "engineer"]
    )
    p.set_salary(actor, "default", "2026-10", key, "50000")
    norm = [480] * 20 + [0] * 11
    assert p.sheet(actor, "default", "2026-10")["people"][key]["estimate"] is None
    p.set_norm(actor, "default", "2026-10", norm, revision=None)
    with pytest.raises(ValueError):
        p.set_norm(actor, "default", "2026-10", norm, revision=None)
    row = j.save(
        t["dispatch"], {**data(), "entry_type": "attendance", "employee": key, "equipment": "", "workplace": ""}
    )
    before = p.sheet(actor, "default", "2026-10")["people"][key]
    assert before["estimate"] == 0 and before["pending"] == 480
    j.transition(t["dispatch"], row["id"], 1, "approved")
    person = p.sheet(actor, "default", "2026-10")["people"][key]
    assert person["estimate"] == 250000 and person["minutes"] == 480 and person["pending"] == 0
    revision = p.sheet(actor, "default", "2026-10")["norm_id"]
    p.set_norm(actor, "default", "2026-10", [360] + [0] * 30, revision=revision)
    person = p.sheet(actor, "default", "2026-10")["people"][key]
    assert person["estimate"] == 6666667 and person["extra_minutes"] == 120 and person["extra_cents"] == 1666667
    p.set_salary(actor, "default", "2026-11", key, "60000")
    assert p.sheet(actor, "default", "2026-10")["people"][key]["salary"] == 5000000


def test_recurring_item_consent_stop_and_no_cross_org_or_driver_access(payroll):
    j, p, t = payroll
    actor = t["cloud"]
    kind = p.add_type(actor, "default", "За сменность", "credit")
    for group in ("credit", "debit"):
        with pytest.raises(ValueError):
            p.add_type(actor, "default", "за сменность", group)
    item = p.add_item(actor, "default", "2026-10", "driver", kind, "1000.01", "Приказ 8", monthly=True)
    p.add_item(actor, "default", "2026-10", "driver", "deduction", 200, "Документ 1")
    p.add_item(actor, "default", "2026-10", "driver", "note", 500, "Справочная отметка")
    with pytest.raises(ValueError):
        p.add_item(actor, "default", "2026-10", "driver", "contribution", 100, "Сбор")
    p.add_item(actor, "default", "2026-10", "driver", "contribution", 100, "Заявление 7", consent=True)
    assert p.sheet(actor, "default", "2026-10")["people"]["driver"]["estimate"] == 70001
    assert p.sheet(actor, "default", "2026-11")["people"]["driver"]["estimate"] == 100001
    p.stop_item(actor, "default", "2026-11", item)
    assert p.sheet(actor, "default", "2026-11")["items"][0]["stopped"] == 1
    assert "driver" not in p.sheet(actor, "default", "2026-12")["people"]
    assert p.sheet(actor, "default", "2026-10")["people"]["driver"]["estimate"] == 70001
    with pytest.raises(PermissionError):
        p.sheet(t["driver"], "default", "2026-10")
    with pytest.raises(PermissionError):
        p.add_item(actor, "other", "2026-10", "driver", "bonus", 100, "Приказ")
    with pytest.raises(ValueError):
        p.add_item(actor, "default", "2026-10", "driver", "bonus", "-100", "Приказ")
