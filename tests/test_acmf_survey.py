"""Unit tests for the ACMF survey answer log (run_acmf_survey and helpers).

Synthetic flow, runs and messages only — no network. Run from the repo root:
    python -m unittest discover -s tests
"""
import os
import sys
import unittest
from unittest import mock

for k, v in {
    "YELLOW_FLOW": "f-yellow", "ACMF_FLOW": "f-acmf", "V4W_FLOW": "f-v4w",
    "YES_FLOW": "f-yes", "SHEET_SERVICE": "https://sheets.test", "SHEET_ID": "sheet-1",
    "ACMF_DECLINED_FLOW": "f-survey", "ACMF_NR_FLOW": "",
    "ACMF_SURVEY_TAB": "ACMF Survey Responses", "TEXTIT_TOKEN": "t",
}.items():
    os.environ[k] = v
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src import main  # noqa: E402

FLOW = "f-survey"
CFG = (FLOW, "Declined", "result_2", "result_6", ["result_3"], "message_variant")


def _msg(uid, text, dest):
    return {"uuid": uid, "actions": [{"type": "send_msg", "text": text}],
            "exits": [{"uuid": uid + "-x", "destination_uuid": dest}]}


def _wait(uid, name, cats, timeout_dest=None):
    """cats: [(category name, destination)]"""
    categories, exits = [], []
    for i, (cname, dest) in enumerate(cats):
        categories.append({"uuid": f"{uid}-c{i}", "name": cname, "exit_uuid": f"{uid}-e{i}"})
        exits.append({"uuid": f"{uid}-e{i}", "destination_uuid": dest})
    wait = {"type": "msg"}
    if timeout_dest:
        categories.append({"uuid": f"{uid}-cnr", "name": "No Response", "exit_uuid": f"{uid}-enr"})
        exits.append({"uuid": f"{uid}-enr", "destination_uuid": timeout_dest})
        wait["timeout"] = {"seconds": 21600, "category_uuid": f"{uid}-cnr"}
    return {"uuid": uid, "actions": [], "exits": exits,
            "router": {"type": "switch", "operand": "@input.text", "result_name": name,
                       "wait": wait, "categories": categories}}


def _window(uid, wait_uuid, reminder_uuid):
    return {"uuid": uid, "actions": [],
            "exits": [{"uuid": uid + "-in", "destination_uuid": reminder_uuid},
                      {"uuid": uid + "-out", "destination_uuid": wait_uuid}],
            "router": {"type": "switch",
                       "operand": '@(format_datetime(now(), "tt", "America/Phoenix"))',
                       "categories": [
                           {"uuid": uid + "-ci", "name": "In Window", "exit_uuid": uid + "-in"},
                           {"uuid": uid + "-co", "name": "Other", "exit_uuid": uid + "-out"}]}}


def _enter(uid, flow_uuid, name, dest):
    return {"uuid": uid,
            "actions": [{"type": "enter_flow", "flow": {"uuid": flow_uuid, "name": name}}],
            "exits": [{"uuid": uid + "-x", "destination_uuid": dest}],
            "router": {"type": "switch", "operand": "@child.status",
                       "categories": [{"uuid": uid + "-c", "name": "Complete",
                                       "exit_uuid": uid + "-x"}]}}


def _field(uid, value, dest):
    return {"uuid": uid,
            "actions": [{"type": "set_contact_field", "field": {"key": "temp"}, "value": value}],
            "exits": [{"uuid": uid + "-x", "destination_uuid": dest}]}


DEFINITION = {"uuid": FLOW, "nodes": [
    _msg("n-start", "Hi. Which fits?\r\n \r\n1) Found other support\r\n2) Not needed\r\n"
                    "3) Something else ", "w-q1"),
    _wait("w-q1", "Result 2", [("1", "n-close1"), ("2", "n-variant"), ("3", "n-ftq"),
                               ("Other", "n-humble1")], timeout_dest="win-q1"),
    _msg("n-humble1", "I'm just a humble chatbot. Please reply with a number.", "w-q1"),
    _window("win-q1", "w-q1", "n-reminder"),
    _msg("n-reminder", "Remember you can always reach them directly.", None),
    _msg("n-close1", "Glad you found support. Take good care.", None),
    {"uuid": "n-variant", "actions": [], "exits": [{"uuid": "v-x", "destination_uuid": "n-ynq"}],
     "router": {"type": "random", "result_name": "Message Variant",
                "categories": [{"uuid": "v-c", "name": "social norm", "exit_uuid": "v-x"}]}},
    _msg("n-ynq", "Want me to ask them to reach out again? (Y/N)", "w-yn"),
    _wait("w-yn", "Result 6", [("yes", "e-rc"), ("no", "n-no"), ("Other", "n-humble2")]),
    _msg("n-humble2", "I'm just a humble chatbot. Please reply YES or NO.", "w-yn"),
    _enter("e-rc", "f-requestcall", "Request call", "n-great"),
    _msg("n-great", "Great, I'll pass your information along again.", None),
    _msg("n-no", "No problem. Take good care.", None),
    _msg("n-ftq", "Can you tell me a bit more?", "w-ft"),
    _wait("w-ft", "Result 3", [("All Responses", "f-set")]),
    _field("f-set", "Freetext", "e-tl"),
    _enter("e-tl", "f-toplevel", "Catch-all top level", "f-clear"),
    _field("f-clear", None, "n-ftclose"),
    _msg("n-ftclose", "Got it. I'll pass this to the team. Take good care.", None),
]}
SHAPE = main.flow_shape(DEFINITION)


def _p(node, t):
    return {"node": node, "time": t}


def _m(t, direction, text, flow=FLOW):
    return {"created_on": t, "direction": direction, "text": text,
            "flow": {"uuid": flow} if flow else None}


def _run(path, values, exit_type=None, exited_on=None, created="2026-10-14T20:00:00Z"):
    return {"uuid": "run-1", "contact": {"uuid": "c-1", "name": "Test Person"},
            "path": path, "values": values, "created_on": created,
            "exited_on": exited_on, "exit_type": exit_type}


class FlowShapeTests(unittest.TestCase):
    def test_roles_derived_from_definition(self):
        self.assertEqual(SHAPE["wait_by_key"]["result_2"], "w-q1")
        self.assertEqual(SHAPE["humble"], {"n-humble1": "w-q1", "n-humble2": "w-yn"})
        self.assertEqual(SHAPE["window"], {"win-q1"})
        self.assertEqual(SHAPE["reminder"], {"n-reminder"})
        self.assertEqual(main._yesno_branch_enter(SHAPE, "result_6"), "e-rc")

    def test_choice_meanings(self):
        m = main.choice_meanings(SHAPE["msg_text"]["n-start"])
        self.assertEqual(m, {"1": "Found other support", "2": "Not needed",
                             "3": "Something else"})


class RowTests(unittest.TestCase):
    def test_yes_path_with_rejects_variant_and_rereferral(self):
        run = _run(
            [_p("n-start", "2026-10-14T20:00:00Z"), _p("w-q1", "2026-10-14T20:00:00Z"),
             _p("n-humble1", "2026-10-14T20:01:00Z"), _p("w-q1", "2026-10-14T20:01:00Z"),
             _p("n-variant", "2026-10-14T20:02:00.5Z"), _p("n-ynq", "2026-10-14T20:02:00.5Z"),
             _p("w-yn", "2026-10-14T20:02:00.5Z"), _p("n-humble2", "2026-10-14T20:03:00Z"),
             _p("w-yn", "2026-10-14T20:03:00Z"), _p("e-rc", "2026-10-14T20:04:00.5Z"),
             _p("n-great", "2026-10-14T20:04:05Z")],
            {"result_2": {"value": "2", "category": "2", "time": "2026-10-14T20:02:00.5Z"},
             "message_variant": {"value": "", "category": "social norm",
                                 "time": "2026-10-14T20:02:00.5Z"},
             "result_6": {"value": "yeah", "category": "yes", "time": "2026-10-14T20:04:00.5Z"}},
            exit_type="completed", exited_on="2026-10-14T20:04:05Z")
        msgs = [_m("2026-10-14T20:00:00Z", "out", "Hi"),
                _m("2026-10-14T20:01:00Z", "in", "no one called"),
                _m("2026-10-14T20:01:00Z", "out", "humble"),
                _m("2026-10-14T20:02:00Z", "in", "2"),
                _m("2026-10-14T20:02:00.5Z", "out", "Want me to"),
                _m("2026-10-14T20:03:00Z", "in", "maybe?"),
                _m("2026-10-14T20:03:00Z", "out", "humble"),
                _m("2026-10-14T20:04:00Z", "in", "yeah"),
                _m("2026-10-14T20:04:05Z", "out", "Great")]
        child = {"flow": {"uuid": "f-requestcall"}, "created_on": "2026-10-14T20:04:01Z",
                 "exit_type": "completed",
                 "values": {"acmf_submission": {"category": "Success", "value": "200"},
                            "sheet_log": {"category": "Success", "value": "200"}}}
        row = main.acmf_survey_row(run, CFG, SHAPE, msgs, [child])
        self.assertEqual(row["q1 choice"], "2")
        self.assertEqual(row["q1 choice meaning"], "Not needed")
        self.assertEqual(row["q1 rejected replies"], "no one called")
        self.assertEqual(row["message variant"], "social norm")
        self.assertEqual(row["yes/no question"], "Want me to ask them to reach out again? (Y/N)")
        self.assertEqual(row["yes/no reply"], "yeah")
        self.assertEqual(row["yes/no answer"], "Yes")
        self.assertEqual(row["yes/no rejected replies"], "maybe?")
        self.assertEqual(row["re-referral step"], "completed")
        self.assertEqual(row["ACMF form result"], "Success (200)")
        self.assertEqual(row["Referrals sheet result"], "Success (200)")
        self.assertEqual(row["messages sent"], "5")
        self.assertEqual(row["first reply at"], "2026-10-14 16:01")
        self.assertEqual(row["minutes to first reply"], "1.0")
        self.assertEqual(row["closing message"], "Great, I'll pass your information along again.")
        self.assertEqual(row["reminder sent"], "no")
        self.assertTrue(set(main.ACMF_RUN_FIELDS) <= set(row))
        tab = main.acmf_sheet_row(row)
        self.assertEqual(list(tab), main.ACMF_SURVEY_COLUMNS)
        self.assertEqual(tab["status"], "Answered")
        self.assertEqual(tab["option chosen"], "2 – Not needed")
        self.assertEqual(tab["message shown"], "social norm")
        self.assertEqual(tab["wants follow-up"], "Yes")
        self.assertEqual(tab["re-referred"], "sent to ACMF")
        self.assertEqual(tab["reminder"], "")
        self.assertIn("q1 rejected replies: no one called", tab["details"].splitlines())
        self.assertNotIn("message variant: social norm", tab["details"])

    def test_timeout_hold_then_reminder(self):
        run = _run(
            [_p("n-start", "2026-10-14T20:00:00Z"), _p("w-q1", "2026-10-14T20:00:00Z"),
             _p("win-q1", "2026-10-15T02:00:00Z"), _p("w-q1", "2026-10-15T02:00:00Z"),
             _p("win-q1", "2026-10-15T08:00:00Z"), _p("w-q1", "2026-10-15T08:00:00Z"),
             _p("win-q1", "2026-10-15T14:00:00Z"), _p("n-reminder", "2026-10-15T14:00:00Z")],
            {"result_2": {"value": "", "category": "No Response", "time": "2026-10-15T14:00:00Z"}},
            exit_type="completed", exited_on="2026-10-15T14:00:00Z")
        row = main.acmf_survey_row(run, CFG, SHAPE, [_m("2026-10-14T20:00:00Z", "out", "Hi")], [])
        self.assertEqual(row["reminder sent"], "yes")
        self.assertEqual(row["reminder after"], "q1 wait")
        self.assertEqual(row["reminder sent at"], "2026-10-15 10:00")
        self.assertEqual(row["window holds"], "2")
        self.assertEqual(row["q1 choice"], "")
        self.assertEqual(row["q1 answered at"], "")
        self.assertEqual(row["closing message"], "Remember you can always reach them directly.")
        tab = main.acmf_sheet_row(row)
        self.assertEqual(tab["status"], "No reply, reminder sent")
        self.assertEqual(tab["option chosen"], "No reply")
        self.assertEqual(tab["reminder"], "after the number question")
        self.assertIn("window holds: 2", tab["details"].splitlines())

    def test_interrupted_and_replies_after_end(self):
        run = _run([_p("n-start", "2026-10-14T20:00:00Z"), _p("w-q1", "2026-10-14T20:00:00Z")],
                   {}, exit_type="interrupted", exited_on="2026-10-15T13:00:00Z")
        others = [{"flow": {"uuid": "f-sub", "name": "Earlier flow"},
                   "created_on": "2026-10-14T19:59:30Z"},
                  {"flow": {"uuid": "f-checkin", "name": "Contact with Subscribers"},
                   "created_on": "2026-10-15T13:00:00Z"}]
        msgs = [_m("2026-10-14T20:00:00Z", "out", "Hi"),
                _m("2026-10-15T13:05:00Z", "in", "3", flow="f-checkin"),
                _m("2026-10-19T13:05:00Z", "in", "too late", flow=None)]
        row = main.acmf_survey_row(run, CFG, SHAPE, msgs, others)
        self.assertEqual(row["how it ended"], "interrupted")
        self.assertEqual(row["interrupted by"], "Contact with Subscribers")
        self.assertEqual(row["replies after end"], "3")
        self.assertEqual(row["ended"], "2026-10-15 09:00")
        tab = main.acmf_sheet_row(row)
        self.assertEqual(tab["status"], "Cut short by Contact with Subscribers")
        self.assertEqual(tab["replied after survey"], "3")
        self.assertEqual(tab["option chosen"], "")

    def test_waiting_run(self):
        run = _run([_p("n-start", "2026-10-14T20:00:00Z"), _p("w-q1", "2026-10-14T20:00:00Z")], {})
        row = main.acmf_survey_row(run, CFG, SHAPE, [_m("2026-10-14T20:00:00Z", "out", "Hi")], [])
        self.assertEqual(row["how it ended"], "waiting")
        self.assertEqual(row["ended"], "")
        self.assertEqual(row["closing message"], "")
        tab = main.acmf_sheet_row(row)
        self.assertEqual(tab["status"], "Waiting")
        self.assertEqual(tab["option chosen"], "")
        self.assertEqual(tab["wants follow-up"], "")

    def test_free_text_and_catch_all(self):
        run = _run(
            [_p("n-start", "2026-10-14T20:00:00Z"), _p("w-q1", "2026-10-14T20:00:00Z"),
             _p("n-ftq", "2026-10-14T20:01:00.2Z"), _p("w-ft", "2026-10-14T20:01:00.2Z"),
             _p("f-set", "2026-10-14T20:02:00.2Z"), _p("e-tl", "2026-10-14T20:02:00.2Z"),
             _p("f-clear", "2026-10-14T20:02:02Z"), _p("n-ftclose", "2026-10-14T20:02:02Z")],
            {"result_2": {"value": "3", "category": "3", "time": "2026-10-14T20:01:00.2Z"},
             "result_3": {"value": "they never called", "category": "All Responses",
                          "time": "2026-10-14T20:02:00.2Z"}},
            exit_type="completed", exited_on="2026-10-14T20:02:02Z")
        umf = {"flow": {"uuid": "f-umf"}, "created_on": "2026-10-14T20:02:00.3Z",
               "values": {"bucket": {"value": "other"}, "message_id": {"value": "m-9"},
                          "referring_flow_id": {"value": FLOW}}}
        triage = {"flow": {"uuid": "f-triage"}, "created_on": "2026-10-14T20:02:00.5Z",
                  "values": {"triagerequestid": {"value": "x"}}}
        det = {"flow": {"uuid": "f-det"}, "created_on": "2026-10-14T20:10:00Z",
               "values": {"determination": {"value": "LowConcern"},
                          "message_id": {"value": "m-9"}}}
        msgs = [_m("2026-10-14T20:00:00Z", "out", "Hi"),
                _m("2026-10-14T20:01:00Z", "in", "3"),
                _m("2026-10-14T20:01:00.2Z", "out", "Can you tell me"),
                _m("2026-10-14T20:02:00Z", "in", "they never called"),
                _m("2026-10-14T20:02:00.4Z", "out", "Sorry - I am just a humble chatbot", "f-umf"),
                _m("2026-10-14T20:02:00.4Z", "out", "call 988, then Press 1", "f-crisis"),
                _m("2026-10-14T20:02:02Z", "out", "Got it")]
        row = main.acmf_survey_row(run, CFG, SHAPE, msgs, [umf, triage, det])
        self.assertEqual(row["free-text question"], "Can you tell me a bit more?")
        self.assertEqual(row["free-text answer"], "they never called")
        self.assertEqual(row["catch-all category"], "other")
        self.assertEqual(row["humble line sent"], "yes")
        self.assertEqual(row["crisis numbers sent"], "yes")
        self.assertEqual(row["triage started"], "yes")
        self.assertEqual(row["counselor determination"], "LowConcern")
        self.assertEqual(row["determination at"], "2026-10-14 16:10")
        self.assertIn("enter Catch-all top level", row["step trail"])
        tab = main.acmf_sheet_row(row)
        self.assertEqual(tab["their words"], "they never called")
        self.assertEqual(tab["triage"], "LowConcern, 2026-10-14 16:10")
        self.assertEqual(tab["option chosen"], "3 – Something else")


class TabRowTests(unittest.TestCase):
    def _full(self, **kw):
        full = {f: "" for f in main.ACMF_RUN_FIELDS}
        full.update({"_q1 timed out": False, "_reminder after kind": "",
                     "_started utc": "2026-10-14T20:00:00Z", "run uuid": "r1",
                     "uuid": "c-1", "survey": "Declined", "started": "2026-10-14 16:00",
                     "how it ended": "completed"})
        full.update(kw)
        return full

    def test_rereferral_failed_and_expired(self):
        tab = main.acmf_sheet_row(self._full(**{
            "re-referral step": "completed", "ACMF form result": "Failure (500)",
            "how it ended": "expired"}))
        self.assertEqual(tab["re-referred"], "failed (Failure (500))")
        self.assertEqual(tab["status"], "Expired")
        tab = main.acmf_sheet_row(self._full(**{"re-referral step": "expired"}))
        self.assertEqual(tab["re-referred"], "failed (no form result)")

    def test_details_order_omits_empty_and_cuts_trail(self):
        trail = " › ".join(f"10-14 16:{i % 60:02d} step {i}" for i in range(6000))
        tab = main.acmf_sheet_row(self._full(**{"ended": "2026-10-14 16:05",
                                                 "step trail": trail}))
        d = tab["details"]
        self.assertLess(len(d), main.ACMF_DETAILS_MAX)
        lines = d.splitlines()
        self.assertEqual(lines[:4], ["run uuid: r1", "started: 2026-10-14 16:00",
                                     "ended: 2026-10-14 16:05", "how it ended: completed"])
        self.assertTrue(lines[-1].startswith("step trail (cut to fit the cell: first "))
        self.assertIn("of 6000 steps)", lines[-1])

    def test_one_row_per_contact_latest_run(self):
        old = self._full(**{"run uuid": "r-old", "_started utc": "2026-10-10T20:00:00Z",
                            "started": "2026-10-10 16:00"})
        new = self._full(**{"run uuid": "r-new", "_started utc": "2026-10-14T20:00:00Z"})
        rows = main.acmf_rows_by_contact([new, old])
        self.assertEqual(len(rows), 1)
        self.assertIn("run uuid: r-new", rows[0]["details"])
        self.assertIn("other survey runs: Declined run r-old started 2026-10-10 16:00",
                      rows[0]["details"])


class _FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class WriteTests(unittest.TestCase):
    def _row(self, rid, answer=""):
        row = {c: "" for c in main.ACMF_SURVEY_COLUMNS}
        row.update({"uuid": rid, "name": "P " + rid, "survey": "Declined",
                    "sent": "2026-10-14 16:00", "their words": answer,
                    "details": "run uuid: x\nstarted: 2026-10-14 16:00"})
        return row

    def _post(self, headers_present=True, existing=()):
        calls = []

        def post(url, json=None, timeout=None):
            calls.append((url, json))
            route = url.rsplit("/", 1)[1]
            if route == "coordinates":
                body = {} if not headers_present else {
                    c["result_name"]: "x" for c in json["coordinates"]
                    if c["column"] != "details" or headers_present == "all"}
                return _FakeResponse(200, body)
            if route == "read":
                return _FakeResponse(200, {"rows": list(existing)})
            if route == "write":
                if json["newrow"] == "no":
                    return _FakeResponse(200, {"matched": 1})
                return _FakeResponse(200, {"row": 2})
            raise AssertionError(url)
        return post, calls

    def test_missing_headers_refuse_and_name_them(self):
        post, calls = self._post(headers_present=True)
        with mock.patch.object(main.requests, "post", post), \
                mock.patch.object(main, "build_acmf_rows", return_value=[self._row("r1")]):
            with self.assertRaises(main.AcmfHeaderError) as ctx:
                main.run_acmf_survey()
        self.assertIn("details", str(ctx.exception))
        self.assertFalse([c for c in calls if c[0].endswith("/write")])

    def test_upsert_insert_update_unchanged_only_on_own_tab(self):
        same = self._row("r1")
        existing = [
            {"_rowNumber": 2, **{c: v for c, v in same.items()},
             "sent": "2026-10-14 16:00:00"},
            {"_rowNumber": 3, **self._row("r2", "old answer")},
        ]
        rows = [same, self._row("r2", "=new answer"), self._row("r3")]
        post, calls = self._post(headers_present="all", existing=existing)
        with mock.patch.object(main.requests, "post", post), \
                mock.patch.object(main, "build_acmf_rows", return_value=rows), \
                mock.patch.object(main.time, "sleep"):
            stats = main.run_acmf_survey()
        self.assertEqual(stats, {"acmf_survey_rows": 3, "acmf_survey_inserted": 1,
                                 "acmf_survey_updated": 1, "acmf_survey_unchanged": 1})
        writes = [b for u, b in calls if u.endswith("/write")]
        self.assertEqual(len(writes), 2)
        for b in writes:
            self.assertEqual(b["tab"], "ACMF Survey Responses")
            self.assertEqual(b["sheetid"], "sheet-1")
            self.assertEqual(b["key"], "uuid")
        upd = next(b for b in writes if b["newrow"] == "no")
        self.assertEqual(upd["uuid"], "r2")
        self.assertEqual(upd["their words"], "'=new answer")
        self.assertEqual(sorted(k for k in upd if k not in (
            "tab", "key", "newrow", "password", "sheetid")), sorted(main.ACMF_SURVEY_COLUMNS))
        ins = next(b for b in writes if b["newrow"] == "yes")
        self.assertEqual(ins["uuid"], "r3")
        for u, b in calls:
            self.assertNotIn(b.get("tab"), ("Referrals", "Flow", "Requests"))

    def test_dry_run_makes_no_sheet_call(self):
        def post(*a, **k):
            raise AssertionError("sheet call in dry run")
        with mock.patch.object(main.requests, "post", post), \
                mock.patch.object(main, "build_acmf_rows", return_value=[self._row("r1")]):
            res = main.run_acmf_survey(dry_run=True)
        self.assertEqual(res["acmf_survey_rows"], 1)

    def test_unconfigured_is_a_no_op(self):
        with mock.patch.object(main, "ACMF_SURVEY_TAB", None):
            self.assertEqual(main.run_acmf_survey(), {})


if __name__ == "__main__":
    unittest.main()
