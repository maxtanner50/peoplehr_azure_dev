import json
import os
import logging
import datetime
import uuid
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
import requests


VERSION = "VERSION_2026_02_11_peoplehr_v1_workpattern_scaffold"


def _utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _try_parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


def _require_env(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v:
        raise ValueError(f"Missing env var {name}")
    return v


def _extract_employee_id(req: func.HttpRequest) -> Optional[str]:
    content_type = (req.headers.get("content-type") or "").lower()

    data = None
    if "application/json" in content_type:
        try:
            data = req.get_json()
        except Exception:
            data = None

    if data is None:
        try:
            raw = (req.get_body() or b"").decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        data = _try_parse_json(raw)

    if not isinstance(data, dict):
        return None

    for key in ["employeeId", "EmployeeId", "employee_id"]:
        v = data.get(key)
        if v:
            return str(v).strip()

    return None


def _peoplehr_post(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    return {"http_status": r.status_code, "body_text": r.text or ""}


def _get_employee_detail(employee_id: str) -> Dict[str, Any]:
    api_key = _require_env("PEOPLEHR_API_KEY")
    url = os.environ.get(
        "PEOPLEHR_EMPLOYEE_DETAIL_URL",
        "https://api.peoplehr.net/Employee/GetEmployeeDetailById",
    )
    payload = {"APIKey": api_key, "Action": "GetEmployeeDetailById", "EmployeeId": employee_id}
    return _peoplehr_post(url, payload)


def _get_workpattern_detail(employee_id: str) -> Dict[str, Any]:
    api_key = _require_env("PEOPLEHR_API_KEY")
    url = os.environ.get(
        "PEOPLEHR_WORKPATTERN_DETAIL_URL",
        "https://api.peoplehr.net/WorkPattern/GetWorkPatternDetail",
    )
    payload = {"APIKey": api_key, "Action": "GetWorkPatternDetail", "EmployeeId": employee_id}
    return _peoplehr_post(url, payload)


def _get_display_value(result_obj: Any, field_name: str) -> str:
    if not isinstance(result_obj, dict):
        return ""
    field_obj = result_obj.get(field_name)
    if not isinstance(field_obj, dict):
        return ""
    v = field_obj.get("DisplayValue")
    if v is None:
        return ""
    return str(v).strip()


def _parse_date_yyyy_mm_dd(s: str) -> Optional[datetime.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_timestamp(s: str) -> Optional[datetime.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _normalise_week_list(pattern: Dict[str, Any]) -> List[Dict[str, Any]]:
    week = pattern.get("Week")
    if isinstance(week, list):
        return [w for w in week if isinstance(w, dict)]
    if isinstance(week, dict):
        return [week]
    return []


def _extract_weekdetail_minutes(week_obj: Dict[str, Any]) -> Tuple[int, List[int]]:
    week_detail = week_obj.get("WeekDetail")

    if not isinstance(week_detail, list):
        return 0, []

    per_day: List[int] = []
    total = 0

    for day in week_detail:
        if not isinstance(day, dict):
            continue
        mins = day.get("TotalWorkingMins")
        try:
            mins_i = int(float(mins)) if mins is not None and str(mins).strip() != "" else 0
        except Exception:
            mins_i = 0
        per_day.append(mins_i)
        total += mins_i

    return total, per_day


def _matches_employee(pattern: Dict[str, Any], employee_id: str) -> List[Dict[str, Any]]:
    assigned = pattern.get("AssignedTo")
    if not isinstance(assigned, list):
        return []
    out = []
    for a in assigned:
        if not isinstance(a, dict):
            continue
        if str(a.get("EmployeeId") or "").strip() == employee_id:
            out.append(a)
    return out


def _pick_best_assignment(assignments: List[Dict[str, Any]], as_of: Optional[datetime.date]) -> Optional[Dict[str, Any]]:
    if not assignments:
        return None

    scored: List[Tuple[Tuple[int, datetime.date, datetime.datetime], Dict[str, Any]]] = []

    for a in assignments:
        eff = _parse_date_yyyy_mm_dd(str(a.get("EffectiveDate") or ""))
        ts = _parse_timestamp(str(a.get("TimeStamp") or ""))

        eff_key = eff if eff is not None else datetime.date(1900, 1, 1)
        ts_key = ts if ts is not None else datetime.datetime(1900, 1, 1, 0, 0, 0)

        if as_of is None or eff is None:
            bucket = 0
        else:
            bucket = 1 if eff <= as_of else 0

        scored.append(((bucket, eff_key, ts_key), a))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def _resolve_weekly_hours_from_workpattern_body(workpattern_body_text: str, employee_id: str, as_of_date: Optional[str]) -> Dict[str, Any]:
    parsed = _try_parse_json(workpattern_body_text)
    if not isinstance(parsed, dict):
        raise ValueError("Invalid WorkPattern response (not JSON object)")

    results = parsed.get("Result")
    if not isinstance(results, list) or not results:
        raise ValueError("WorkPattern response missing/empty Result array")

    as_of = _parse_date_yyyy_mm_dd(as_of_date or "")

    # Optional filter: comma-separated WorkPatternIds
    filter_raw = (os.environ.get("PEOPLEHR_WORKPATTERN_ID_FILTER") or "").strip()
    filter_ids: Optional[set] = None
    if filter_raw:
        ids = set()
        for part in filter_raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ids.add(int(part))
            except Exception:
                continue
        filter_ids = ids if ids else None

    candidates: List[Tuple[Tuple[int, datetime.date, datetime.datetime], Dict[str, Any], Dict[str, Any]]] = []
    for item in results:
        if not isinstance(item, dict):
            continue

        wp_id = item.get("WorkPatternId")
        try:
            wp_id_int = int(wp_id)
        except Exception:
            wp_id_int = None

        if filter_ids is not None and wp_id_int is not None and wp_id_int not in filter_ids:
            continue

        matches = _matches_employee(item, employee_id)
        if not matches:
            continue

        best_assignment = _pick_best_assignment(matches, as_of)
        if not best_assignment:
            continue

        eff = _parse_date_yyyy_mm_dd(str(best_assignment.get("EffectiveDate") or ""))
        ts = _parse_timestamp(str(best_assignment.get("TimeStamp") or ""))

        eff_key = eff if eff is not None else datetime.date(1900, 1, 1)
        ts_key = ts if ts is not None else datetime.datetime(1900, 1, 1, 0, 0, 0)

        if as_of is None or eff is None:
            bucket = 1
        else:
            bucket = 2 if eff <= as_of else 1

        score = (bucket, eff_key, ts_key)
        candidates.append((score, item, best_assignment))

    if not candidates:
        raise ValueError(f"No WorkPattern assignment found for employee_id={employee_id}")

    candidates.sort(key=lambda x: x[0], reverse=True)
    _, chosen_pattern, chosen_assignment = candidates[0]

    weeks = _normalise_week_list(chosen_pattern)
    if not weeks:
        raise ValueError("Missing Week data (expected list or object)")

    weekly_total = 0
    per_day_minutes: List[int] = []

    for w in weeks:
        w_total, w_per_day = _extract_weekdetail_minutes(w)
        weekly_total += w_total
        if w_per_day:
            per_day_minutes = w_per_day

    weekly_hours = round(weekly_total / 60.0, 2)

    return {
        "weekly_minutes": weekly_total,
        "weekly_hours": weekly_hours,
        "per_day_minutes": per_day_minutes,
        "selected": {
            "workpattern_id": chosen_pattern.get("WorkPatternId"),
            "workpattern_name": chosen_pattern.get("WorkPatternName"),
            "assignment_effective_date": chosen_assignment.get("EffectiveDate"),
            "assignment_timestamp": chosen_assignment.get("TimeStamp"),
        },
        "debug": {
            "result_count": len(results),
            "candidate_count": len(candidates),
            "filter_ids_active": list(filter_ids) if filter_ids is not None else None,
        },
    }


def main(req: func.HttpRequest) -> func.HttpResponse:
    run_id = str(uuid.uuid4())
    logging.info(f"[{VERSION}] run_id={run_id} started")

    employee_id = _extract_employee_id(req)
    if not employee_id:
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "version": VERSION,
                    "run_id": run_id,
                    "error": "Missing employeeId in request body",
                }
            ),
            status_code=400,
            mimetype="application/json",
        )

    try:
        emp = _get_employee_detail(employee_id)
        emp_json = _try_parse_json(emp["body_text"])
        emp_result = emp_json.get("Result") if isinstance(emp_json, dict) else None
        start_date = _get_display_value(emp_result, "StartDate")

        wp = _get_workpattern_detail(employee_id)

        resolved = _resolve_weekly_hours_from_workpattern_body(wp["body_text"], employee_id, start_date)

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "version": VERSION,
                    "run_id": run_id,
                    "employee_id": employee_id,
                    "peoplehr_employee_http_status": emp["http_status"],
                    "peoplehr_workpattern_http_status": wp["http_status"],
                    "start_date": start_date,
                    "resolved": resolved,
                    "mode": "dry_run",
                    "captured_at": _utc_now_iso(),
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        logging.exception(f"[{VERSION}] run_id={run_id} failed")
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "version": VERSION,
                    "run_id": run_id,
                    "employee_id": employee_id,
                    "error": str(e),
                }
            ),
            status_code=500,
            mimetype="application/json",
        )
