import json
import os
import logging
import datetime
import uuid
from typing import Any, Dict, Optional

import azure.functions as func
import requests


VERSION = "VERSION_2026_02_11_peoplehr_v9_send_to_famly_no_blob"


FAMLY_CREATE_EMPLOYEES_MUTATION = (
    "mutation CreateEmployees($createEmployees:[EmployeeInput!]!){ "
    "employees{ create(employees:$createEmployees){ id }}}"
)


def _mask(s: str, keep_start: int = 4, keep_end: int = 4) -> str:
    if not s:
        return ""
    if len(s) <= keep_start + keep_end:
        return "*" * len(s)
    return f"{s[:keep_start]}***{s[-keep_end:]}"


def _utc_now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat() + "Z"


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


def _extract_employee_id(req: func.HttpRequest) -> Dict[str, Any]:
    raw_body = req.get_body() or b""
    content_type = (req.headers.get("content-type") or "").lower()

    parsed_type = "unknown"
    data: Any = None

    if "application/json" in content_type:
        try:
            data = req.get_json()
            parsed_type = "json"
        except Exception:
            parsed_type = "json_parse_failed"
            data = None

    if data is None:
        try:
            body_text = raw_body.decode("utf-8", errors="replace")
        except Exception:
            body_text = ""
        data = _try_parse_json(body_text)
        if data is not None:
            parsed_type = "json_from_text"

    if data is None:
        try:
            form = req.form
            if form:
                data = dict(form)
                parsed_type = "form"
        except Exception:
            pass

    employee_id = None
    if isinstance(data, dict):
        candidates = [
            "employee_id",
            "employeeId",
            "EmployeeId",
            "EmployeeID",
            "Employee",
            "employee",
            "Id",
            "id",
        ]
        for k in candidates:
            v = data.get(k)
            if v:
                employee_id = str(v).strip()
                break

    return {
        "parsed_type": parsed_type,
        "employee_id": employee_id,
        "raw_len": len(raw_body),
        "data_keys": list(data.keys()) if isinstance(data, dict) else None,
    }


def _make_peoplehr_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
    }


def _peoplehr_get_employee_detail(employee_id: str) -> Dict[str, Any]:
    api_key = (os.environ.get("PEOPLEHR_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("Missing env var PEOPLEHR_API_KEY")

    url = "https://api.peoplehr.net/Employee/GetEmployeeDetailById"
    payload = {
        "APIKey": api_key,
        "Action": "GetEmployeeDetailById",
        "EmployeeId": employee_id,
    }

    safe_payload = dict(payload)
    safe_payload["APIKey"] = _mask(safe_payload["APIKey"], 4, 4)

    headers = _make_peoplehr_headers()

    logging.info(f"[{VERSION}] PeopleHR request url={url}")
    logging.info(f"[{VERSION}] PeopleHR request headers={json.dumps(headers)}")
    logging.info(f"[{VERSION}] PeopleHR request body={json.dumps(safe_payload)}")

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)

    body_text = r.text or ""
    content_encoding = (r.headers.get("Content-Encoding") or "").lower()
    content_type = (r.headers.get("Content-Type") or "").lower()

    logging.info(
        f"[{VERSION}] PeopleHR response status={r.status_code} content-type={content_type} content-encoding={content_encoding}"
    )
    logging.info(f"[{VERSION}] PeopleHR response snippet={body_text[:800]}")

    return {
        "http_status": r.status_code,
        "headers": dict(r.headers),
        "body_text": body_text,
    }


def _get_display_value(result_obj: Any, field_name: str) -> str:
    if not isinstance(result_obj, dict):
        return ""
    field_obj = result_obj.get(field_name)
    if not isinstance(field_obj, dict):
        return ""
    v = field_obj.get("DisplayValue")
    if v is None:
        return ""
    return str(v)


def _load_role_mapping() -> Dict[str, str]:
    raw = _require_env("FAMLY_ROLE_MAPPING_JSON")
    obj = _try_parse_json(raw)
    if not isinstance(obj, dict):
        raise ValueError("FAMLY_ROLE_MAPPING_JSON must be a JSON object mapping PeopleHR JobRole -> Famly roleId")

    out: Dict[str, str] = {}
    for k, v in obj.items():
        if k is None or v is None:
            continue
        ks = str(k).strip()
        vs = str(v).strip()
        if ks and vs:
            out[ks] = vs

    if not out:
        raise ValueError("FAMLY_ROLE_MAPPING_JSON parsed but produced an empty mapping")
    return out


def _make_famly_headers(access_token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-famly-accesstoken": access_token,
    }


def _famly_graphql_post(endpoint: str, access_token: str, body: Dict[str, Any]) -> Dict[str, Any]:
    headers = _make_famly_headers(access_token)
    r = requests.post(endpoint, headers=headers, data=json.dumps(body), timeout=30)
    body_text = r.text or ""

    logging.info(f"[{VERSION}] Famly response status={r.status_code}")
    logging.info(f"[{VERSION}] Famly response snippet={body_text[:800]}")

    return {
        "http_status": r.status_code,
        "body_text": body_text,
        "headers": dict(r.headers),
    }


def main(req: func.HttpRequest) -> func.HttpResponse:
    run_id = str(uuid.uuid4())
    started = _utc_now_iso()

    logging.info(f"[{VERSION}] run_id={run_id} started={started} method={req.method} path={req.url}")

    content_type = req.headers.get("content-type")
    logging.info(f"[{VERSION}] run_id={run_id} content-type={content_type}")

    parsed = _extract_employee_id(req)
    employee_id = parsed["employee_id"]

    logging.info(
        f"[{VERSION}] run_id={run_id} parsed_type={parsed['parsed_type']} raw_len={parsed['raw_len']} data_keys={parsed['data_keys']}"
    )
    logging.info(f"[{VERSION}] run_id={run_id} employee_id={employee_id}")

    if not employee_id:
        resp = {
            "status": "ok",
            "version": VERSION,
            "mode": "no_employee_id",
            "run_id": run_id,
            "employee_id": None,
            "note": "No employee_id found in payload. Nothing sent to PeopleHR. Nothing sent to Famly.",
        }
        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    try:
        famly_endpoint = _require_env("FAMLY_API_ENDPOINT")
        famly_access_token = _require_env("FAMLY_ACCESS_TOKEN")
        famly_institution_id = _require_env("FAMLY_INSTITUTION_ID")
        famly_group_id = _require_env("FAMLY_GROUP_ID_TEST_ROOM")
        role_mapping = _load_role_mapping()

        peoplehr = _peoplehr_get_employee_detail(employee_id)
        peoplehr_status = peoplehr["http_status"]
        body_text = peoplehr["body_text"]

        parsed_body = _try_parse_json(body_text) if body_text else None
        result_obj = parsed_body.get("Result") if isinstance(parsed_body, dict) else None

        first_name = _get_display_value(result_obj, "FirstName")
        last_name = _get_display_value(result_obj, "LastName")
        full_name = f"{first_name} {last_name}".strip()

        email = _get_display_value(result_obj, "EmailId")
        if not email:
            email = f"{employee_id.lower()}@placeholder.local"

        first_day = _get_display_value(result_obj, "StartDate")

        job_role = _get_display_value(result_obj, "JobRole")
        mapped_role_id = role_mapping.get(job_role, "")

        logging.info(f"[{VERSION}] run_id={run_id} job_role={job_role} mapped_role_id={mapped_role_id}")

        if not mapped_role_id:
            resp = {
                "status": "error",
                "version": VERSION,
                "run_id": run_id,
                "employee_id": employee_id,
                "peoplehr_http_status": peoplehr_status,
                "error": f"Unmapped PeopleHR JobRole -> Famly roleId: '{job_role}'",
            }
            return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

        famly_employee_input = {
            "name": full_name,
            "institutionId": famly_institution_id,
            "groupId": famly_group_id,
            "roleId": mapped_role_id,
            "email": email,
            "firstDay": first_day if first_day else None,
            "employeeWorkDayHours": 7.5,
            "employeeWorkDayMin": 450,
        }

        famly_request_body = {
            "query": FAMLY_CREATE_EMPLOYEES_MUTATION,
            "variables": {"createEmployees": [famly_employee_input]},
        }

        logging.info(f"[{VERSION}] run_id={run_id} famly_endpoint={famly_endpoint}")
        logging.info(f"[{VERSION}] run_id={run_id} famly_request_body={json.dumps(famly_request_body)}")

        famly = _famly_graphql_post(famly_endpoint, famly_access_token, famly_request_body)
        famly_status = famly["http_status"]
        famly_body_text = famly["body_text"]

        famly_body_json = _try_parse_json(famly_body_text)
        created_id = None
        if isinstance(famly_body_json, dict):
            try:
                created_id = famly_body_json["data"]["employees"]["create"][0]["id"]
            except Exception:
                created_id = None

        resp = {
            "status": "ok" if (peoplehr_status == 200 and famly_status == 200) else "error",
            "version": VERSION,
            "run_id": run_id,
            "employee_id": employee_id,
            "peoplehr_http_status": peoplehr_status,
            "famly_http_status": famly_status,
            "famly_employee_id": created_id,
            "famly_response": famly_body_json if famly_body_json is not None else famly_body_text,
            "captured_at": _utc_now_iso(),
        }

        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception(f"[{VERSION}] run_id={run_id} failed: {repr(e)}")
        resp = {
            "status": "error",
            "version": VERSION,
            "run_id": run_id,
            "employee_id": employee_id,
            "error": str(e),
        }
        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")
