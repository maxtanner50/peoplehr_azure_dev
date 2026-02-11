import json
import os
import logging
import datetime
import uuid
from typing import Any, Dict, Optional

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings


VERSION = "VERSION_2026_02_06_peoplehr_v4_flat_fields_json_only"


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


def _blob_client() -> BlobServiceClient:
    conn_str = (os.environ.get("PEOPLEHR_STORAGE_CONNECTION_STRING") or "").strip()
    if not conn_str:
        raise ValueError("Missing env var PEOPLEHR_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_str)


def _upload_to_blob(
    container_name: str,
    blob_name: str,
    content: bytes,
    content_type: str,
    content_encoding: Optional[str] = None,
) -> None:
    svc = _blob_client()
    container = svc.get_container_client(container_name)
    blob = container.get_blob_client(blob_name)

    blob.upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(
            content_type=content_type,
            content_encoding=content_encoding,
        ),
    )


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
            "note": "No employee_id found in payload. Nothing sent to PeopleHR. Nothing stored.",
        }
        return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")

    container_name = (os.environ.get("PEOPLEHR_STORAGE_CONTAINER") or "peoplehrinitial").strip()
    prefix = (os.environ.get("PEOPLEHR_STORAGE_PREFIX") or "peoplehr_raw").strip()

    now = _utc_now()
    base_name = f"{prefix}/{now:%Y/%m/%d}/{employee_id}/{now:%H%M%S}_{run_id}"
    blob_name_json = f"{base_name}.json"

    logging.info(
        f"[{VERSION}] run_id={run_id} storage_container={container_name} blob_name_json={blob_name_json}"
    )

    try:
        peoplehr = _peoplehr_get_employee_detail(employee_id)
        status = peoplehr["http_status"]
        body_text = peoplehr["body_text"]

        parsed_body = _try_parse_json(body_text) if body_text else None
        result_obj = parsed_body.get("Result") if isinstance(parsed_body, dict) else None

        employee_fields = {
            "fullName": (
                f"{_get_display_value(result_obj, 'FirstName')} "
                f"{_get_display_value(result_obj, 'LastName')}".strip()
            ),
            "birthDate": _get_display_value(result_obj, "DateOfBirth"),
            "email": _get_display_value(result_obj, "EmailId"),
            "firstDay": _get_display_value(result_obj, "StartDate"),
            "title": _get_display_value(result_obj, "Gender"),
            "roleId": _get_display_value(result_obj, "JobRole"),
            "siteId": _get_display_value(result_obj, "Department"),
        }

        out_obj: Dict[str, Any] = {
            "status": "ok" if status == 200 else "error",
            "version": VERSION,
            "run_id": run_id,
            "employee_id": employee_id,
            "peoplehr_http_status": status,
            "employee": employee_fields,
            "captured_at": _utc_now_iso(),
        }

        raw_json_pretty = json.dumps(out_obj, indent=2, ensure_ascii=False).encode("utf-8")

        logging.info(
            f"[{VERSION}] run_id={run_id} uploading blob bytes_json={len(raw_json_pretty)}"
        )

        _upload_to_blob(
            container_name=container_name,
            blob_name=blob_name_json,
            content=raw_json_pretty,
            content_type="application/json",
            content_encoding=None,
        )

        logging.info(
            f"[{VERSION}] run_id={run_id} blob_upload_success json={blob_name_json}"
        )

        resp = {
            "status": out_obj["status"],
            "version": VERSION,
            "run_id": run_id,
            "employee_id": employee_id,
            "peoplehr_http_status": status,
            "blob_container": container_name,
            "blob_name_json": blob_name_json,
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
