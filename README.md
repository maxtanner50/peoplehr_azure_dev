# PeopleHR Azure Function (v1 folder-per-function template)

Deploy this folder as an Azure Functions app (Python).

Function:
- peoplehr_webhook  ->  /api/peoplehr_webhook

Smoke test:
- GET returns 200 with version JSON.
- POST echoes content-type, parsed JSON (if any), and raw body.
