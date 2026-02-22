import os
from pathlib import Path

import msal
import requests
from dotenv import load_dotenv

import time
import random
import requests


# Constants
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH = "https://graph.microsoft.com/v1.0"


def get_graph_headers(env_path: Path | None = None) -> dict:
    """
    Loads env vars and returns Authorization headers for Microsoft Graph.
    """
    if env_path is None:
        project_root = Path(__file__).resolve().parents[2]
        env_path = project_root / ".env"

    load_dotenv(dotenv_path=env_path, override=False)

    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    missing = [k for k, v in {
        "TENANT_ID": tenant_id,
        "CLIENT_ID": client_id,
        "CLIENT_SECRET": client_secret,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    authority = f"https://login.microsoftonline.com/{tenant_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )

    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Token error: {result}")

    return {"Authorization": f"Bearer {result['access_token']}"}


def graph_get(
    url: str,
    headers: dict,
    params: dict | None = None,
    timeout: int = 90,
    max_retries: int = 5,
) -> dict:
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)

        # --- THROTTLING ---
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else (2 ** attempt)
            wait += random.uniform(0, 0.5)  # jitter

            print(
                f"[429] Throttled. Retry in {wait:.2f}s "
                f"(attempt {attempt + 1}/{max_retries})"
            )

            time.sleep(wait)
            continue

        # --- TRANSIENT SERVER ERRORS ---
        if r.status_code in (500, 502, 503, 504):
            wait = (2 ** attempt) + random.uniform(0, 0.5)
            print(
                f"[{r.status_code}] Server error. Retry in {wait:.2f}s "
                f"(attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(wait)
            continue

        # --- SUCCESS OR FATAL ERROR ---
        r.raise_for_status()
        return r.json()

    # If we exhausted retries
    r.raise_for_status()


def graph_get_all(url: str, headers: dict, params: dict | None = None) -> list:
    items = []
    data = graph_get(url, headers, params=params)

    items.extend(data.get("value", []))
    next_link = data.get("@odata.nextLink")

    while next_link:
        data = graph_get(next_link, headers)
        items.extend(data.get("value", []))
        next_link = data.get("@odata.nextLink")

    return items

print("config Complete")