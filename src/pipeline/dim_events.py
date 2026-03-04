import datetime as dt
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

import numpy as np
import pandas as pd
import requests

from pipeline.graph import GRAPH, get_graph_headers, graph_get_all, get_sharepoint_root


# -----------------------------
# Path helpers (works from any folder)
# -----------------------------
# SHAREPOINT_ROOT = Path(r"C:\Users\criss\TP Caterers\TCP BI - Documents\Data\planner_data_pipeline\data")

# DATA_DIR = SHAREPOINT_ROOT / "prod"
# DIM_EVENTS_PATH = DATA_DIR / "Dim_Events.csv"

SHAREPOINT_ROOT = get_sharepoint_root()

print(SHAREPOINT_ROOT)
DATA_DIR = SHAREPOINT_ROOT / "prod"
DIM_EVENTS_PATH = DATA_DIR / "Dim_Events.csv"


# -----------------------------
# Transform helpers
# -----------------------------
def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date

def parse_event_date(series: pd.Series) -> pd.Series:
    raw = series.astype("string").str.strip()
    d1 = pd.to_datetime(raw, format="%m-%d-%y", errors="coerce")
    d2 = pd.to_datetime(raw, format="%m-%d-%Y", errors="coerce")
    return d1.fillna(d2).dt.date


# -----------------------------
# Config
# -----------------------------
GROUP_FIELDS = [
    "id",
    "createdDateTime",
    "renewedDateTime",
    "description",
    "displayName",
    "groupTypes",
    "visibility",
]

CUTOFF_DATE = dt.date(2024, 1, 1)
EVENT_PATTERN = r"^E\d{5}\b"
EXCLUDE_GROUP_IDS = {
    "d010be59-9dc2-44ef-b7f2-4436a4e319dd"  # Tasting Team Planner Format Planner Group
}


SCHEMA = {
    "GroupID": "string",
    "EventId": "string",
    "PlanID": "string",

    "GroupName": "string",
    "GroupDescription": "string",
    "EventName": "string",
    "PlanTitle": "string",
    "GroupType": "string",
    "GroupVisibility": "string",
    "ContainerType": "string",

    # store as dates then format at write-time
    "GroupCreatedDate": "datetime",
    "GroupRenewedDate": "datetime",
    "EventDate": "datetime",
    "PlanCreatedDate": "datetime",

    "ArchivedPlan": "string",
    "rn": "int64",
    "CurrentRecordIndicator": "int32",
}




# -----------------------------
# Extract + Build
# -----------------------------
def build_dim_events(headers: dict) -> pd.DataFrame:
    # 1) Pull groups
    
    print("Extracting Groups")
    params = {"$select": ",".join(GROUP_FIELDS), "$top": 999}
    all_groups = graph_get_all(f"{GRAPH}/groups", headers=headers, params=params)
    df_groups = pd.DataFrame(all_groups)

    if df_groups.empty:
        return df_groups

    # 2) Normalize / clean
    df_groups = df_groups.rename(columns={
        "id": "GroupID",
        "displayName": "GroupName",
        "createdDateTime": "GroupCreatedDate",
        "renewedDateTime": "GroupRenewedDate",
        "description": "GroupDescription",
        "groupTypes": "GroupType",
        "visibility": "GroupVisibility",
    })

    for col in ["GroupCreatedDate", "GroupRenewedDate"]:
        if col in df_groups.columns:
            df_groups[col] = to_date(df_groups[col])

    # groupTypes list -> string
    if "GroupType" in df_groups.columns:
        df_groups["GroupType"] = df_groups["GroupType"].apply(
            lambda x: ",".join(x) if isinstance(x, list) else x
        )

    # Filter created from 2024+
    if "GroupCreatedDate" in df_groups.columns:
        df_groups = df_groups[df_groups["GroupCreatedDate"] >= CUTOFF_DATE].copy()

    # 3) Keep only event groups
    df_groups_events = df_groups[
        df_groups["GroupName"].astype("string").str.match(EVENT_PATTERN, na=False)
    ].copy()

    if df_groups_events.empty:
        return df_groups_events

    # 4) Split GroupName => EventId / EventDate / EventName
    parts = df_groups_events["GroupName"].astype("string").str.split(" - ", n=2, expand=True)

    df_groups_events["EventId"] = parts[0]
    df_groups_events["EventDate"] = parse_event_date(parts[1])
    df_groups_events["EventName"] = parts[2]

    # QA: unparseable dates (kept for debugging if needed)
    bad_dates = df_groups_events[df_groups_events["EventDate"].isna()][["GroupName"]]
    if not bad_dates.empty:
        print("WARNING: Unparseable EventDate for these GroupName values (showing up to 10):")
        print(bad_dates.head(10).to_string(index=False))

    # Remove test events
    df_groups_events = df_groups_events[
        ~df_groups_events["EventName"].astype("string").str.contains("Testing", na=False)
    ].copy()

    # Exclude specific group(s)
    df_groups_events = df_groups_events[~df_groups_events["GroupID"].isin(EXCLUDE_GROUP_IDS)].copy()

    # Filter to last 31 days forward window (your logic)
    df_groups_events = df_groups_events[
        df_groups_events["EventDate"] > (dt.date.today() - dt.timedelta(days=31))
    ].sort_values("EventDate")


    print(f"Group Extracted: {len(df_groups_events)}")


    # -----------------------------
    # Planners: Pull plan metadata for each group
    # -----------------------------
    
    print("Extracting Planners")
    plan_rows = []

    for group_id in df_groups_events["GroupID"][:50]:
        try:
            plans = graph_get_all(f"{GRAPH}/groups/{group_id}/planner/plans", headers=headers)
        except requests.HTTPError as e:
            print("Failed plans for group:", group_id, str(e))
            continue

        for p in plans:
            plan_rows.append({**p, "GroupID": group_id})

    df_plan_meta_raw = pd.json_normalize(plan_rows)

    print(f"Plans Extracted: {len(df_plan_meta_raw)}")

    if df_plan_meta_raw.empty:
        # still return groups with no plans? your current logic inner-joins plans,
        # so returning empty makes it explicit
        return pd.DataFrame()

    col_map = {
        "GroupID": "GroupID",
        "createdDateTime": "PlanCreatedDate",
        "title": "PlanTitle",
        "id": "PlanID",
        "container.type": "ContainerType",
    }

    for raw_col in col_map.keys():
        if raw_col not in df_plan_meta_raw.columns:
            df_plan_meta_raw[raw_col] = None

    df_plan_meta = df_plan_meta_raw[list(col_map.keys())].rename(columns=col_map)

    df_plan_meta["PlanCreatedDate"] = pd.to_datetime(
        df_plan_meta["PlanCreatedDate"], errors="coerce"
    ).dt.date

    df_plan_meta["ArchivedPlan"] = df_plan_meta["PlanTitle"].str.startswith("Archived", na=False).map(
        {True: "Y", False: "N"}
    )


    # -----------------------------
    # Merge group + plan info
    # -----------------------------
    df_event = df_groups_events.merge(df_plan_meta, on="GroupID", how="inner")

    # Keep newest record per EventId
    df_event = df_event.sort_values(["EventId", "GroupCreatedDate"], ascending=[True, False])
    df_event["rn"] = df_event.groupby("EventId").cumcount() + 1
    df_event["CurrentRecordIndicator"] = np.where(df_event["rn"] == 1, 1, 0)
    df_event = df_event[df_event["CurrentRecordIndicator"] == 1].copy()

    # Types
    for col, dtype in SCHEMA.items():
        if col not in df_event.columns:
            continue
        if dtype == "string":
            df_event[col] = df_event[col].astype("string")
        elif dtype == "datetime":
            df_event[col] = pd.to_datetime(df_event[col], errors="coerce")

    # Key for dataverse
    df_event["EventIdDateKey"] = (
        df_event["EventId"].astype("string")
        + "_"
        + df_event["PlanCreatedDate"].dt.strftime("%Y%m%d")
    )

    return df_event


# -----------------------------
# Upsert into Dim_Events.csv
# -----------------------------
def upsert_dim_events(df_new: pd.DataFrame) -> pd.DataFrame:
    print("Updating Dim Events")
    if df_new.empty:
        raise RuntimeError("No new event data produced (df_new is empty).")

    if not DIM_EVENTS_PATH.exists():
        # first run: just write it
        final_df = df_new.copy()
    else:
        df_current = pd.read_csv(DIM_EVENTS_PATH)

        if not df_current["EventIdDateKey"].is_unique:
            raise ValueError("Existing Dim_Events.csv has duplicate EventIdDateKey values.")

        if not df_new["EventIdDateKey"].is_unique:
            raise ValueError("New data has duplicate EventIdDateKey values.")

        df_current = df_current.set_index("EventIdDateKey")
        df_new = df_new.set_index("EventIdDateKey")

        print(df_current.dtypes[df_current.dtypes.astype(str).str.contains("string|str")])
        # update existing
        df_current.update(df_new)

        # append new keys
        new_rows = df_new.loc[~df_new.index.isin(df_current.index)]
        final_df = pd.concat([df_current, new_rows])
        
        print(f"New Planners Added: {len(new_rows)}")

        final_df = final_df.reset_index()

    # enforce schema + format dates for CSV
    for col, dtype in SCHEMA.items():
        if col not in final_df.columns:
            continue

        if dtype == "string":
            final_df[col] = final_df[col].astype("string")

        elif dtype == "datetime":
            final_df[col] = pd.to_datetime(final_df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # final uniqueness check
    if not final_df["EventIdDateKey"].is_unique:
        dups = final_df[final_df["EventIdDateKey"].duplicated(keep=False)].sort_values("EventIdDateKey")
        print("EventIdDateKey is NOT unique. Duplicate keys:")
        print(dups[["EventIdDateKey", "EventId", "PlanCreatedDate", "PlanID"]].head(50).to_string(index=False))
        raise ValueError("Duplicate EventIdDateKey values found after upsert")

    # write only required columns (your list)
    cols_out = [
        "EventIdDateKey",
        "EventId",
        "EventDate",
        "EventName",
        "GroupID",
        "GroupCreatedDate",
        "GroupRenewedDate",
        "GroupDescription",
        "GroupName",
        "GroupType",
        "GroupVisibility",
        "PlanCreatedDate",
        "PlanTitle",
        "PlanID",
        "ContainerType",
        "ArchivedPlan",
        "CurrentRecordIndicator",
    ]

    # keep only columns that exist (prevents crash if Graph returns missing fields)
    cols_out = [c for c in cols_out if c in final_df.columns]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    final_df[cols_out].to_csv(DIM_EVENTS_PATH, index=False)
    print(f"Wrote: {DIM_EVENTS_PATH}")

    return final_df


def main():
    headers = get_graph_headers()  # env path handled in graph.py using project root
    df_new = build_dim_events(headers)
    _final = upsert_dim_events(df_new)
    print("Done.")


if __name__ == "__main__":
    main()
