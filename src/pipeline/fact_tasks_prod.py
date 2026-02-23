import datetime as dt
from pathlib import Path

import pandas as pd
import requests

from pipeline.graph import GRAPH, get_graph_headers, graph_get

SHAREPOINT_ROOT = Path(r"C:\Users\criss\TP Caterers\TCP BI - Documents\Data\planner_data_pipeline")


STAGING_FACT_TASKS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Fact_Tasks.csv"
STAGING_DIM_BUCKETS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Dim_Buckets.csv"
STAGING_DIM_LABELS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Dim_Labels.csv"

PROD_FACT_TASKS_PATH = SHAREPOINT_ROOT / "data" / "prod" / "Fact_Tasks.csv"


def get_user(user_id: str, headers: dict) -> dict:
    url = f"{GRAPH}/users/{user_id}"
    params = {
        "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled"
    }
    return graph_get(url, headers=headers, params=params)


def read_fact_tasks_staging() -> pd.DataFrame:
    schema = {
        "PlanID": "string",
        "BucketID": "string",
        "TaskId": "string",
        "AssignedUserId": "string",
        "CompletedByUserId": "string",
        "TaskTitle": "string",
        "Description": "string",
        "TaskOrderHint": "string",
        "TaskAssignee": "string",
        "TaskAssigneeEmail": "string",
        "completedBy": "string",
        "PreviewType": "string",
        "appliedCategories": "string",
        "TaskAssignments": "string",
        "TaskChecklistItemCount": "Int64",
        "TaskActiveChecklistItemCount": "Int64",
        "TaskPriority": "Int64",
        "assigneePriority": "string",
        "TaskHasDescription": "string",
        "TaskCompleted": "string",
        "TaskStartDateTime": "datetime64[ns]",
        "TaskCreatedDateTime": "datetime64[ns]",
        "TaskDueDateTime": "datetime64[ns]",
        "TaskCompletedDateTime": "datetime64[ns]",
    }

    date_cols = [k for k, v in schema.items() if v.startswith("datetime")]
    dtype_cols = {k: v for k, v in schema.items() if k not in date_cols}

    df = pd.read_csv(
        STAGING_FACT_TASKS_PATH,
        dtype=dtype_cols,
        parse_dates=date_cols,
        keep_default_na=True,
    )
    return df


def read_fact_tasks_prod_current() -> pd.DataFrame:
    df = pd.read_csv(
        PROD_FACT_TASKS_PATH,
        dtype={
            "TaskId": "string",
            "PlanID": "string",
            "TaskTitle": "string",
            "TaskDescription": "string",
            "TaskOrderHint": "string",
            "TaskAssignee": "string",
            "TaskAssigneeEmail": "string",
            "TaskCompletedByName": "string",
            "TaskCompletedByUserEmail": "string",
            "TaskCompleted": "string",
            "TaskPreviewType": "string",
            "Label": "string",
            "TaskHasDescription": "string",
            "TaskPriority": "Int64",
            "TaskChecklistItemCount": "Int64",
            "TaskActiveChecklistItemCount": "Int64",
            "BucketKey": "Int64",
            "LastRun": "string",
        },
    )

    # Parse datetime/date columns explicitly
    for col in ["TaskDueDateTime", "TaskStartDateTime", "TaskCompletedDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Keep your existing filter
    if "TaskId" in df.columns:
        df = df[df["TaskId"] != "#NAME?"].copy()

    return df


def main():
    headers = get_graph_headers()

    # -----------------------------
    # Read staging tasks
    # -----------------------------
    df_tasks_staging = read_fact_tasks_staging()

    # -----------------------------
    # Update Buckets
    # -----------------------------
    print("Updating Buckets")
    df_buckets_staging = pd.read_csv(
        STAGING_DIM_BUCKETS_PATH,
        dtype={"BucketID": "string", "BucketKey": "Int64"},
    )

    df_tasks_prod = df_tasks_staging.merge(df_buckets_staging, on="BucketID", how="left")
    print("Buckets Updated")

    # -----------------------------
    # Update Labels
    # -----------------------------
    print("Updating Labels")
    df_labels_staging = pd.read_csv(
        STAGING_DIM_LABELS_PATH,
        dtype={"TaskId": "string", "Label": "string"},
    )

    df_tasks_prod = df_tasks_prod.merge(df_labels_staging, on="TaskId", how="left")
    print("Labels Updated")

    # -----------------------------
    # add task completed by users
    # -----------------------------
    print("Updating completed by users")

    user_ids = df_tasks_prod["CompletedByUserId"].dropna().unique()

    dim_user_rows = []
    for uid in user_ids:
        try:
            u = get_user(uid, headers=headers)
            dim_user_rows.append(u)
        except requests.HTTPError as e:
            print("Failed user:", uid, str(e))
            dim_user_rows.append({"id": uid})

    DimUser = pd.DataFrame(dim_user_rows).rename(columns={"id": "UserID"})
    print("DimUser rows:", len(DimUser))

    DimUser = DimUser[["UserID", "displayName", "userPrincipalName"]].copy()

    DimUser.rename(
        columns={
            "displayName": "TaskCompletedByName",
            "userPrincipalName": "TaskCompletedByUserEmail",
        },
        inplace=True,
    )

    DimUser["TaskCompletedByName"] = DimUser["TaskCompletedByName"].astype("string")
    DimUser["TaskCompletedByUserEmail"] = DimUser["TaskCompletedByUserEmail"].astype("string")

    df_tasks_prod = df_tasks_prod.merge(
        DimUser,
        left_on="CompletedByUserId",
        right_on="UserID",
        how="left",
    )

    df_tasks_prod.rename(
        columns={
            "Description": "TaskDescription",
            "TaskCompletedDateTime": "TaskCompletedDate",
            "PreviewType": "TaskPreviewType",
        },
        inplace=True,
    )

    print("Completed by users Done")

    # -----------------------------
    # Shape output slice
    # -----------------------------
    df_tasks_prod["LastRun"] = dt.date.today()

    df_events_prod_last_month = df_tasks_prod[
        [
            "TaskId",
            "TaskTitle",
            "TaskDescription",
            "TaskHasDescription",
            "TaskOrderHint",
            "TaskAssignee",
            "TaskAssigneeEmail",
            "TaskDueDateTime",
            "TaskStartDateTime",
            "TaskCompletedDate",
            "TaskCompleted",
            "TaskCompletedByName",
            "TaskCompletedByUserEmail",
            "TaskPriority",
            "TaskPreviewType",
            "TaskChecklistItemCount",
            "TaskActiveChecklistItemCount",
            "Label",
            "BucketKey",
            "PlanID",
            "LastRun",
        ]
    ].copy()

    # -----------------------------
    # Read current prod
    # -----------------------------
    df_tasks_current = read_fact_tasks_prod_current()

    # -----------------------------
    # Upsert logic by TaskId
    # -----------------------------
    cur = df_tasks_current.set_index("TaskId")
    new = df_events_prod_last_month.set_index("TaskId")

    cur_filtered = cur.loc[~cur.index.isin(new.index)].copy()
    df_tasks_out = pd.concat([cur_filtered, new], axis=0).reset_index()

    print("Rows before:", len(df_tasks_current))
    print("Rows in last month:", len(df_events_prod_last_month))
    print("Rows after:", len(df_tasks_out))

    df_tasks_out.to_csv(PROD_FACT_TASKS_PATH, index=False)
    print(f"Wrote prod: {PROD_FACT_TASKS_PATH}")


if __name__ == "__main__":
    main()
