import datetime as dt
from pathlib import Path

import pandas as pd
import requests

from pipeline.graph import GRAPH, get_graph_headers, graph_get, get_sharepoint_root


SHAREPOINT_ROOT = get_sharepoint_root()


PROD_FACT_TASKS_PATH = SHAREPOINT_ROOT / "staging" / "Fact_Tasks.csv"
PROD_FACT_SUB_TASKS_PATH = SHAREPOINT_ROOT / "prod" / "Fact_Sub_Tasks.csv"


def get_user(user_id: str, headers: dict) -> dict:
    url = f"{GRAPH}/users/{user_id}"
    params = {"$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled"}
    return graph_get(url, headers=headers, params=params)


def pull_task_details(unique_task_ids, headers: dict) -> list[dict]:
    print("getting sub tasks...")
    task_detail_rows = []

    for task_id in unique_task_ids[:]:
        try:
            details = graph_get(
                f"{GRAPH}/planner/tasks/{task_id}/details",
                headers=headers,
            )
        except requests.HTTPError as e:
            print(f"Failed task details for {task_id}: {e}")
            continue

        task_detail_rows.append({**details, "TaskId": task_id})

    return task_detail_rows


def build_subtasks(task_detail_rows: list[dict]) -> pd.DataFrame:
    subtask_rows = []

    for d in task_detail_rows:
        # FIX: you were using TaskID/id inconsistently
        task_id = d.get("TaskId")

        checklist = d.get("checklist") or {}
        if not isinstance(checklist, dict) or len(checklist) == 0:
            continue

        for checklist_id, item in checklist.items():
            user = (item.get("lastModifiedBy") or {}).get("user") or {}

            subtask_rows.append(
                {
                    "TaskId": task_id,
                    "SubTaskID": str(checklist_id),
                    "SubTaskTitle": item.get("title"),
                    "IsChecked": item.get("isChecked"),
                    "OrderHint": item.get("orderHint"),
                    "LastModifiedDateTime": item.get("lastModifiedDateTime"),
                    "LastModifiedByUserId": user.get("id"),
                    "LastModifiedByUserName": user.get("displayName"),
                }
            )

    df_subtasks = pd.DataFrame(subtask_rows)

    if len(df_subtasks) == 0:
        return df_subtasks

    df_subtasks["LastModifiedDateTime"] = (
        pd.to_datetime(df_subtasks["LastModifiedDateTime"], errors="coerce").dt.date
    )

    df_subtasks.rename(columns={"OrderHint": "SubTaskOrder"}, inplace=True)
    df_subtasks.drop(columns=["LastModifiedByUserName"], inplace=True)

    print("Subtasks rows:", len(df_subtasks))
    return df_subtasks


def enrich_subtasks_with_user_details(df_subtasks: pd.DataFrame, headers: dict) -> pd.DataFrame:
    print("getting sub tasks user details...")

    if len(df_subtasks) == 0:
        return df_subtasks

    subtasks_userid = df_subtasks["LastModifiedByUserId"].dropna().unique()

    dim_user_rows = []
    for uid in subtasks_userid:
        try:
            u = get_user(uid, headers=headers)
            dim_user_rows.append(u)
        except requests.HTTPError as e:
            print("Failed user:", uid, str(e))
            dim_user_rows.append({"id": uid})

    sub_task_user = pd.DataFrame(dim_user_rows).rename(columns={"id": "UserID"})
    print("sub_task_user rows:", len(sub_task_user))

    sub_task_user = sub_task_user[["UserID", "displayName", "userPrincipalName"]].copy()
    sub_task_user.rename(
        columns={
            "displayName": "LastModifiedByName",
            "userPrincipalName": "LastModifiedByUserEmail",
        },
        inplace=True,
    )

    df_subtasks = df_subtasks.merge(
        sub_task_user,
        left_on="LastModifiedByUserId",
        right_on="UserID",
        how="left",
    )

    df_subtasks["IsChecked"] = df_subtasks["IsChecked"].map({True: "Y", False: "N"})

    # FIX: key should use TaskId consistently
    df_subtasks["SubTaskIDKey"] = df_subtasks["SubTaskID"].astype("string") + df_subtasks["TaskId"].astype("string")

    # Keep string casting (as you wrote it)
    string_cols = [
        "SubTaskIDKey",
        "SubTaskID",
        "SubTaskTitle",
        "TaskId",
        "IsChecked",
        "SubTaskOrder",
        "LastModifiedByUserId",
        "LastModifiedByName",
        "LastModifiedByUserEmail",
    ]
    for col in string_cols:
        if col in df_subtasks.columns:
            df_subtasks[col] = df_subtasks[col].astype("string")

    df_subtasks["LastModifiedDateTime"] = (
        pd.to_datetime(df_subtasks["LastModifiedDateTime"], errors="coerce").dt.date
    )

    return df_subtasks


def upsert_to_prod(df_subtasks_last_month: pd.DataFrame) -> None:
    # Keep your output column names for prod (TaskID etc.)
    df_subtasks_last_month_out = df_subtasks_last_month[
        [
            "SubTaskIDKey",
            "SubTaskID",
            "SubTaskTitle",
            "TaskId",
            "IsChecked",
            "SubTaskOrder",
            "LastModifiedDateTime",
            "LastModifiedByUserId",
            "LastModifiedByName",
            "LastModifiedByUserEmail",
        ]
    ].copy()

    # Rename TaskId -> TaskID to match your prod file schema
    df_subtasks_last_month_out.rename(columns={"TaskId": "TaskID"}, inplace=True)

    df_subtasks_current = pd.read_csv(
        PROD_FACT_SUB_TASKS_PATH,
        dtype={
            "SubTaskIDKey": "string",
            "SubTaskID": "string",
            "SubTaskTitle": "string",
            "TaskID": "string",
            "IsChecked": "string",
            "SubTaskOrder": "string",
            "LastModifiedByUserId": "string",
            "LastModifiedByName": "string",
            "LastModifiedByUserEmail": "string",
        },
    )
    print(f"fact sub tasks current length: {len(df_subtasks_current)}")

    # UPSERT LOGIC (your version)
    df_cur = df_subtasks_current.drop_duplicates("SubTaskIDKey", keep="last")
    df_new = df_subtasks_last_month_out.drop_duplicates("SubTaskIDKey", keep="last")

    cur_i = df_cur.set_index("SubTaskIDKey")
    new_i = df_new.set_index("SubTaskIDKey")


    cur_i.update(new_i)
    to_add = new_i.loc[new_i.index.difference(cur_i.index)]

    df_subtasks_updated = pd.concat([cur_i, to_add]).reset_index()

    if df_subtasks_updated["SubTaskIDKey"].is_unique:
        print("SubTaskIDKey is unique")
        df_subtasks_updated.to_csv(PROD_FACT_SUB_TASKS_PATH, index=False)
    print(f"fact sub tasks update length: {len(df_subtasks_current)}")
    print("Fact Sub Tasks Update Complete!")


def main():
    headers = get_graph_headers()

    df_tasks = pd.read_csv(PROD_FACT_TASKS_PATH, dtype={"TaskId": "string"})
    unique_task_ids = df_tasks["TaskId"].dropna().unique()

    print("Unique TaskId:", len(unique_task_ids))

    task_detail_rows = pull_task_details(unique_task_ids, headers=headers)
    df_subtasks = build_subtasks(task_detail_rows)
    df_subtasks = enrich_subtasks_with_user_details(df_subtasks, headers=headers)

    # If no subtasks pulled, avoid wiping/doing unnecessary work
    if df_subtasks is None or len(df_subtasks) == 0:
        print("No subtasks found; nothing to upsert.")
        return

    upsert_to_prod(df_subtasks)


if __name__ == "__main__":
    main()
