import pandas as pd
import requests
from pathlib import Path
import datetime as dt


from pipeline.graph import GRAPH, get_graph_headers, graph_get_all, graph_get

SHAREPOINT_ROOT = Path(r"C:\Users\criss\TP Caterers\TCP BI - Documents\Data\planner_data_pipeline")

DATA_DIR = SHAREPOINT_ROOT / "data/prod"
DIM_EVENTS_PATH = DATA_DIR / "Dim_Events.csv"


STAGING_DIR = SHAREPOINT_ROOT / "data/staging"
STAGING_FACT_TASKS_PATH = STAGING_DIR / "Fact_Tasks.csv"



def extract_completed_by_user_id(x):
    if isinstance(x, dict):
        user = x.get("user")
        if isinstance(user, dict):
            return user.get("id")
    return None


def get_user(user_id: str, headers: dict) -> dict:
    url = f"{GRAPH}/users/{user_id}"
    params = {
        "$select": "id,displayName,mail,userPrincipalName,jobTitle,department,accountEnabled"
    }
    return graph_get(url, headers=headers, params=params)


def get_tasks():
    # -----------------------------
    # Get planner tasks
    # -----------------------------
    headers = get_graph_headers()

    # Read Dim_Events (relative path as you wrote it)
    df_tasks_read = pd.read_csv(DIM_EVENTS_PATH, 
        dtype={
            "EventIdDateKey": "string",
            "EventId": "string",
            "EventName": "string",
            "GroupID": "string",
            "GroupDescription": "string",
            "GroupName": "string",
            "GroupType": "string",
            "GroupVisibility": "string",
            "PlanTitle": "string",
            "PlanID": "string",
            "ContainerType": "string",
            "ArchivedPlan": "string",
            "CurrentRecordIndicator": "int",
        },
    )

    df_tasks_read["EventDate"] = pd.to_datetime(df_tasks_read["EventDate"])
    df_tasks_recent = df_tasks_read[df_tasks_read["EventDate"] >= (pd.Timestamp.today().normalize() - pd.Timedelta(days=31))] 

    
    plan_ids = df_tasks_recent["PlanID"].dropna().unique()
    
    print(f"Getting Planner Tasks for {len(plan_ids)} plans..")

    task_rows = []

    for plan_id in plan_ids:
        try:
            tasks = graph_get_all(f"{GRAPH}/planner/plans/{plan_id}/tasks", headers=headers)
        except requests.HTTPError as e:
            print("Failed tasks for plan:", plan_id, str(e))
            continue

        for t in tasks:
            task_rows.append(t)

    df_tasks = pd.DataFrame(task_rows)
    print("Total tasks pulled:", len(df_tasks))

    df_tasks.drop(
        columns=["@odata.etag", "referenceCount", "conversationThreadId", "createdBy"],
        inplace=True,
    )

    df_tasks = df_tasks.rename(
        columns={
            "id": "TaskId",
            "planId": "PlanID",
            "bucketId": "BucketID",
            "title": "TaskTitle",
            "orderHint": "TaskOrderHint",
            "percentComplete": "TaskPercentComplete",
            "startDateTime": "TaskStartDateTime",
            "createdDateTime": "TaskCreatedDateTime",
            "dueDateTime": "TaskDueDateTime",
            "hasDescription": "TaskHasDescription",
            "previewType": "PreviewType",
            "completedDateTime": "TaskCompletedDateTime",
            "checklistItemCount": "TaskChecklistItemCount",
            "activeChecklistItemCount": "TaskActiveChecklistItemCount",
            "priority": "TaskPriority",
            "assignments": "TaskAssignments",
        }
    )

    # -----------------------------
    #Assignees
    # -----------------------------
    df_tasks['AssignedUserId'] = df_tasks['TaskAssignments'].apply(
    lambda x: next(iter(x)) if isinstance(x, dict) and x else None
    )

    task_assignee_userid = df_tasks["AssignedUserId"].dropna().unique()
    print("Unique AssignedUserId:", len(task_assignee_userid))

    task_assignee_userid_rows = []

    for i, uid in enumerate(task_assignee_userid, start=1):
        try:
            u = get_user(uid, headers)
            task_assignee_userid_rows.append(u)
        except requests.HTTPError as e:
            print(f"[{i}] Failed user: {uid} {e}")
            task_assignee_userid_rows.append({"id": uid})
            continue


    print(f"[{i}] user processed")

    
    task_assignee_user = (pd.DataFrame(task_assignee_userid_rows)
    [["id", "displayName", "userPrincipalName"]])

    task_assignee_user.rename(
        columns={
            "id": "AssignedUserId",
            "displayName": "TaskAssignee",
            "userPrincipalName": "TaskAssigneeEmail",
        },
        inplace=True,)

    df_tasks = df_tasks.merge(task_assignee_user, on='AssignedUserId', how='left', validate='m:1')


    # -----------------------------
    #Completed By
    # -----------------------------
    df_tasks["CompletedByUserId"] = df_tasks["completedBy"].apply(extract_completed_by_user_id)

    df_tasks["TaskCompleted"] = (df_tasks["TaskPercentComplete"].fillna(0).eq(100).map({True: "Yes", False: "No"})
    )

    date_cols = [
        "TaskCreatedDateTime",
        "TaskStartDateTime",
        "TaskDueDateTime",
        "TaskCompletedDateTime",
    ]

    df_tasks[date_cols] = df_tasks[date_cols].apply(
        lambda s: pd.to_datetime(s, errors="coerce").dt.date
    )

    return df_tasks, headers




def add_task_descriptions(df_tasks: pd.DataFrame, headers: dict) -> pd.DataFrame:
    # -----------------------------
    # Get planner Description
    # -----------------------------
    print("Getting Task Details...")
    df_tasks_with_description = df_tasks[df_tasks["TaskHasDescription"] == True].copy()
    

    task_ids = df_tasks_with_description["TaskId"].dropna().unique()
    print(f"Tasks with description {len(task_ids)}")
    
    task_detail_rows = []
    
    for task_id in (task_ids):
        try:
            details = graph_get(
                f"{GRAPH}/planner/tasks/{task_id}/details",
                headers=headers,
            )
        except requests.HTTPError as e:
            print(f"Failed task details for {task_id}: {e}")
            continue

        task_detail_rows.append({**details, "TaskId": task_id})


    print("Task Details Extracted:", len(task_detail_rows))

    task_details_rows = []
    # checklist_rows = []  # kept (unused) exactly like your snippet

    for d in task_detail_rows:
        task_id = d.get("TaskId")
        # checklist = d.get("checklist") or {}  # kept (unused) exactly like your snippet

        task_details_rows.append({
            "TaskId": task_id,
            "Description": d.get("description"),
        })

    TaskDetails = pd.DataFrame(task_details_rows)

    df_tasks = df_tasks.merge(TaskDetails, on="TaskId", how="left")


    
    return df_tasks


def main():
    df_tasks, headers = get_tasks()
    df_tasks_staging = add_task_descriptions(df_tasks, headers)
    
    schema = {
        # Identifiers / keys
        "PlanID": "string",
        "BucketID": "string",
        "TaskId": "string",
        # "AssignedUserId": "string",
        "CompletedByUserId": "string",

        # Text fields
        "TaskTitle": "string",
        "Description": "string",
        "TaskOrderHint": "string",
        "TaskAssignee": "string",
        "TaskAssigneeEmail": "string",
        "completedBy": "string",

        # Categorical / semi-structured
        "PreviewType": "string",
        "appliedCategories": "string",
        "TaskAssignments": "string",

        # Numeric
        "TaskChecklistItemCount": "Int64",
        "TaskActiveChecklistItemCount": "Int64",
        "TaskPriority": "Int64",
        "assigneePriority": "string",

        # Booleans
        "TaskHasDescription": "string",
        "TaskCompleted": "string",

        # Dates
        "TaskStartDateTime": "datetime64[ns]",
        "TaskCreatedDateTime": "datetime64[ns]",
        "TaskDueDateTime": "datetime64[ns]",
        "TaskCompletedDateTime": "datetime64[ns]"
        }
    
    for col, dtype in schema.items():
        if dtype.startswith("datetime"):
            df_tasks_staging[col] = pd.to_datetime(df_tasks_staging[col])
        else:
            df_tasks_staging[col] = df_tasks_staging[col].astype(dtype)

    df_tasks_staging.to_csv(STAGING_FACT_TASKS_PATH, index= False)

if __name__ == "__main__":
    main()