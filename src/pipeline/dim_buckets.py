import pandas as pd
import requests
from pathlib import Path

from pipeline.graph import GRAPH, get_graph_headers, graph_get_all


SHAREPOINT_ROOT = Path(r"C:\Users\criss\TP Caterers\TCP BI - Documents\Data\planner_data_pipeline")

STAGING_FACT_TASKS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Fact_Tasks.csv"

PROD_DIM_BUCKETS_PATH = SHAREPOINT_ROOT / "data" / "prod" / "Dim_Buckets.csv"
STAGING_DIM_BUCKETS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Dim_Buckets.csv"


def pull_buckets(headers: dict) -> pd.DataFrame:
    # -----------------------------
    # Get Planner Buckets
    # -----------------------------
    df_tasks = pd.read_csv(STAGING_FACT_TASKS_PATH, dtype={"PlanID": "string"})
    plan_ids = df_tasks["PlanID"].dropna().unique()

    print(f"Unique Plans: {len(plan_ids)}")

    print("Pulling Buckets...")

    bucket_rows = []

    for plan_id in plan_ids:
        try:
            buckets = graph_get_all(
                f"{GRAPH}/planner/plans/{plan_id}/buckets",
                headers=headers
            )
        except requests.HTTPError as e:
            print("Failed buckets for plan:", plan_id, str(e))
            continue

        for b in buckets:
            bucket_rows.append({
                "PlanID": plan_id,
                "BucketID": b.get("id"),
                "BucketName": b.get("name"),
                "BucketOrderHint": b.get("orderHint"),
            })

    df_buckets = pd.DataFrame(bucket_rows)
    print("Buckets pulled:", len(df_buckets))

    return df_buckets


def update_prod_dim_buckets(bucket_key: pd.DataFrame) -> pd.DataFrame:
    # -----------------------------
    # Check for new buckets & Update Dim Buckets in Prod
    # -----------------------------
    print("Updating Dim Buckets in Prod...")
    df_buckets_current = pd.read_csv(
        PROD_DIM_BUCKETS_PATH,
        dtype={
            "BucketName": "string",
            "BucketKey": "Int64",
            "bucket_order": "Int64",
        },
    )

    bucket_key["BucketName"] = bucket_key["BucketName"].astype("string")

    new_buckets = bucket_key[
        ~bucket_key["BucketName"].isin(df_buckets_current["BucketName"])
    ].copy()

    print("New buckets to append:", len(new_buckets))

    if len(new_buckets) > 0:
        current_max_key = pd.to_numeric(df_buckets_current["BucketKey"], errors="coerce").max()
        current_max_key = int(current_max_key) if pd.notna(current_max_key) else 0

        new_buckets["BucketKey"] = range(
            current_max_key + 1,
            current_max_key + 1 + len(new_buckets),
        )

    df_buckets_updated = pd.concat([df_buckets_current, new_buckets], ignore_index=True)

    dim_buckets_out = df_buckets_updated[["BucketName", "BucketKey", "bucket_order"]].drop_duplicates()

    dim_buckets_out.to_csv(PROD_DIM_BUCKETS_PATH, index=False)
    print("Dim Bucket in Prod Updated")

    return df_buckets_updated


def update_staging_dim_buckets(dim_buckets: pd.DataFrame, df_buckets_updated: pd.DataFrame) -> None:
    # -----------------------------
    # Update staging -- to update key in fact tasks
    # -----------------------------
    print("Updating staging Dim Buckets...")

    new_buckets = dim_buckets.merge(df_buckets_updated, on="BucketName", how="left")

    new_buckets["BucketID"] = new_buckets["BucketID"].astype("string")
    new_buckets["BucketName"] = new_buckets["BucketName"].astype("string")
    new_buckets["BucketKey"] = new_buckets["BucketKey"].astype("Int64")

    dim_buckets_staging = pd.read_csv(
        STAGING_DIM_BUCKETS_PATH,
        dtype={
            "BucketID": "string",
            "BucketKey": "Int64",
        },
    )

    bucket_ids_to_replace = new_buckets["BucketID"].dropna().unique()

    dim_bucket_staging_filtered = dim_buckets_staging[
        ~dim_buckets_staging["BucketID"].isin(bucket_ids_to_replace)
    ].copy()

    # Your original print was reversed; this prints what was removed/kept more clearly
    print("Rows kept from existing staging:", len(dim_bucket_staging_filtered))
    print("Rows replacing from this run:", len(new_buckets))

    dim_bucket_staging_updated = pd.concat(
        [dim_bucket_staging_filtered, new_buckets[["BucketID", "BucketKey"]]],
        ignore_index=True,
    )

    dim_bucket_staging_updated.to_csv(STAGING_DIM_BUCKETS_PATH, index=False)
    print(f"Updated Dim Buckets in staging")


def main():
    headers = get_graph_headers()

    df_buckets = pull_buckets(headers)

    dim_buckets = df_buckets[["BucketID", "BucketName"]].copy()

    bucket_key = (
        dim_buckets[["BucketName"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    bucket_key["BucketName"] = bucket_key["BucketName"].astype("string")

    df_buckets_updated = update_prod_dim_buckets(bucket_key)

    update_staging_dim_buckets(dim_buckets, df_buckets_updated)


if __name__ == "__main__":
    main()
