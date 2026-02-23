import ast
from pathlib import Path

import pandas as pd
import requests

from pipeline.graph import GRAPH, get_graph_headers, graph_get


SHAREPOINT_ROOT = Path(r"C:\Users\criss\TP Caterers\TCP BI - Documents\Data\planner_data_pipeline")

STAGING_FACT_TASKS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Fact_Tasks.csv"
PROD_DIM_LABELS_PATH = SHAREPOINT_ROOT / "data" / "prod" / "Dim_Labels.csv"
STAGING_DIM_LABELS_PATH = SHAREPOINT_ROOT / "data" / "staging" / "Dim_Labels.csv"
print("Pullinf Labels...")

def build_dim_label_out(df_tasks: pd.DataFrame, headers: dict) -> pd.DataFrame:
    # -----------------------------
    # Pull plan details - Labels
    # -----------------------------
    plan_id_lengths = df_tasks["PlanID"].astype("string").str.len()
    bad_plan_ids = df_tasks.loc[plan_id_lengths < 25, ["PlanID"]]
    if not bad_plan_ids.empty:
        print("WARNING: Found PlanID values with length < 30 (showing up to 20):")
        print(bad_plan_ids.drop_duplicates().head(20).to_string(index=False))

    labels = df_tasks[["PlanID", "TaskId", "appliedCategories"]].copy()
    labels = labels.dropna(subset=["appliedCategories"])

    # appliedCategories is stored as a string in CSV; convert back to dict
    labels["appliedCategories"] = labels["appliedCategories"].apply(ast.literal_eval)

    labels["Category"] = labels["appliedCategories"].apply(
        lambda d: [k for k, v in d.items() if v is True] if isinstance(d, dict) else []
    )

    labels = labels.explode("Category").reset_index(drop=True)
    print("Label rows (after explode):", len(labels))

    plan_ids = df_tasks["PlanID"].dropna().unique()
    print("Unique plan_ids:", len(plan_ids))

    plan_details_rows = []
    for plan_id in plan_ids:
        try:
            details = graph_get(f"{GRAPH}/planner/plans/{plan_id}/details", headers=headers)
        except requests.HTTPError as e:
            print("Failed plan details for:", plan_id, str(e))
            continue

        plan_details_rows.append({**details, "PlanID": plan_id})

    print("Plan details pulled:", len(plan_details_rows))

    # -----------------------------
    # Flatten + keep only categoryDescriptions (Label names)
    # -----------------------------
    clean_plan_details_rows = []
    for d in plan_details_rows:
        d2 = dict(d)
        d2.pop("sharedWith", None)
        clean_plan_details_rows.append(d2)

    df_plan_details_raw = pd.json_normalize(clean_plan_details_rows)

    df_plan_labels = df_plan_details_raw.copy()
    df_plan_labels = df_plan_labels.dropna(axis=1, how="all")

    label_name_cols = [c for c in df_plan_labels.columns if c != "TaskId" and c != "PlanID"]
    if label_name_cols:
        df_plan_labels = df_plan_labels.dropna(subset=label_name_cols, how="all")

    id_cols = ["PlanID"]
    cat_cols = [c for c in df_plan_labels.columns if c.startswith("categoryDescriptions.")]

    dim_label = df_plan_labels.melt(
        id_vars=id_cols,
        value_vars=cat_cols,
        var_name="Category",
        value_name="Label",
    )

    dim_label["Category"] = dim_label["Category"].str.replace("categoryDescriptions.", "", regex=False)

    dim_label["Label"] = dim_label["Label"].astype("string").str.strip()
    dim_label = dim_label[dim_label["Label"].notna() & (dim_label["Label"] != "")].copy()

    dim_label_out = labels.merge(dim_label, on=["PlanID", "Category"], how="left")[["TaskId", "Label"]]
    dim_label_out = dim_label_out[~dim_label_out["Label"].isna()].copy()

    return dim_label_out


def update_prod_dim_labels(dim_label_out: pd.DataFrame) -> pd.DataFrame:
    # -----------------------------
    # Update records (remove+replace by TaskId)
    # -----------------------------
    dim_label_current = pd.read_csv(
        PROD_DIM_LABELS_PATH,
        dtype={"TaskId": "string", "Label": "string"},
    )

    task_ids_to_replace = dim_label_out["TaskId"].dropna().unique()
    dim_label_current_filtered = dim_label_current[
        ~dim_label_current["TaskId"].isin(task_ids_to_replace)
    ].copy()

    final_df = pd.concat([dim_label_current_filtered, dim_label_out], ignore_index=True)

    final_df["TaskId"] = final_df["TaskId"].astype("string")
    final_df["Label"] = final_df["Label"].astype("string")

    final_df.to_csv(PROD_DIM_LABELS_PATH, index=False)
    print(f"Wrote prod: {PROD_DIM_LABELS_PATH}")

    return final_df


def write_task_label_concat(final_df: pd.DataFrame) -> None:
    # -----------------------------
    # Group by TaskID - join to fact tasks later
    # -----------------------------
    df_task_labels_concat = (
        final_df
        .groupby("TaskId", as_index=False)
        .agg({"Label": lambda s: ", ".join(sorted(set(s.dropna())))})
    )

    df_task_labels_concat.to_csv(STAGING_DIM_LABELS_PATH, index=False)
    print(f"Wrote staging: {STAGING_DIM_LABELS_PATH}")


def main():
    headers = get_graph_headers()

    df_tasks = pd.read_csv(STAGING_FACT_TASKS_PATH, dtype={"PlanID": "string", "TaskId": "string"})
    dim_label_out = build_dim_label_out(df_tasks, headers)

    final_df = update_prod_dim_labels(dim_label_out)
    write_task_label_concat(final_df)
    print("Labels updated")


if __name__ == "__main__":
    main()
