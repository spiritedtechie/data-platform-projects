import os
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ARTIFACT_DIR = Path(os.getenv("ML_ARTIFACT_DIR", "version_2/ml/artifacts"))
SCORES_DIR = Path(os.getenv("ML_SCORES_DIR", "version_2/ml/scores"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")

ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
SCORES_DIR.mkdir(parents=True, exist_ok=True)


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}'")
    return con


def load_df(con: duckdb.DuckDBPyConnection, table_path: str) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM iceberg_scan('{table_path}')").df()


def safe_auc(y_true, y_pred_proba) -> float:
    if len(np.unique(y_true)) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_pred_proba))


def train_ml1_disruption_classifier(con: duckdb.DuckDBPyConnection):
    df = load_df(con, "s3://lake/warehouse/gold/ml_features_line_hour")
    df = df.dropna(subset=["disruption_next_1h_label"]).copy()
    if df.empty:
        return

    features = [
        "line_id",
        "mode",
        "hour_of_day",
        "day_of_week",
        "disruption_seconds",
        "good_service_pct",
        "state_change_count",
        "severity_weighted_seconds",
        "lag_1h_disruption_seconds",
        "lag_2h_disruption_seconds",
        "lag_24h_disruption_seconds",
        "rolling_6h_disruption_seconds",
        "rolling_24h_disruption_seconds",
    ]

    X = df[features]
    y = df["disruption_next_1h_label"].astype(int)

    numeric = [c for c in features if c not in ("line_id", "mode")]
    categorical = ["line_id", "mode"]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("scale", StandardScaler())]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )

    model = Pipeline(
        steps=[
            ("pre", pre),
            ("clf", LogisticRegression(max_iter=400, n_jobs=1)),
        ]
    )

    model.fit(X, y)
    proba = model.predict_proba(X)[:, 1]
    auc = safe_auc(y, proba)

    out = df[["bucket_hour", "line_id", "mode"]].copy()
    out["disruption_next_1h_probability"] = proba
    out["target"] = y

    out.to_parquet(SCORES_DIR / "ml_scores_line_hour.parquet", index=False)
    joblib.dump(model, ARTIFACT_DIR / "ml1_disruption_classifier.joblib")

    with open(ARTIFACT_DIR / "ml1_metrics.txt", "w", encoding="utf-8") as f:
        f.write(f"auc={auc}\nrows={len(df)}\n")


def train_ml2_downtime_forecast(con: duckdb.DuckDBPyConnection):
    df = load_df(con, "s3://lake/warehouse/gold/ml_features_line_day")
    df = df.dropna(subset=["downtime_next_day_minutes_label"]).copy()
    if df.empty:
        return

    features = [
        "line_id",
        "mode",
        "day_of_week",
        "downtime_minutes",
        "good_service_pct",
        "num_state_changes",
        "severity_weighted_seconds",
        "lag_1d_downtime_minutes",
        "lag_7d_downtime_minutes",
        "rolling_7d_downtime_minutes",
    ]

    X = df[features]
    y = df["downtime_next_day_minutes_label"].astype(float)

    numeric = [c for c in features if c not in ("line_id", "mode")]
    categorical = ["line_id", "mode"]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("scale", StandardScaler())]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )

    model = Pipeline(
        steps=[
            ("pre", pre),
            ("reg", HistGradientBoostingRegressor(random_state=42)),
        ]
    )

    model.fit(X, y)
    pred = model.predict(X)
    mae = float(mean_absolute_error(y, pred))

    out = df[["service_date", "line_id", "mode"]].copy()
    out["predicted_downtime_next_day_minutes"] = pred
    out["target"] = y

    out.to_parquet(SCORES_DIR / "ml_forecast_line_day.parquet", index=False)
    joblib.dump(model, ARTIFACT_DIR / "ml2_downtime_forecaster.joblib")

    with open(ARTIFACT_DIR / "ml2_metrics.txt", "w", encoding="utf-8") as f:
        f.write(f"mae={mae}\nrows={len(df)}\n")


def train_ml3_incident_duration(con: duckdb.DuckDBPyConnection):
    df = load_df(con, "s3://lake/warehouse/gold/ml_features_incident_start")
    df = df.dropna(subset=["incident_duration_seconds_label"]).copy()
    if df.empty:
        return

    features = [
        "line_id",
        "mode",
        "status_severity",
        "hour_of_day",
        "day_of_week",
        "recent_24h_incident_count",
        "recent_24h_mean_incident_seconds",
        "recent_24h_state_changes",
    ]

    X = df[features]
    y = df["incident_duration_seconds_label"].astype(float)

    numeric = [c for c in features if c not in ("line_id", "mode")]
    categorical = ["line_id", "mode"]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("scale", StandardScaler())]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )

    model = Pipeline(
        steps=[
            ("pre", pre),
            ("reg", HistGradientBoostingRegressor(random_state=42)),
        ]
    )

    model.fit(X, y)
    pred = model.predict(X)
    mae = float(mean_absolute_error(y, pred))

    out = df[["line_id", "mode", "incident_start_ts", "status_severity"]].copy()
    out["predicted_incident_duration_seconds"] = pred
    out["target"] = y

    out.to_parquet(SCORES_DIR / "ml_predicted_incident_duration.parquet", index=False)
    joblib.dump(model, ARTIFACT_DIR / "ml3_incident_duration.joblib")

    with open(ARTIFACT_DIR / "ml3_metrics.txt", "w", encoding="utf-8") as f:
        f.write(f"mae={mae}\nrows={len(df)}\n")


def build_ml4_anomalies(con: duckdb.DuckDBPyConnection):
    df = load_df(con, "s3://lake/warehouse/gold/ml_anomalies_line_hour")
    if df.empty:
        return

    alert_df = df[df["is_anomaly"] == True].copy()  # noqa: E712
    alert_df.to_parquet(SCORES_DIR / "ml_alerts_line_hour.parquet", index=False)

    with open(ARTIFACT_DIR / "ml4_metrics.txt", "w", encoding="utf-8") as f:
        f.write(f"anomaly_rows={len(alert_df)}\nrows={len(df)}\n")


if __name__ == "__main__":
    con = connect()
    train_ml1_disruption_classifier(con)
    train_ml2_downtime_forecast(con)
    train_ml3_incident_duration(con)
    build_ml4_anomalies(con)
    con.close()
