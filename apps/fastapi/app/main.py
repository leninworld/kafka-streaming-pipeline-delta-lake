import csv
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest
from starlette.responses import Response


app = FastAPI(title="Model Serving API", version="0.2.0")

# Keep notebook artifacts under the shared notebooks volume so they are easy to inspect.
NOTEBOOKS_DIR = Path("/opt/spark/notebooks")
ARTIFACTS_DIR = NOTEBOOKS_DIR / "artifacts"
REPORTS_DIR = NOTEBOOKS_DIR / "reports"
MLRUNS_DIR = NOTEBOOKS_DIR / "experiments" / "mlruns"
REGISTERED_MODEL_NAME = "telco_customer_churn_lr"
REGISTERED_MODEL_DIR = MLRUNS_DIR / "models" / REGISTERED_MODEL_NAME
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SCORED_BATCHES_CSV = ARTIFACTS_DIR / "fastapi_scored_batches.csv"
WHYLOGS_PROFILE_PATH = "fastapi_predictions_latest.bin"
DRIFT_REPORT_PATH = REPORTS_DIR / "fastapi_evidently_report.html"

requests_total = Counter("fastapi_requests_total", "Total FastAPI requests", ["path"])
predict_requests_total = Counter(
    "fastapi_predict_requests_total",
    "Total prediction requests handled by the inference API",
)
predictions_total = Counter(
    "fastapi_predictions_total",
    "Total number of prediction rows processed by the inference API",
)
predict_latency_seconds = Histogram(
    "fastapi_predict_latency_seconds",
    "How long prediction requests take in seconds",
    buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)


class TelcoRecord(BaseModel):
    gender: str
    SeniorCitizen: int
    Partner: str
    Dependents: str
    tenure: int
    PhoneService: str
    MultipleLines: str
    InternetService: str
    OnlineSecurity: str
    OnlineBackup: str
    DeviceProtection: str
    TechSupport: str
    StreamingTV: str
    StreamingMovies: str
    Contract: str
    PaperlessBilling: str
    PaymentMethod: str
    MonthlyCharges: float
    TotalCharges: float


class PredictionRequest(BaseModel):
    # Primary contract: {"instances": [{...telco fields...}, ...]}
    instances: List[TelcoRecord] = Field(
        ...,
        examples=[
            [
                {
                    "gender": "Female",
                    "SeniorCitizen": 0,
                    "Partner": "Yes",
                    "Dependents": "No",
                    "tenure": 12,
                    "PhoneService": "Yes",
                    "MultipleLines": "No",
                    "InternetService": "Fiber optic",
                    "OnlineSecurity": "No",
                    "OnlineBackup": "Yes",
                    "DeviceProtection": "No",
                    "TechSupport": "Yes",
                    "StreamingTV": "Yes",
                    "StreamingMovies": "Yes",
                    "Contract": "Two year",
                    "PaperlessBilling": "Yes",
                    "PaymentMethod": "Credit card (automatic)",
                    "MonthlyCharges": 75.95,
                    "TotalCharges": 4542.35,
                }
            ]
        ],
    )


class PredictionResponse(BaseModel):
    model_name: str
    model_version: str
    probability_for_class: str
    threshold: float
    predictions: List[Dict]


def _latest_version_meta_path() -> Path:
    if not REGISTERED_MODEL_DIR.exists():
        raise FileNotFoundError(
            f"Registered model directory not found: {REGISTERED_MODEL_DIR}. "
            "Run the telco notebook model registration step first."
        )

    version_meta_paths = []
    for p in REGISTERED_MODEL_DIR.glob("version-*"):
        try:
            int(p.name.split("-")[-1])
        except ValueError:
            continue
        meta_path = p / "meta.yaml"
        if meta_path.exists():
            version_meta_paths.append(meta_path)

    if not version_meta_paths:
        raise FileNotFoundError(
            f"No version metadata found under: {REGISTERED_MODEL_DIR}"
        )

    return sorted(
        version_meta_paths,
        key=lambda p: int(p.parent.name.split("-")[-1]),
    )[-1]


def _parse_simple_yaml_map(path: Path) -> dict:
    data = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip().strip("'").strip('"')
    return data


@lru_cache(maxsize=1)
def _load_churn_model():
    import mlflow.sklearn

    latest_meta_path = _latest_version_meta_path()
    meta = _parse_simple_yaml_map(latest_meta_path)
    source = meta.get("source")
    if not source:
        raise FileNotFoundError(f"`source` not found in {latest_meta_path}")

    if source.startswith("file://"):
        model_path = source.replace("file://", "", 1)
    else:
        model_path = source

    model = mlflow.sklearn.load_model(model_path)
    version = meta.get("version", "unknown")
    return model, version


def _probability_class_info(model) -> Tuple[int, str]:
    classes = list(getattr(model, "classes_", []))
    if not classes:
        return 1, "1"

    class_strings = [str(c).lower() for c in classes]
    if "yes" in class_strings:
        idx = class_strings.index("yes")
        return idx, str(classes[idx])
    if "1" in class_strings:
        idx = class_strings.index("1")
        return idx, str(classes[idx])

    idx = len(classes) - 1
    return idx, str(classes[idx])


def _predict_telco(records: List[TelcoRecord]) -> Tuple[List[Dict], str, str]:
    import pandas as pd

    model, version = _load_churn_model()
    input_df = pd.DataFrame([record.model_dump() for record in records])

    predicted_labels = model.predict(input_df)
    positive_idx, probability_for_class = _probability_class_info(model)

    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(input_df)[:, positive_idx]
    else:
        probabilities = [None] * len(predicted_labels)

    prediction_rows = []
    for i in range(len(input_df)):
        label = predicted_labels[i]
        prob = probabilities[i]
        prediction_rows.append(
            {
                "prediction_label": str(label),
                "churn_probability": None if prob is None else float(prob),
            }
        )

    return prediction_rows, version, probability_for_class


def _append_scored_rows(records: List[TelcoRecord], predictions: List[Dict]) -> None:
    row_dicts = []
    now = datetime.now(timezone.utc).isoformat()
    for record, prediction in zip(records, predictions):
        payload = record.model_dump()
        payload["timestamp_utc"] = now
        payload["prediction_label"] = prediction["prediction_label"]
        payload["churn_probability"] = prediction["churn_probability"]
        row_dicts.append(payload)

    if not row_dicts:
        return

    header = list(row_dicts[0].keys())
    file_exists = SCORED_BATCHES_CSV.exists()
    header_mismatch = False
    if file_exists:
        with SCORED_BATCHES_CSV.open("r", newline="") as handle:
            first_line = handle.readline().strip()
            if first_line:
                existing_header = first_line.split(",")
                header_mismatch = existing_header != header

    write_mode = "a"
    write_header = not file_exists
    if header_mismatch:
        # We changed API payload shape from demo math to telco records.
        # Reset the scored batch file to keep a consistent schema.
        write_mode = "w"
        write_header = True

    with SCORED_BATCHES_CSV.open(write_mode, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if write_header:
            writer.writeheader()
        for row in row_dicts:
            writer.writerow(row)


def _write_whylogs_profile(records: List[TelcoRecord], predictions: List[Dict]) -> None:
    import pandas as pd
    import whylogs as why
    from whylogs.api.writer.local import LocalWriter

    row_dicts = []
    for record, prediction in zip(records, predictions):
        payload = record.model_dump()
        payload["prediction_label"] = prediction["prediction_label"]
        payload["churn_probability"] = prediction["churn_probability"]
        row_dicts.append(payload)
    scored_df = pd.DataFrame(row_dicts)

    # WhyLogs creates a compact summary of what predictions looked like.
    profile = why.log(scored_df).profile()
    writer = LocalWriter(base_dir=str(ARTIFACTS_DIR), base_name=WHYLOGS_PROFILE_PATH)
    writer.write(profile)


def _generate_drift_report_from_scored_batches() -> dict:
    import pandas as pd
    from evidently import Report
    from evidently.presets import DataDriftPreset

    if not SCORED_BATCHES_CSV.exists():
        raise HTTPException(
            status_code=400,
            detail="No scored batch log exists yet. Call /predict first to create one.",
        )

    scored_df = pd.read_csv(SCORED_BATCHES_CSV)
    if len(scored_df) < 6:
        raise HTTPException(
            status_code=400,
            detail="Need at least 6 scored rows before generating a drift report.",
        )

    midpoint = len(scored_df) // 2
    candidate_numeric_columns = [
        "SeniorCitizen",
        "tenure",
        "MonthlyCharges",
        "TotalCharges",
        "churn_probability",
    ]
    numeric_columns = [c for c in candidate_numeric_columns if c in scored_df.columns]
    if not numeric_columns:
        raise HTTPException(
            status_code=400,
            detail=(
                "No numeric columns available for drift report. "
                "Call /predict first with telco payloads."
            ),
        )

    reference_data = scored_df.iloc[:midpoint][numeric_columns].copy()
    current_data = scored_df.iloc[midpoint:][numeric_columns].copy()

    # The first half acts like older baseline data; the second half acts like newer data.
    report = Report([DataDriftPreset()])
    snapshot = report.run(reference_data=reference_data, current_data=current_data)
    snapshot.save_html(str(DRIFT_REPORT_PATH))

    return {
        "status": "report_generated",
        "report_path": str(DRIFT_REPORT_PATH),
        "reference_rows": len(reference_data),
        "current_rows": len(current_data),
    }


@app.get("/health")
def health() -> dict:
    requests_total.labels(path="/health").inc()
    return {"status": "ok"}


@app.get("/")
def root() -> dict:
    requests_total.labels(path="/").inc()
    return {"message": "FastAPI churn-serving API is running"}


@app.post("/predict", response_model=PredictionResponse)
def predict(
    payload: Union[PredictionRequest, List[TelcoRecord], TelcoRecord]
) -> PredictionResponse:
    requests_total.labels(path="/predict").inc()
    predict_requests_total.inc()

    if isinstance(payload, PredictionRequest):
        instances = payload.instances
    elif isinstance(payload, list):
        instances = payload
    else:
        instances = [payload]

    if not instances:
        raise HTTPException(
            status_code=400,
            detail=(
                "No records provided. Send one telco object or "
                '{"instances": [ ... ]}.'
            ),
        )

    start_time = time.perf_counter()
    predictions, model_version, probability_for_class = _predict_telco(instances)
    predict_latency_seconds.observe(time.perf_counter() - start_time)
    predictions_total.inc(len(predictions))

    # Save scored data locally so monitoring tools have something real to inspect.
    _append_scored_rows(instances, predictions)
    _write_whylogs_profile(instances, predictions)

    return PredictionResponse(
        model_name=REGISTERED_MODEL_NAME,
        model_version=str(model_version),
        probability_for_class=probability_for_class,
        threshold=0.5,
        predictions=predictions,
    )


@app.post("/generate-drift-report")
async def generate_drift_report() -> dict:
    requests_total.labels(path="/generate-drift-report").inc()
    return _generate_drift_report_from_scored_batches()


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type="text/plain; version=0.0.4")
