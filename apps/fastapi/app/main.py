import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest
from starlette.responses import Response


app = FastAPI(title="Model Serving API", version="0.2.0")

# Keep notebook artifacts under the shared notebooks volume so they are easy to inspect.
NOTEBOOKS_DIR = Path("/opt/spark/notebooks")
ARTIFACTS_DIR = NOTEBOOKS_DIR / "artifacts"
REPORTS_DIR = NOTEBOOKS_DIR / "reports"
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


class PredictionRequest(BaseModel):
    # A beginner-friendly request shape: just send a list of numbers.
    instances: List[float] = Field(..., examples=[[1.0, 2.5, 4.0]])


class PredictionResponse(BaseModel):
    model_name: str
    formula: str
    predictions: List[float]


def _predict(values: List[float]) -> List[float]:
    # This tiny demo model mirrors the TensorFlow Serving and TorchServe demos.
    return [(value * 2.0) + 1.0 for value in values]


def _append_scored_rows(inputs: List[float], predictions: List[float]) -> None:
    header = ["timestamp_utc", "input_value", "prediction"]
    now = datetime.now(timezone.utc).isoformat()

    with SCORED_BATCHES_CSV.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if handle.tell() == 0:
            writer.writerow(header)
        for input_value, prediction in zip(inputs, predictions):
            writer.writerow([now, input_value, prediction])


def _write_whylogs_profile(inputs: List[float], predictions: List[float]) -> None:
    import pandas as pd
    import whylogs as why
    from whylogs.api.writer.local import LocalWriter

    scored_df = pd.DataFrame(
        {
            "input_value": inputs,
            "prediction": predictions,
        }
    )

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
    reference_data = scored_df.iloc[:midpoint][["input_value", "prediction"]].copy()
    current_data = scored_df.iloc[midpoint:][["input_value", "prediction"]].copy()

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
    return {"message": "FastAPI model-serving starter is running"}


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest) -> PredictionResponse:
    requests_total.labels(path="/predict").inc()
    predict_requests_total.inc()

    if not payload.instances:
        raise HTTPException(status_code=400, detail="instances must contain at least one number")

    start_time = time.perf_counter()
    predictions = _predict(payload.instances)
    predict_latency_seconds.observe(time.perf_counter() - start_time)
    predictions_total.inc(len(predictions))

    # Save scored data locally so monitoring tools have something real to inspect.
    _append_scored_rows(payload.instances, predictions)
    _write_whylogs_profile(payload.instances, predictions)

    return PredictionResponse(
        model_name="demo_math_fastapi",
        formula="output = input * 2 + 1",
        predictions=predictions,
    )


@app.post("/generate-drift-report")
async def generate_drift_report() -> dict:
    requests_total.labels(path="/generate-drift-report").inc()
    return _generate_drift_report_from_scored_batches()


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type="text/plain; version=0.0.4")
