from fastapi import FastAPI
from prometheus_client import Counter, generate_latest
from starlette.responses import Response

app = FastAPI(title="Model Serving API", version="0.1.0")
requests_total = Counter("fastapi_requests_total", "Total FastAPI requests", ["path"])


@app.get("/health")
def health() -> dict:
    requests_total.labels(path="/health").inc()
    return {"status": "ok"}


@app.get("/")
def root() -> dict:
    requests_total.labels(path="/").inc()
    return {"message": "FastAPI model-serving starter is running"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type="text/plain; version=0.0.4")
