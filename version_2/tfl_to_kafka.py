# The purpose of this script is to fetch Transport for London (TfL) line status data
# and severity metadata from the TfL API and publish it to specified Kafka topics.
# It is designed to be run as a single micro-batch process, fetching the latest data
# and sending it to Kafka for further processing downstream. It pulls a snapshot of the
# current line statuses and severity metadata, encapsulates them in structured envelopes,
# and ensures reliable delivery to Kafka with retries and logging.

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx
from confluent_kafka import Producer
from tenacity import retry, stop_after_attempt, wait_exponential_jitter


# ============================================================
# Constants
# ============================================================
TFL_BASE_URL = "https://api.tfl.gov.uk"
TFL_LINE_STATUS_ENDPOINT = "/Line/Mode/{modes}/Status"
TFL_SEVERITY_META_ENDPOINT = "/Line/Meta/Severity"

DEFAULT_MODES = "tube,dlr,overground,elizabeth-line"

TOPIC_RAW_LINE_STATUS_DEFAULT = "tfl.raw.line_status"
TOPIC_RAW_SEVERITY_META_DEFAULT = "tfl.raw.severity_meta"


# ============================================================
# Exceptions
# ============================================================
class TfLIngestError(RuntimeError):
    """Raised when TfL → Kafka micro-batch ingestion fails."""


# ============================================================
# Logging
# ============================================================
def setup_logger(name: str = "tfl_microbatch") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)

    h = logging.StreamHandler()
    h.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s")
    )
    logger.addHandler(h)
    logger.propagate = False
    return logger


# ============================================================
# Settings
# ============================================================
@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    tfl_app_key: Optional[str]
    modes: str

    topic_raw_line_status: str
    topic_raw_severity_meta: str

    http_timeout_s: float
    fetch_severity: bool  # include severity in this run

    @staticmethod
    def from_env_and_args(args: argparse.Namespace) -> "Settings":
        kafka_bootstrap = (
            args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        ).strip()
        if not kafka_bootstrap:
            raise TfLIngestError(
                "Kafka bootstrap is empty. Set --kafka-bootstrap or KAFKA_BOOTSTRAP."
            )

        tfl_app_key = os.environ.get("TFL_APP_KEY")
        if not tfl_app_key:
            raise TfLIngestError("TfL app key is empty. Set TFL_APP_KEY.")

        modes = (args.modes or os.environ.get("TFL_MODES", DEFAULT_MODES)).strip()
        if not modes:
            raise TfLIngestError("TfL modes is empty. Set --modes or TFL_MODES.")

        topic_raw_line_status = (
            args.topic_raw_line_status
            or os.environ.get("TOPIC_RAW_LINE_STATUS", TOPIC_RAW_LINE_STATUS_DEFAULT)
        ).strip()
        topic_raw_severity_meta = (
            args.topic_raw_severity_meta
            or os.environ.get(
                "TOPIC_RAW_SEVERITY_META", TOPIC_RAW_SEVERITY_META_DEFAULT
            )
        ).strip()

        http_timeout_s = float(
            args.http_timeout_s or os.environ.get("TFL_HTTP_TIMEOUT_S", "15.0")
        )
        fetch_severity = (
            args.fetch_severity
            if args.fetch_severity is not None
            else (
                os.environ.get("FETCH_SEVERITY", "false").strip().lower()
                in ("1", "true", "yes", "y", "on")
            )
        )

        return Settings(
            kafka_bootstrap=kafka_bootstrap,
            tfl_app_key=tfl_app_key,
            modes=modes,
            topic_raw_line_status=topic_raw_line_status,
            topic_raw_severity_meta=topic_raw_severity_meta,
            http_timeout_s=http_timeout_s,
            fetch_severity=fetch_severity,
        )


# ============================================================
# Utilities
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_of_obj(obj: Any) -> str:
    return hashlib.sha256(stable_json_dumps(obj).encode("utf-8")).hexdigest()


def build_params(app_key: Optional[str]) -> Dict[str, Any]:
    return {"app_key": app_key} if app_key else {}


# ============================================================
# HTTP
# ============================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=10))
def http_get_json(
    url: str,
    params: Dict[str, Any],
    timeout_s: float,
) -> Tuple[Any, int, int, Dict[str, str]]:
    t0 = time.time()
    with httpx.Client(timeout=timeout_s) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    duration_ms = int((time.time() - t0) * 1000)
    headers = {
        "date": resp.headers.get("date", ""),
        "etag": resp.headers.get("etag", ""),
        "cache-control": resp.headers.get("cache-control", ""),
    }
    return data, resp.status_code, duration_ms, headers


# ============================================================
# Kafka
# ============================================================
def make_producer(settings: Settings) -> Producer:
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 25,
        "compression.type": "snappy",
        "message.timeout.ms": 30000,
        "socket.keepalive.enable": True,
    }
    return Producer(conf)


def publish_envelope(
    producer: Producer, topic: str, key: str, envelope: Dict[str, Any]
) -> None:
    value_bytes = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    producer.produce(topic=topic, key=key.encode("utf-8"), value=value_bytes)


# ============================================================
# Fetchers (return raw envelopes)
# ============================================================
def fetch_line_status(settings: Settings) -> Dict[str, Any]:
    url = f"{TFL_BASE_URL}{TFL_LINE_STATUS_ENDPOINT.format(modes=settings.modes)}"
    data, status, duration_ms, headers = http_get_json(
        url=url,
        params=build_params(settings.tfl_app_key),
        timeout_s=settings.http_timeout_s,
    )
    return {
        "source": "tfl",
        "endpoint": TFL_LINE_STATUS_ENDPOINT,
        "modes": settings.modes,
        "request_id": str(uuid.uuid4()),
        "ingested_at": utc_now_iso(),
        "http_status": status,
        "duration_ms": duration_ms,
        "response_headers": headers,
        "payload_hash": sha256_of_obj(data),
        "payload": data,
        "schema_version": 1,
    }


def fetch_severity_meta(settings: Settings) -> Dict[str, Any]:
    url = f"{TFL_BASE_URL}{TFL_SEVERITY_META_ENDPOINT}"
    data, status, duration_ms, headers = http_get_json(
        url=url,
        params=build_params(settings.tfl_app_key),
        timeout_s=settings.http_timeout_s,
    )
    return {
        "source": "tfl",
        "endpoint": TFL_SEVERITY_META_ENDPOINT,
        "request_id": str(uuid.uuid4()),
        "ingested_at": utc_now_iso(),
        "http_status": status,
        "duration_ms": duration_ms,
        "response_headers": headers,
        "payload_hash": sha256_of_obj(data),
        "payload": data,
        "schema_version": 1,
    }


# ============================================================
# Main single-run
# ============================================================
def run_once(settings: Settings, logger: logging.Logger) -> Dict[str, Any]:
    producer = make_producer(settings)

    sent = 0

    # 1) line status
    env = fetch_line_status(settings)
    key = f"line_status|{settings.modes}"

    publish_envelope(producer, settings.topic_raw_line_status, key, env)
    sent += 1
    logger.info(
        "Sent line status | topic=%s key=%s lines=%d",
        settings.topic_raw_line_status,
        key,
        len(env["payload"]),
    )

    producer.poll(0)

    # 2) severity (optional)
    if settings.fetch_severity:
        env = fetch_severity_meta(settings)
        key = "severity_meta"

        publish_envelope(producer, settings.topic_raw_severity_meta, key, env)
        sent += 1
        logger.info(
            "Sent severity meta | topic=%s rows=%d",
            settings.topic_raw_severity_meta,
            len(env["payload"]),
        )
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining != 0:
        raise TfLIngestError(
            f"Kafka flush timed out; {remaining} message(s) not delivered"
        )

    summary = {
        "sent_messages": sent,
        "modes": settings.modes,
        "topics": {
            "raw_line_status": settings.topic_raw_line_status,
            "raw_severity_meta": settings.topic_raw_severity_meta,
        },
        "fetch_severity": settings.fetch_severity,
        "ingested_at": utc_now_iso(),
    }
    logger.info("Run complete | %s", summary)
    return summary


# ============================================================
# CLI
# ============================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="TfL → Kafka raw ingestion (single fetch per endpoint)."
    )
    p.add_argument("--kafka-bootstrap", default=None)
    p.add_argument("--modes", default=None)

    p.add_argument("--topic-raw-line-status", default=None)
    p.add_argument("--topic-raw-severity-meta", default=None)

    p.add_argument(
        "--fetch-severity", action=argparse.BooleanOptionalAction, default=None
    )

    p.add_argument("--http-timeout-s", type=float, default=None)
    return p.parse_args()


def main() -> None:
    logger = setup_logger()
    args = parse_args()
    settings = Settings.from_env_and_args(args)
    summary = run_once(settings, logger)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
