#!/usr/bin/env python3
"""NWS hourly observation ingest → MongoDB daily doc with rolling aggregates.

Can be imported by an Airflow DAG (via ``run_ingest``) or run standalone from
the command line (``python nws_ingest.py --date 2026-03-12``).
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    mongo_uri: str
    db_name: str
    collection_name: str
    user_agent: str
    station_id: str
    stn: int
    wban: int
    tz_name: str
    min_hourly_successes: int
    request_timeout_seconds: int
    per_call_sleep_seconds: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull one day of NWS hourly observations for KSFO, upsert one daily Mongo doc, "
            "and compute rolling weekly/monthly aggregate fields for that new doc."
        )
    )
    parser.add_argument(
        "--date",
        help="Target local date in YYYY-MM-DD. Defaults to local yesterday.",
        default=None,
    )
    return parser.parse_args()


def load_config() -> Config:
    """Build a ``Config`` from environment variables (for standalone CLI use)."""
    mongo_uri = os.getenv("MONGO_URI", "").strip()
    user_agent = os.getenv("NOAA_USER_AGENT", "").strip()

    if not mongo_uri:
        raise ValueError("Missing required env var: MONGO_URI")
    if not user_agent:
        raise ValueError("Missing required env var: NOAA_USER_AGENT")

    return Config(
        mongo_uri=mongo_uri,
        db_name=os.getenv("DB_NAME", "dds-group-project"),
        collection_name=os.getenv("COLLECTION_NAME", "big_query_SFO_weather_v5"),
        user_agent=user_agent,
        station_id=os.getenv("STATION_ID", "KSFO"),
        stn=int(os.getenv("STN", "724940")),
        wban=int(os.getenv("WBAN", "23234")),
        tz_name=os.getenv("STATION_TZ", "America/Los_Angeles"),
        min_hourly_successes=int(os.getenv("MIN_HOURLY_SUCCESSES", "18")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        per_call_sleep_seconds=float(os.getenv("PER_CALL_SLEEP_SECONDS", "0.05")),
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def resolve_target_date(arg_date: str | None, tz_name: str) -> date:
    station_tz = ZoneInfo(tz_name)
    if arg_date:
        return date.fromisoformat(arg_date)
    return datetime.now(station_tz).date() - timedelta(days=1)


# ---------------------------------------------------------------------------
# Unit conversions
# ---------------------------------------------------------------------------

def c_to_f(celsius_value: Any) -> float | None:
    if celsius_value is None:
        return None
    return (float(celsius_value) * 9.0 / 5.0) + 32.0


def mm_to_in(mm_value: Any) -> float | None:
    if mm_value is None:
        return None
    return float(mm_value) / 25.4


# ---------------------------------------------------------------------------
# NWS API interaction
# ---------------------------------------------------------------------------

def safe_get_json(session: requests.Session, url: str, timeout_seconds: int) -> dict[str, Any] | None:
    response = session.get(url, timeout=timeout_seconds)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def build_present_weather_blob(properties: dict[str, Any]) -> str:
    weather_tokens: list[str] = []
    for item in properties.get("presentWeather") or []:
        token = " ".join(str(item.get(k, "")) for k in ("intensity", "modifier", "weather")).lower().strip()
        if token:
            weather_tokens.append(token)
    text_description = (properties.get("textDescription") or "").lower()
    return " ".join(weather_tokens + [text_description]).strip()


def fetch_hourly_observations(config: Config, target_date: date) -> list[dict[str, Any]]:
    base_url = f"https://api.weather.gov/stations/{config.station_id}/observations"
    station_tz = ZoneInfo(config.tz_name)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": config.user_agent,
            "Accept": "application/geo+json",
        }
    )

    day_start_local = datetime(target_date.year, target_date.month, target_date.day, 0, 0, tzinfo=station_tz)
    rows: list[dict[str, Any]] = []

    for hour in range(24):
        ts_local = day_start_local + timedelta(hours=hour)
        ts_utc = ts_local.astimezone(ZoneInfo("UTC"))
        ts_utc_str = ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = safe_get_json(
            session=session,
            url=f"{base_url}/{ts_utc_str}",
            timeout_seconds=config.request_timeout_seconds,
        )
        if payload is None:
            time.sleep(config.per_call_sleep_seconds)
            continue

        props = payload.get("properties", {})
        weather_blob = build_present_weather_blob(props)

        row = {
            "temp": c_to_f((props.get("temperature") or {}).get("value")),
            "prcp": mm_to_in((props.get("precipitationLastHour") or {}).get("value")),
            "fog": int("fog" in weather_blob),
            "rain_drizzle": int(any(k in weather_blob for k in ("rain", "drizzle", "shower"))),
            "snow_ice_pellets": int(any(k in weather_blob for k in ("snow", "sleet", "ice pellets", "freezing"))),
            "hail": int("hail" in weather_blob),
            "thunder": int("thunder" in weather_blob),
            "tornado_funnel_cloud": int(any(k in weather_blob for k in ("tornado", "funnel"))),
        }
        rows.append(row)
        time.sleep(config.per_call_sleep_seconds)

    return rows


# ---------------------------------------------------------------------------
# Daily document building
# ---------------------------------------------------------------------------

def avg(values: list[float]) -> float:
    return float(mean(values))


def to_mdy_2digit_year(d: date) -> str:
    return f"{d.month}/{d.day}/{d.year % 100:02d}"


def build_daily_doc(config: Config, target_date: date, hourly_rows: list[dict[str, Any]]) -> dict[str, Any]:
    temps = [float(r["temp"]) for r in hourly_rows if r.get("temp") is not None]
    prcps = [float(r["prcp"]) for r in hourly_rows if r.get("prcp") is not None]

    if not temps:
        raise RuntimeError("No temperature values available in successful hourly calls.")

    return {
        "stn": config.stn,
        "wban": config.wban,
        "date": to_mdy_2digit_year(target_date),
        "year": target_date.year,
        "mo": target_date.month,
        "da": target_date.day,
        "temp": avg(temps),
        "max": max(temps),
        "min": min(temps),
        "prcp": float(sum(prcps)) if prcps else 0.0,
        "fog": int(max(int(r.get("fog", 0)) for r in hourly_rows)),
        "rain_drizzle": int(max(int(r.get("rain_drizzle", 0)) for r in hourly_rows)),
        "snow_ice_pellets": int(max(int(r.get("snow_ice_pellets", 0)) for r in hourly_rows)),
        "hail": int(max(int(r.get("hail", 0)) for r in hourly_rows)),
        "thunder": int(max(int(r.get("thunder", 0)) for r in hourly_rows)),
        "tornado_funnel_cloud": int(max(int(r.get("tornado_funnel_cloud", 0)) for r in hourly_rows)),
    }


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def day_key_filter(config: Config, target_date: date) -> dict[str, Any]:
    return {
        "stn": config.stn,
        "wban": config.wban,
        "year": target_date.year,
        "mo": target_date.month,
        "da": target_date.day,
    }


def upsert_daily_base_doc(coll: Any, day_key: dict[str, Any], daily_doc: dict[str, Any]) -> None:
    coll.update_one(day_key, {"$set": daily_doc}, upsert=True)


def build_docs_upto_target_filter(config: Config, target_date: date) -> dict[str, Any]:
    y, m, d = target_date.year, target_date.month, target_date.day
    return {
        "stn": config.stn,
        "wban": config.wban,
        "$or": [
            {"year": {"$lt": y}},
            {"year": y, "mo": {"$lt": m}},
            {"year": y, "mo": m, "da": {"$lte": d}},
        ],
    }


def numeric_value(doc: dict[str, Any], key: str) -> float:
    value = doc.get(key)
    if value is None:
        return 0.0
    return float(value)


def compute_aggregate_fields(target_date: date, trailing_desc: list[dict[str, Any]]) -> dict[str, Any]:
    if not trailing_desc:
        raise RuntimeError("No trailing documents found; cannot compute aggregate fields.")

    weekly_docs = trailing_desc[:7]
    monthly_docs = trailing_desc[:30]

    weekly_max_values = [numeric_value(d, "max") for d in weekly_docs]
    weekly_min_values = [numeric_value(d, "min") for d in weekly_docs]
    weekly_temp_values = [numeric_value(d, "temp") for d in weekly_docs]
    weekly_prcp_values = [numeric_value(d, "prcp") for d in weekly_docs]

    monthly_max_values = [numeric_value(d, "max") for d in monthly_docs]
    monthly_min_values = [numeric_value(d, "min") for d in monthly_docs]
    monthly_temp_values = [numeric_value(d, "temp") for d in monthly_docs]
    monthly_prcp_values = [numeric_value(d, "prcp") for d in monthly_docs]

    return {
        "day_of_the_year": target_date.timetuple().tm_yday,
        "month_of_the_year": target_date.month,
        "weekly_avg_max_temp": avg(weekly_max_values),
        "weekly_avg_min_temp": avg(weekly_min_values),
        "weekly_avg_temp": avg(weekly_temp_values),
        "weekly_max_max_temp": max(weekly_max_values),
        "weekly_min_min_temp": min(weekly_min_values),
        "weekly_sum_precip": float(sum(weekly_prcp_values)),
        "monthly_avg_max_temp": avg(monthly_max_values),
        "monthly_avg_min_temp": avg(monthly_min_values),
        "monthly_avg_temp": avg(monthly_temp_values),
        "monthly_max_max_temp": max(monthly_max_values),
        "monthly_min_min_temp": min(monthly_min_values),
        "monthly_sum_precip": float(sum(monthly_prcp_values)),
    }


def update_doc_aggregates(coll: Any, day_key: dict[str, Any], aggregate_fields: dict[str, Any]) -> None:
    coll.update_one(day_key, {"$set": aggregate_fields}, upsert=False)


# ---------------------------------------------------------------------------
# Core pipeline entry point (importable by the Airflow DAG)
# ---------------------------------------------------------------------------

def run_ingest(config: Config, target_date: date) -> dict:
    """Fetch hourly NWS data, upsert daily doc to MongoDB, compute aggregates.

    Returns the final document (without ``_id``) so the calling DAG can pass
    it downstream via XCom.
    """
    from pymongo import MongoClient

    hourly_rows = fetch_hourly_observations(config=config, target_date=target_date)
    successful_calls = len(hourly_rows)
    if successful_calls < config.min_hourly_successes:
        raise RuntimeError(
            f"Only {successful_calls}/24 hourly calls succeeded for {target_date.isoformat()} "
            f"(required at least {config.min_hourly_successes})."
        )

    daily_doc = build_daily_doc(config=config, target_date=target_date, hourly_rows=hourly_rows)
    day_key = day_key_filter(config=config, target_date=target_date)

    client = MongoClient(config.mongo_uri)
    coll = client[config.db_name][config.collection_name]

    upsert_daily_base_doc(coll=coll, day_key=day_key, daily_doc=daily_doc)

    trailing_cursor = coll.find(
        build_docs_upto_target_filter(config=config, target_date=target_date),
        {"_id": 0, "temp": 1, "max": 1, "min": 1, "prcp": 1, "year": 1, "mo": 1, "da": 1},
    ).sort([("year", -1), ("mo", -1), ("da", -1)]).limit(30)
    trailing_desc = list(trailing_cursor)

    aggregate_fields = compute_aggregate_fields(target_date=target_date, trailing_desc=trailing_desc)
    update_doc_aggregates(coll=coll, day_key=day_key, aggregate_fields=aggregate_fields)

    # Re-fetch the completed document (without _id) for XCom / downstream use.
    final_doc = coll.find_one(day_key, {"_id": 0})
    client.close()

    print(f"Target date: {target_date.isoformat()}")
    print(f"Hourly success count: {successful_calls}/24")
    print(f"Mongo target: {config.db_name}.{config.collection_name}")
    print("Upserted daily base fields and updated aggregate fields for target doc.")

    return dict(final_doc) if final_doc else {}


# ---------------------------------------------------------------------------
# Standalone CLI entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    config = load_config()
    target_date = resolve_target_date(args.date, config.tz_name)
    run_ingest(config=config, target_date=target_date)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
