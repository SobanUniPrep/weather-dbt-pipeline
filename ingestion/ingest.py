import argparse
import logging
import requests
from datetime import datetime, timedelta, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

from config import (
    get_snowflake_connection,
    CITIES,
    API_BASE_URL,
    HOURLY_VARIABLES,
    BACKFILL_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@retry(
    retry=retry_if_exception_type((requests.exceptions.Timeout,
                                   requests.exceptions.ConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def fetch_weather(city: dict, start_date: str, end_date: str) -> dict:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": city["timezone"],
        "start_date": start_date,
        "end_date": end_date,
    }
    logger.info(f"Fetching weather for {city['name']} | {start_date} to {end_date}")
    response = requests.get(API_BASE_URL, params=params, timeout=10)

    if response.status_code >= 500:
        raise requests.exceptions.ConnectionError(
            f"Server error {response.status_code} for {city['name']}"
        )
    if response.status_code >= 400:
        logger.error(f"Client error {response.status_code} for {city['name']} - no retry")
        response.raise_for_status()

    return response.json()


def parse_response(city: dict, data: dict) -> list[dict]:
    hourly = data["hourly"]
    rows = []
    for time, temp, humidity, precip, code, wind_speed, wind_dir in zip(
        hourly["time"],
        hourly["temperature_2m"],
        hourly["relative_humidity_2m"],
        hourly["precipitation"],
        hourly["weather_code"],
        hourly["wind_speed_10m"],
        hourly["wind_direction_10m"],
    ):
        rows.append({
            "CITY_NAME": city["name"],
            "LATITUDE": str(city["latitude"]),
            "LONGITUDE": str(city["longitude"]),
            "TIMEZONE": city["timezone"],
            "TIMESTAMP": str(time),
            "TEMPERATURE_2M": str(temp),
            "RELATIVE_HUMIDITY_2M": str(humidity),
            "PRECIPITATION": str(precip),
            "WEATHER_CODE": str(code),
            "WIND_SPEED_10M": str(wind_speed),
            "WIND_DIRECTION_10M": str(wind_dir),
        })
    return rows


def is_first_run(conn) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM WEATHER_DB.RAW.RAW_WEATHER")
    count = cursor.fetchone()[0]
    cursor.close()
    return count == 0


def load_to_snowflake(conn, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="RAW_WEATHER",
        database="WEATHER_DB",
        schema="RAW",
    )
    logger.info(f"Loaded {num_rows} rows in {num_chunks} chunks | success={success}")


def main():
    parser = argparse.ArgumentParser(description="Weather ingestion pipeline")
    parser.add_argument("--start-date", type=str, help="Backfill start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="Backfill end date YYYY-MM-DD")
    args = parser.parse_args()

    conn = get_snowflake_connection()

    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        logger.info(f"Manual backfill mode: {start_date} to {end_date}")
    elif is_first_run(conn):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%d")
        logger.info(f"First run detected - backfill mode: {start_date} to {end_date}")
    else:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_date = now.strftime("%Y-%m-%d")
        start_date = end_date
        logger.info(f"Hourly mode: {start_date}")

    all_rows = []
    for city in CITIES:
        try:
            data = fetch_weather(city, start_date, end_date)
            rows = parse_response(city, data)
            all_rows.extend(rows)
            logger.info(f"Parsed {len(rows)} rows for {city['name']}")
        except Exception as e:
            logger.error(f"Failed to fetch {city['name']}: {e}")
            continue

    if all_rows:
        load_to_snowflake(conn, all_rows)
    else:
        logger.warning("No rows to load")

    conn.close()
    logger.info("Pipeline finished")


if __name__ == "__main__":
    main()