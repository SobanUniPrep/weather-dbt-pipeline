import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

CITIES = [
    {"name": "Prague", "latitude": 50.08, "longitude": 14.44, "timezone": "Europe/Prague"},
    {"name": "London", "latitude": 51.51, "longitude": -0.13, "timezone": "Europe/London"},
    {"name": "New York", "latitude": 40.71, "longitude": -74.01, "timezone": "America/New_York"},
    {"name": "Tokyo", "latitude": 35.69, "longitude": 139.69, "timezone": "Asia/Tokyo"},
    {"name": "Sydney", "latitude": -33.87, "longitude": 151.21, "timezone": "Australia/Sydney"},
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
]

BACKFILL_DAYS = 7


def get_snowflake_connection():
    missing = [k for k, v in SNOWFLAKE_CONFIG.items() if v is None]
    if missing:
        raise ValueError(f"Missing environment variables: {missing}")
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)