"""Transform raw JSON into clean tabular data. Flatten JSON, handle nulls, convert types"""

# Standard library imports
import json

# Third-party imports
import pandas as pd


def flatten_weather_data(data: dict):
    """Flatten raw OpenWeather JSON into a single-row dictionary."""
    return {
        "city": data.get("name"),
        "country": data.get("sys", {}).get("country"),
        "timestamp": data.get("timestamp"),
        "datetime_utc": pd.to_datetime(data.get("dt"), unit="s"),
        "lat": data.get("coord", {}).get("lat"),
        "lon": data.get("coord", {}).get("lon"),
        # Main weather measurements
        "temp": data.get("main", {}).get("temp"),
        "feels_like": data.get("main", {}).get("feels_like"),
        "temp_min": data.get("main", {}).get("temp_min"),
        "temp_max": data.get("main", {}).get("temp_max"),
        "pressure": data.get("main", {}).get("pressure"),
        "humidity": data.get("main", {}).get("humidity"),
        "sea_level": data.get("main", {}).get("sea_level"),
        "ground_level": data.get("main", {}).get("grnd_level"),
        # Wind
        "wind_speed": data.get("wind", {}).get("speed"),
        "wind_deg": data.get("wind", {}).get("deg"),
        "wind_gust": data.get("wind", {}).get("gust"),
        # Clouds and visibility
        "clouds": data.get("clouds", {}).get("all"),
        "visibility": data.get("visibility"),
        # Weather description
        "weather_main": data.get("weather", [{}])[0].get("main"),
        "weather_description": data.get("weather", [{}])[0].get("description"),
        # Sunrise and sunset
        "sunrise": pd.to_datetime(data.get("sys", {}).get("sunrise"), unit="s"),
        "sunset": pd.to_datetime(data.get("sys", {}).get("sunset"), unit="s"),
    }


def transform_city_file(filepath: str, city_override: str = None) -> pd.DataFrame:
    """
    Open, flatten, and clean a JSON weather file into a DataFrame.
    Ensures consistent types and removes incomplete or duplicated entries.
    """
    with open(filepath, "r", encoding="utf-8") as file:
        raw_data = json.load(file)

    flat_data = flatten_weather_data(raw_data)

    # Override the city name if provided
    if city_override:
        flat_data["city"] = city_override

    df = pd.DataFrame([flat_data])

    # Remove duplicates
    df = df.drop_duplicates()

    # Enforce numeric types (coerce non-numeric to NaN)
    numeric_cols = [
        "temp",
        "feels_like",
        "temp_min",
        "temp_max",
        "pressure",
        "humidity",
        "sea_level",
        "ground_level",
        "wind_speed",
        "wind_deg",
        "wind_gust",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows missing critical values (no NaN values)
    df = df.dropna(subset=["temp", "humidity", "datetime_utc", "city"])

    return df
