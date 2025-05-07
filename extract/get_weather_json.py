"""Fetch and save current weather data from OpenWeatherMap API for multiple cities."""

# Standard library imports
import json
import os
from datetime import datetime, timedelta, timezone

# Third-party imports
import requests


def fetch_and_save_weather(city: str, api_key: str) -> None:
    """
    Fetch weather data for a city and save it as a local JSON file.
    """
    geocoding_url = "http://api.openweathermap.org/geo/1.0/direct"
    weather_url = "https://api.openweathermap.org/data/2.5/weather"

    # Step 1: Get coordinates
    try:
        geo_resp = requests.get(
            geocoding_url, params={"q": city, "limit": 1, "appid": api_key}, timeout=10
        )
        geo_resp.raise_for_status()  # raise exception if status code is not 200
        geo_data = geo_resp.json()  # convert response to json
        if not geo_data:  # if no data is returned
            print(f"⚠️ No geocoding data found for {city}. Skipping.")
            return
        lat = geo_data[0]["lat"]
        lon = geo_data[0]["lon"]
    except (
        requests.RequestException
    ) as e:  # capture exception raised by .raise_for_status()
        print(f"❌ Geocoding error for {city}: {e}")
        return

    # Step 2: Get weather data
    try:
        weather_resp = requests.get(
            weather_url,
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
            timeout=10,
        )
        weather_resp.raise_for_status()
        weather_data = weather_resp.json()
    except requests.RequestException as e:
        print(f"❌ Weather fetch error for {city}: {e}")
        return

    # Step 3: Add local timestamp
    timezone_offset = weather_data.get("timezone", 0)
    local_time = datetime.now(timezone.utc) + timedelta(seconds=timezone_offset)
    weather_data["timestamp"] = local_time.strftime("%Y-%m-%d %H:%M:%S")

    # Step 4: Save to file
    output_path = f"extract/{city.replace(' ', '_')}_weather_output.json"
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(weather_data, f, indent=4)
        print(f"✅ Weather for {city} saved to {output_path}")
    except (IOError, OSError) as e:
        print(f"❌ Error saving {city}'s data: {e}")


def main():
    """Fetch and save weather data for a predefined list of cities."""
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise EnvironmentError("❌ API_KEY environment variable not set.")

    cities = ["London", "New York", "Tokyo", "Paris", "Berlin"]
    for city in cities:
        print(f"\n📡 Fetching weather for {city}...")
        fetch_and_save_weather(city, api_key)


if __name__ == "__main__":
    main()
