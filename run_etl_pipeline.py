"""
This script runs the ETL pipeline for weather data.

It extracts weather data from an external source, transforms it into a usable format,
and loads it into a Microsoft SQL Server database.
"""

# Standard library imports
import os
import subprocess

# Third-party imports
from dotenv import load_dotenv
from pandas.errors import ParserError
from sqlalchemy.exc import SQLAlchemyError

# Local application imports
from transform.clean_json_weather import transform_city_file
from load.load_to_msserver import load_dataframe_to_msserver


def main():
    """Run the complete ETL pipeline: extract weather data, transform it, and load to database."""
    load_dotenv()  # Load and validate environment variables
    required_vars = ["API_KEY", "MSSERVER_USER", "MSSERVER_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(
            f"❌ Missing required environment variables: {', '.join(missing_vars)}"
        )

    # Step 1: Run extract.py to fetch new data
    print("\n📡 Starting Extract...")
    try:
        subprocess.run(["python", "extract/get_weather_json.py"], check=True)
        print("✅ Extract completed.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during extraction: {e}")
        return

    # Step 2: Transform + Load
    print("\n🧪 Starting Transform + Load...")
    cities = ["London", "New York", "Tokyo", "Paris", "Berlin"]

    for city in cities:
        file_path = f"extract/{city.replace(' ', '_')}_weather_output.json"

        if not os.path.exists(file_path):  # Check if file exists
            print(f"⚠️ File not found for city: {file_path}")
            continue

        try:
            df = transform_city_file(file_path, city_override=city)

            if df.empty:
                print(f"⚠️ No valid data for {city}, skipping.")
                continue

            load_dataframe_to_msserver(df)

        except (ParserError, SQLAlchemyError) as e:
            print(f"❌ Error processing {city}: {e}")


if __name__ == "__main__":
    main()
