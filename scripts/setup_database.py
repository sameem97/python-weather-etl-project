"""Initialize SQL Server database and table for weather data ETL pipeline."""

# Standard library imports
import os

# Third-party imports
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def setup_database():
    """Create database and table if they don't exist."""
    # Load environment variables
    load_dotenv()

    # Database connection parameters
    server = "localhost"
    username = os.getenv("MSSERVER_USER")
    password = os.getenv("MSSERVER_PASSWORD")
    driver = "ODBC Driver 17 for SQL Server"

    # First connect to master database to create weather_db
    master_conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE=master;"
        f"UID={username};"
        f"PWD={password};"
    )

    try:
        # Create engine for master database
        master_engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={master_conn_str}"
        )

        with master_engine.connect() as conn:
            # Create database if it doesn't exist
            conn.execute(
                text(
                    "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'weather_db') CREATE DATABASE weather_db"
                )
            )
            print("✅ Database 'weather_db' created or already exists")

        # Now connect to weather_db to create table
        weather_conn_str = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE=weather_db;"
            f"UID={username};"
            f"PWD={password};"
        )
        weather_engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={weather_conn_str}"
        )

        with weather_engine.connect() as conn:
            # Create weather_data table if it doesn't exist
            create_table_sql = """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'weather_data')
            CREATE TABLE weather_data (
                city VARCHAR(100),
                country VARCHAR(10),
                timestamp DATETIME,
                datetime_utc DATETIME,
                lat FLOAT,
                lon FLOAT,
                temp FLOAT,
                feels_like FLOAT,
                temp_min FLOAT,
                temp_max FLOAT,
                pressure FLOAT,
                humidity FLOAT,
                sea_level FLOAT,
                ground_level FLOAT,
                wind_speed FLOAT,
                wind_deg FLOAT,
                wind_gust FLOAT,
                clouds FLOAT,
                visibility FLOAT,
                weather_main VARCHAR(50),
                weather_description VARCHAR(100),
                sunrise DATETIME,
                sunset DATETIME
            )
            """
            conn.execute(text(create_table_sql))
            print("✅ Table 'weather_data' created or already exists")

    except SQLAlchemyError as e:
        print(f"❌ Error setting up database: {e}")
        raise


if __name__ == "__main__":
    setup_database()
