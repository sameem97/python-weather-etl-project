"""Load weather data from JSON files into a local MS SQL Server instance."""

# Standard library imports
import os
import urllib

# Third-party imports
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def load_dataframe_to_msserver(df: pd.DataFrame, table_name="weather_data"):
    """Load the DataFrame into a local MS SQL Server instance."""
    server = "localhost"
    database = "weather_db"
    username = os.getenv("MSSERVER_USER")
    password = os.getenv("MSSERVER_PASSWORD")
    driver = "ODBC Driver 17 for SQL Server"

    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        with engine.connect() as connection:
            # Delete existing rows for the same city
            for city in df["city"].unique():
                delete_query = f"DELETE FROM {table_name} WHERE city = '{city}'"
                connection.execute(delete_query)

            # Append new data to the table
            df.to_sql(name=table_name, con=connection, if_exists="append", index=False)
        print("✅ Data loaded into MS SQL Server successfully!")
    except SQLAlchemyError as e:
        print(f"❌ Error loading into MS SQL Server: {e}")
