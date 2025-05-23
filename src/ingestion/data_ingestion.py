# standard library imports
import os
import sys
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from typing import Optional

# third party imports
import pandas as pd
import requests
from tqdm import tqdm

# local imports


class HypixelDataIngestor:
    def __init__(self, ingestion_type: str) -> None:
        """
        Initializes the HypixelDataIngestor with the specified ingestion type.

        :param ingestion_type: Type of ingestion, either "historical" or "most_recent".
        :raises ValueError: If the ingestion type is not valid.
        """

        allowed_ingestion_types = ["historical", "most_recent"]
        if ingestion_type not in allowed_ingestion_types:
            raise ValueError(
                f"Invalid ingestion type: {ingestion_type}. Allowed types are: {allowed_ingestion_types}"
            )

        self.ingestion_type = ingestion_type

        self.base_url = "https://dl.minetrack.me/Java"
        self.export_start_date = datetime(year=2020, month=8, day=1).date()
        self.today_datetime = datetime.now(timezone.utc)

        self.path_to_data = os.path.join(os.path.dirname(__file__), "..", "..", "data")
        self.output_path = os.path.join(self.path_to_data, "player_counts.csv")

    def get_most_recent_date(self) -> date:
        """
        Returns the most recent date for which data is available.

        :return: Most recent date.
        """

        if self.today_datetime.hour < 1:
            return self.today_datetime.date() - timedelta(days=2)
        else:
            return self.today_datetime.date() - timedelta(days=1)

    def get_data_for_date(self, d: date) -> Optional[pd.DataFrame]:
        """
        Fetches data for a specific date from Minetrack and returns a DataFrame with hourly peak player counts.

        :param date: The date for which to fetch data.
        :return: DataFrame containing hourly peak player counts for the specified date or None if an error occurs.
        """

        year = d.year
        month = d.month
        day = d.day
        url = f"{self.base_url}/{day}-{month}-{year}.csv"

        response = requests.get(url)
        try:
            df = pd.read_csv(StringIO(response.text))
            df = (
                df.loc[df["ip"] == "mc.hypixel.net"]
                .drop(columns=["ip"])
                .sort_values(by="timestamp")
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df = df.set_index("timestamp").resample("h").max().reset_index()
            df = df.rename(
                columns={"timestamp": "Timestamp", "playerCount": "Peak Player Count"}
            )

            # Make sure all timestamps correspond to the input date
            df = df.loc[df["Timestamp"].dt.date == d].reset_index(drop=True)

            return df
        except Exception as e:
            return None

    def get_all_historical_data(self) -> pd.DataFrame:
        legacy_df = pd.read_csv(
            os.path.join(self.path_to_data, "old", "legacy_data.csv"),
            parse_dates=["Timestamp"],
        )
        historical_dfs = [legacy_df]
        end_date = self.get_most_recent_date()
        date_list = [
            self.export_start_date + timedelta(days=i)
            for i in range((end_date - self.export_start_date).days + 1)
        ]
        for d in tqdm(date_list):
            df = self.get_data_for_date(d)
            if df is not None:
                historical_dfs.append(df)

        df = (
            pd.concat(historical_dfs, ignore_index=True)
            .sort_values(by="Timestamp")
            .reset_index(drop=True)
        )
        df = df.set_index("Timestamp").resample("h").asfreq().reset_index()
        df["Peak Player Count"] = df["Peak Player Count"].astype("Int64")

        return df

    def get_most_recent_data(self) -> pd.DataFrame:
        old_df = pd.read_csv(
            self.output_path,
            parse_dates=["Timestamp"],
        )
        latest_date = old_df["Timestamp"].max().date()
        end_date = self.get_most_recent_date()
        date_list = [
            latest_date + timedelta(days=i)
            for i in range(1, (end_date - latest_date).days + 1)
        ]
        historical_dfs = [old_df]
        for d in date_list:
            df = self.get_data_for_date(d)
            if df is not None:
                historical_dfs.append(df)

        df = (
            pd.concat(historical_dfs, ignore_index=True)
            .sort_values(by="Timestamp")
            .reset_index(drop=True)
        )
        df = df.set_index("Timestamp").resample("h").asfreq().reset_index()
        df["Peak Player Count"] = df["Peak Player Count"].astype("Int64")

        return df

    def __call__(self) -> None:
        if self.ingestion_type == "historical":
            df = self.get_all_historical_data()
        else:
            df = self.get_most_recent_data()

        if not os.path.exists(self.path_to_data):
            os.makedirs(self.path_to_data)

        df.to_csv(self.output_path, index=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_ingestion.py <ingestion_type>")
        sys.exit(1)

    ingestion_type = sys.argv[1]
    ingestor = HypixelDataIngestor(ingestion_type)
    ingestor()
