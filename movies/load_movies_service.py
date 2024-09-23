import os

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import threading
from sqlalchemy import create_engine

import settings
from exceptions import ApiLimitExceeded
from external_apis import OmdbApiClient
from interfaces import BaseExtractSourceService
from mixins import OmdbMovieTransformMixin


def csv_file_data_loader(source_path="data_sources/revenues_per_day.csv"):
    BASE = os.path.abspath(os.path.dirname(__file__))
    path = f"{BASE}/{source_path}"
    df_raw = pd.read_csv(path)
    df_raw = df_raw.dropna(subset=['title'])
    df_raw.dropna(inplace=True)
    df_raw['date'] = pd.to_datetime(df_raw['date'])
    df_raw.loc[df_raw['distributor'] == "-", "distributor"] = 'not provided'
    df_revenue_sorted = df_raw.sort_values(by='date', ascending=False)
    return df_revenue_sorted


class LoadMovieSourceService(BaseExtractSourceService, OmdbMovieTransformMixin):
    def __init__(self, api_client, db_connection, data_loader):
        self.api_client = api_client
        self.db_connection = db_connection
        self.data_loader = data_loader
        self.lock = threading.Lock()
        self.api_limit_exceeded = False

    def fetch_data(self, title):
        with self.lock:
            if self.api_limit_exceeded:
                return None

        try:
            data = self.api_client.fetch_movie_by_title(title)
            if not data:
                print(f"no additional data for movie title: {title}")
                return None

            joined_data = self.get_context_data(data)
            return joined_data

        except ApiLimitExceeded:
            with self.lock:
                print(f"API limit reached. Stopping further requests.")
                self.api_limit_exceeded = True
            return None
        except Exception as e:
            print(f"Error fetching data for {title}: {e}")
            return None

    def load(self):
        source_data = self.data_loader()
        additional_data = []
        unique_titles = source_data['title'].unique()


        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_data, title): title for title in unique_titles}

            for counter, future in enumerate(as_completed(futures), 1):
                with self.lock:
                    if self.api_limit_exceeded:
                        print("Stopping execution due to API limit being reached.")
                        break

                result = future.result()
                if result:
                    additional_data.append(result)

                if counter % 100 == 0:
                    print(f"Executed for {counter} items")

        df_combined = pd.DataFrame(additional_data)
        self.store(source_data, df_combined)

    def store(self, source_data, df_combined, how="inner"):
        if df_combined.empty:
            print("No data to store.")
            return None
        final_source_data_to_load = pd.merge(source_data, df_combined, on='title', how=how)
        final_source_data_to_load = final_source_data_to_load.dropna()
        final_source_data_to_load.to_sql(
            'raw_source', self.db_connection, schema='dev', if_exists='replace', index=False
        )
        print("Data fetched and saved succesfully.")


if __name__ == '__main__':
    api_client = OmdbApiClient(settings)
    db_connection = create_engine(settings.DB_CONNECTION_STRING)

    service = LoadMovieSourceService(api_client, db_connection, csv_file_data_loader)
    service.load()
