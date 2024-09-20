import os

import pandas as pd
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
    df_raw['date'] = pd.to_datetime(df_raw['date'])
    df_raw.loc[df_raw['distributor'] == "-", "distributor"] = 'not provided'
    df_revenue_sorted = df_raw.sort_values(by='date', ascending=False)
    return df_revenue_sorted


class LoadMovieSourceService(BaseExtractSourceService, OmdbMovieTransformMixin):
    def __init__(self, api_client, db_connection, data_loader):
        self.api_client = api_client
        self.db_connection = db_connection
        self.data_loader = data_loader

    def load(self):
        try:
            source_data = self.data_loader()
            additional_data = []

            for title in source_data['title'].unique():
                data = self.fetch_data(title)
                if data:
                    additional_data.append(data)
                else:
                    print("no additional data for movie title")

            df_combined = pd.DataFrame(additional_data)
            self.store(source_data, df_combined)
        except ApiLimitExceeded as e:
            print(e)
        except Exception as e:
            print(e)

    def fetch_data(self, title):
        data = self.api_client.fetch_movie_by_title(title)
        if not data:
            return None

        return self.get_context_data(data)

    def store(self, source_data, df_combined, how="inner"):
        final_source_data_to_load = pd.merge(source_data, df_combined, on='title', how=how)
        final_source_data_to_load.to_sql(
            'raw_source', self.db_connection, schema='dev', if_exists='replace', index=False
        )
        print("Dane zostały pomyślnie zapisane do bazy danych.")


if __name__ == '__main__':
    api_client = OmdbApiClient(settings)
    db_connection = create_engine(settings.DB_CONNECTION_STRING)

    service = LoadMovieSourceService(api_client, db_connection, csv_file_data_loader)
    service.load()
