import os

import pandas as pd

import settings
from exceptions import ApiLimitExceeded
from external_apis import OmdbApiClient
from interfaces import BaseExtractSourceService


def csv_file_data_loader(source_path="data_sources/revenues_per_day.csv"):
    BASE = os.path.abspath(os.path.dirname(__file__))
    path = f"{BASE}/{source_path}"
    df_raw = pd.read_csv(path)
    df_raw['date'] = pd.to_datetime(df_raw['date'])
    df_raw.loc[df_raw['distributor'] == "-", "distributor"] = 'not provided'
    df_revenue_sorted = df_raw.sort_values(by='date', ascending=False)
    return df_revenue_sorted


class LoadMovieSourceService(BaseExtractSourceService):
    def __init__(self, api_client, data_loader):
        self.api_client = api_client
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
            final_source_data_to_load = pd.merge(source_data, df_combined, on='title', how='left')
            BASE = os.path.abspath(os.path.dirname(__file__))
            final_source_data_to_load.to_csv(f'{BASE}/seeds/raw_source.csv', index=False)

            print("Dane zostały pomyślnie zapisane do bazy danych.")
        except ApiLimitExceeded as e:
            print(e)
        except Exception as e:
            print(e)

    def fetch_data(self, title):
        data = self.api_client.fetch_movie_by_title(title)
        if not data:
            return None
        return {
            'title': data['Title'],
            'year': data['Year'],
            'rated': data['Rated'],
            'released': data['Released'],
            'runtime': data['Runtime'],
            'genre': data['Genre'],
            'director': data['Director'],
            'writer': data['Writer'],
            'actors': data['Actors'],
            'language': data['Language'],
            'country': data['Country'],
            'awards': data['Awards'],
            'poster': data['Poster'],
            'imdb_rating': data['imdbRating'],
            'imdb_votes': data['imdbVotes'],
            'metascore': data.get('Metascore', None),
            'box_office': data['BoxOffice'],
            'imdb_id': data['imdbID']
        }


if __name__ == '__main__':
    api_client = OmdbApiClient(settings)

    service = LoadMovieSourceService(api_client, csv_file_data_loader)
    service.load()
