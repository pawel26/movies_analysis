from http import HTTPStatus

import requests

import settings
from exceptions import ApiLimitExceeded


class OmdbApiClient:
    def __init__(self, settings):
        self.settings = settings

    def build_url(self, param_type, param_value):
        return f"{settings.OMDB_API_URL}/?{param_type}={param_value}&apikey={settings.OMDB_API_KEY}"

    def fetch_movie_by_title(self, title):
        url = self.build_url('t', title)
        response = requests.get(url)
        if response.status_code == HTTPStatus.OK:
            return response.json()
        return self.handle_bad_response(response.json(), title)

    def handle_bad_response(self, response, title):
        if response.get('Response') == 'False':
            error_message = response.get('Error')
            if error_message == "Movie not found!":
                print(f"Błąd: Film {title} nie został znaleziony w OMDb.")
                return None
            elif error_message == "Too many results.":
                print(f"Błąd: Zbyt wiele wyników dla tytułu '{title}'.")
                return None
            elif error_message == "Invalid API key!":
                print("Błąd: Nieprawidłowy klucz API.")
                return None
            elif error_message == "Request limit reached!":
                print("Błąd: Przekroczono limit zapytań API.")
                raise ApiLimitExceeded
            else:
                print(f"Nieznany błąd: {error_message}")
            return None
