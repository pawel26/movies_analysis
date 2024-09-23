import json
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
            try:
                return response.json()
            except ValueError:
                print(f"unable to parse API response correctly for movie {title}")
                return None
        return self.handle_bad_response(response.json(), title)

    def handle_bad_response(self, response, title):
        if response.get('Response') == 'False':
            error_message = response.get('Error')
            if error_message == "Movie not found!":
                print(f"Movie {title} not found in OMDb.")
            elif error_message == "Too many results.":
                print(f"Too many results for '{title}'.")
            elif error_message == "Invalid API key!":
                print("Invalid API key.")
            elif error_message == "Request limit reached!":
                print("API request limit reached.")
                raise ApiLimitExceeded
            else:
                print(f"unknown error: {error_message}")
            return None
