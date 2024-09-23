import json


class OmdbMovieTransformMixin:
    def get_context_data(self, data):
        imdb_rating = None
        if 'imdbRating' in data and data['imdbRating'].replace('.', '', 1).isdigit():
            imdb_rating = float(data['imdbRating'])

        imdb_votes = None
        if 'imdbVotes' in data:
            imdb_votes = (
                int(data['imdbVotes'].replace(',', '')) if data['imdbVotes'].replace(',', '').isdigit() else None
            )
        box_office = None
        if 'BoxOffice' in data:
            box_office_str = data['BoxOffice'].replace('$', '').replace(',', '')
            if 'M' in box_office_str:
                box_office = float(box_office_str.replace('M', '')) * 1000000
            elif 'K' in box_office_str:
                box_office = float(box_office_str.replace('K', '')) * 1000
            elif box_office_str.isdigit():
                box_office = float(box_office_str)
        ratings = data.get('Ratings', {})

        title = data.get('Title')
        if not title:
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
            'languages': data['Language'].split(","),
            'countries': data['Country'].split(","),
            'awards': data['Awards'] if data['Awards'] != 'N/A' else '',
            'ratings': json.dumps(ratings),
            'imdb_rating': imdb_rating,
            'imdb_votes': imdb_votes,
            'metascore': data.get('Metascore', None),
            'box_office': box_office,
            'imdb_id': data['imdbID']
        }
