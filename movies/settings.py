OMDB_API_URL = 'https://www.omdbapi.com'
OMDB_API_KEY = '9449f34e'
DB_NAME = "movies_wh"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
