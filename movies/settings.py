OMDB_API_URL = 'https://www.omdbapi.com'

# for production use this data should be encrypted of course;-)
OMDB_API_KEY = ''
DB_NAME = "movies_wh"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
MAX_WORKERS = 10
