import os

from prefect import flow, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from sqlalchemy import create_engine

import settings
from exceptions import ValidationError
from external_apis import OmdbApiClient
from file_service import FileService
from loaders import CSVFileLoader, ParquetFileLoader
from load_movies_service import LoadMovieSourceService
from produce_parquet_service import ProduceParquetFilesService


@task
def produce_input():
    loader = CSVFileLoader()
    file_service = FileService()
    service = ProduceParquetFilesService(loader, file_service=file_service)
    try:
        service.execute()
    except Exception as exc:
        print("Error ocurred: ", exc)
        return False
    return True

@task
def prepare_data():
    api_client = OmdbApiClient(settings)
    db_connection = create_engine(settings.DB_CONNECTION_STRING)
    loader = ParquetFileLoader()
    BASE_PATH = os.path.abspath(os.path.dirname(__file__))
    DATA_SOURCES_DIR = "data_sources"
    path = f"{BASE_PATH}/{DATA_SOURCES_DIR}/parquet_files/"

    file_service = FileService()
    service = LoadMovieSourceService(api_client, db_connection, loader)
    for file_path in file_service.get_files(path):
        service.load(file_path)
        file_service.move_file(
            file_path=file_path,
            destination_path=f"{BASE_PATH}/{DATA_SOURCES_DIR}/processed_parquet"
        )


@task
def transform_data():
    trigger_dbt_cli_command("dbt debug")
    trigger_dbt_cli_command("dbt test")
    trigger_dbt_cli_command("dbt run")
    trigger_dbt_cli_command("dbt test")  # verify after run again


@flow
def movies_flow():
    if produce_input():
        prepare_data()
        transform_data()
    else:
        print("No new data to process")


if __name__ == "__main__":
    movies_flow.serve(
        name="movies-deployment",
        tags=["onboarding"],
        pause_on_shutdown=True,
        interval=180
    )

