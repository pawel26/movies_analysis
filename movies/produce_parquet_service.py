import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from exceptions import ValidationError
from file_service import FileService
from loaders import CSVFileLoader, DATA_SOURCES_DIR, BASE_PATH


class ProduceParquetFilesService:
    def __init__(self, loader, file_service):
        self.loader = loader
        self.file_service = file_service
        self.output_filename = "movie_revenue.parquet"
        self.output_dir = f"{BASE_PATH}/{DATA_SOURCES_DIR}/parquet_files"
        self.processed_destination_path = f"{BASE_PATH}/{DATA_SOURCES_DIR}/processed"

    def execute(self, group_by_column="date"):
        for file_path in self.file_service.get_files(f"{BASE_PATH}/{DATA_SOURCES_DIR}/raw"):
            print("processing source file: ", file_path)
            loaded_data = self.loader.load(file_path=file_path)
            loaded_data[group_by_column] = pd.to_datetime(loaded_data[group_by_column])
            loaded_data['year_month'] = loaded_data[group_by_column].dt.to_period('M')
            grouped_source_data = loaded_data.groupby('year_month')

            self.file_service.create_dirs(self.output_dir)
            for period, group in grouped_source_data:
                self.append_or_create_parquet(group, self.get_parquet_file_path(period))

            self.handle_processed_file(file_path)

    def append_or_create_parquet(self, data, parquet_file_path):
        new_df = pd.DataFrame(data)
        if self.file_service.file_exists(parquet_file_path):
            existing_df = pd.read_parquet(parquet_file_path)
            if 'title' in existing_df.index.names:
                existing_df.reset_index(inplace=True)
            if 'title' in new_df.index.names:
                new_df.reset_index(inplace=True)
            if new_df['title'].duplicated().any():
                new_df = new_df.drop_duplicates(subset=['title'])
            if existing_df['title'].duplicated().any():
                existing_df = existing_df.drop_duplicates(subset=['title'])

            merged_df = new_df.merge(existing_df, on="title", how='inner', indicator=True)
            new_rows = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)
            if not new_rows.empty:
                existing_df = existing_df.reset_index(drop=True).copy()
                new_rows = new_rows.reset_index(drop=True).copy()
                try:
                    updated_df = pd.concat([existing_df, new_rows], axis=0, ignore_index=True)
                except pd.errors.InvalidIndexError:
                    new_rows.index = pd.RangeIndex(start=0, stop=len(new_rows), step=1)
                    updated_df = pd.concat([existing_df, new_rows], axis=0)
                updated_df.to_parquet(parquet_file_path)
                print(f"Added data to already existing file: {parquet_file_path}")
            else:
                print(f"No new data to add: {parquet_file_path}")
        else:
            output_dir = os.path.dirname(parquet_file_path)
            self.file_service.create_dirs(output_dir)
            print(f"new parquet file created: {parquet_file_path}")

        table = pa.Table.from_pandas(new_df)
        pq.write_table(table, parquet_file_path, compression='snappy', use_dictionary=True)

    def get_parquet_file_path(self, period):
        return os.path.join(f"{self.output_dir}/{period.year}/{period.month:02d}", self.output_filename)

    def handle_processed_file(self, file_path):
        self.file_service.move_file(
            file_path=file_path,
            destination_path=self.processed_destination_path
        )


if __name__ == '__main__':
    loader = CSVFileLoader()
    file_service = FileService()
    service = ProduceParquetFilesService(loader, file_service=file_service)
    try:
        service.execute()
    except ValidationError:
        print("No new source file to process")
