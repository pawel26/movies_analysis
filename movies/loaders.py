import pandas as pd
import os

from exceptions import ValidationError
from interfaces import BaseLoadInputDataService

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
DATA_SOURCES_DIR = "data_sources"


class CSVFileLoader(BaseLoadInputDataService):

    def validate(self, value):
        if not os.path.exists(value):
            print(f"Error: File {value} not exists")
            raise ValidationError(f"Error: File {value} not exists")
        if not value.endswith('.csv'):
            raise ValidationError("Error: input file should be a csv file")

    def load(self, file_path: str) -> pd.DataFrame:
        self.validate(file_path)
        df_raw = pd.read_csv(file_path)
        return self.clean(df_raw)


class ParquetFileLoader(BaseLoadInputDataService):
    def validate(self, value):
        if not os.path.exists(value):
            raise ValidationError(f"Error: File {value} not exists")
        if not value.endswith('.parquet'):
            raise ValidationError("Error: input file should be a parquet file")

    def load(self, file_path: str) -> pd.DataFrame:
        self.validate(file_path)
        df_raw = pd.read_parquet(file_path)
        return self.clean(df_raw)
