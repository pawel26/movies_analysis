from abc import ABC, abstractmethod

import pandas as pd


class BaseExtractSourceService(ABC):

    @abstractmethod
    def load(self, *args, **kwargs):
        pass

    @abstractmethod
    def store(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch_data(self, title):
        pass


class BaseLoadInputDataService(ABC):
    def __init__(
        self,
        sort_values: bool = True,
        sort_by_column="date",
        convert_to_datetime_columns: list = None,
        replace_values: list[dict] = None
    ):
        self.sort_values = sort_values
        self.sort_by_column = sort_by_column
        if convert_to_datetime_columns is None:
            self.convert_to_datetime_columns = ["date"]
        else:
            self.convert_to_datetime_columns = convert_to_datetime_columns
        if replace_values is None:
            self.replace_values = [{"key": "distributor", "value": "-", "replace_by": "N/A"}]
        else:
            self.replace_values = replace_values

    @abstractmethod
    def validate(self, *args, **kwargs):
        pass

    @abstractmethod
    def load(self, *args, **kwargs):
        pass

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df.dropna(inplace=True)
        if self.convert_to_datetime_columns:
            df[self.convert_to_datetime_columns] = df[self.convert_to_datetime_columns].apply(pd.to_datetime)
        if self.replace_values:
            replace_map = {col["key"]: {col["value"]: col["replace_by"]} for col in self.replace_values}
            df.replace(replace_map, inplace=True)
        if self.sort_values:
            df = df.sort_values(by=self.sort_by_column, ascending=False)
        return df

