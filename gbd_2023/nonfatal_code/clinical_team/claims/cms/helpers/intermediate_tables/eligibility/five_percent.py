import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.funcs import parquet_writer, validate_data


class Five_Percent:
    def __init__(self, df: pd.DataFrame, group: int, year: int):
        self.df = df
        self.temp = None
        self.year = year
        self.group = group

    def sample_group(self):
        self.temp["five_percent_sample"] = np.where(
            self.temp.five_percent_sample.isin(["01", "04", "Y"]), True, False
        )

    def enhanced(self):
        self.temp["eh_five_percent_sample"] = np.where(
            self.temp.eh_five_percent_sample == "Y", True, False
        )

    def write_out(self):
        base = filepath_parser(
            ini="pipeline.cms", section="table_outputs", section_key="sample"
        )
        validate_data(df=self.temp, cms_system="mdcr", table="five_percent")
        parquet_writer(df=self.temp, path="FILEPATH")

    def process(self):
        cols = ["eh_five_percent_sample", "five_percent_sample", "year_id"]

        self.temp = self.df[["bene_id"] + cols]
        self.enhanced()
        self.sample_group()
        self.temp.drop_duplicates(inplace=True)
        self.write_out()

        # Remove year_id from cols to retain column in df
        cols.remove("year_id")

        return self.df.drop(cols, axis=1)
