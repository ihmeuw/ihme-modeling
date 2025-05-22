"""
Track the ICD mart by year and file_id
"""

from datetime import datetime
from typing import Any, List

import pandas as pd

from pol_nhf.schema import file_handlers


class YearFileSourceManager:
    def __init__(self, table: pd.DataFrame):
        """Manages the year file source tracker for bested
        raw data inputs.

        Args:
            table (pd.DataFrame): Expected is the ouput of the call
                pull_poland_year_source_table()
        """
        self.table = table
        self.year_id, self.file_source_id = self.set_year_file_source
        self.year_table
        self.best_table
        self.best_list: List[Any] = []

    @property
    def set_year_file_source(self):
        return None, None

    @set_year_file_source.setter
    def set_year_file_source(self, value):
        self.year_id, self.file_source_id = value
        self.year_table
        self.best_table

    @property
    def year_table(self):
        return self.table[(self.table.year_id == self.year_id)]

    @property
    def best_table(self):
        df = self.table[(self.table.year_id == self.year_id) & (self.table.is_best == 1)]
        if df.shape[0] > 1:
            raise ValueError(f"More than one bested file source for year:{self.year_id}")
        return df

    def _verify_file_source_id(self) -> None:
        """
        Verify that file source is present in config file.
        """
        sf = file_handlers.SourceFiles()
        if self.file_source_id not in sf:
            raise RuntimeError(
                "file source id is missing from "
                "FILEPATH"
            )

    def _verify_year_file_source_id(self) -> None:
        """
        Verify that there is only unique year_id and file_source_id pairings.
        """
        df = self.year_table[self.year_table.file_source_id == self.file_source_id]

        if any(df.duplicated(subset=["year_id", "file_source_id"])):
            raise ValueError("Duplicated year and file source combination")

        if not df.empty:
            raise ValueError("year_id and file_source_id combination is already present")

    def _create_best_record(self, df_up: pd.DataFrame) -> pd.DataFrame:
        """
        Create a record / log of the updates made to exisiting year file sources combinations
        in the tracking table when the year id is best.
        """
        id_vars = ["year_id"]
        value_vars = ["file_source_id", "is_best", "conversion_date", "log_uuid"]

        df_up_event_id = int(df_up.event_id)
        df_best_event_id = int(self.best_table.event_id)
        df_up = df_up.melt(id_vars=id_vars, value_vars=value_vars, var_name="col_name")
        df_best = self.best_table.melt(id_vars, value_vars=value_vars, var_name="col_name")

        df_up["event_id"] = df_up_event_id
        df_best["event_id"] = df_best_event_id

        return pd.merge(
            df_up, df_best, on=["year_id", "col_name"], how="outer", suffixes=["_new", "_old"]
        )

    def _clean_cols(self, df: pd.DataFrame, suffix: str) -> pd.DataFrame:
        """
        Identify the correct columns to use after merge and remove suffix name.
        """
        inverse = "old" if suffix == "new" else "new"
        cols = [e for e in df.columns if not e.endswith(inverse)]
        df = df[cols]
        col_up = {e: e.replace(f"_{suffix}", "") for e in df.columns if e.endswith(suffix)}
        df.rename(col_up, axis=1, inplace=True)

        return df.drop("_merge", axis=1)

    def _validate_table(self, df: pd.DataFrame) -> None:
        """
        Validate new table.
        """
        if any(df.duplicated(subset=["year_id", "file_source_id"])):
            raise RuntimeError("There are duplicated year and file sources")

        temp = df[df.is_best == 1]
        temp = temp.groupby("year_id")["is_best"].nunique().reset_index(name="vals")
        if not all(temp.vals == 1):
            raise RuntimeError("Multiple bested years")

    def verification(self, overwrite: bool) -> None:
        self._verify_file_source_id()
        if not overwrite:
            self._verify_year_file_source_id()

    def create_update(self, is_best: bool, overwrite: bool, log_uuid: str) -> pd.DataFrame:
        """
        Create a record of the ICD mart processing at the year / file source id level.
        Extends self.best_list when there is a change to besting for a given year.
        Does not log newly bested years.
        """
        self.verification(overwrite=overwrite)

        df_up = pd.DataFrame(
            {
                "year_id": self.year_id,
                "file_source_id": self.file_source_id,
                "is_best": int(is_best),
                "conversion_date": datetime.now(),
                "log_uuid": log_uuid,
                "event_id": (
                    1 if self.year_table.empty else int(self.year_table.event_id.max()) + 1
                ),
            },
            index=[0],
        )

        if is_best and not self.best_table.empty:
            self.best_list.append(self._create_best_record(df_up))

            # if the file source ids from df_up and self.best_table are the same
            # then continue otherwise update the previous
            best_file_source_id = self.best_table.file_source_id.item()
            if self.file_source_id != best_file_source_id:
                best_cp = self.best_table.copy()
                best_cp["is_best"] = 0
                df_up = pd.concat([df_up, best_cp], sort=False)

        return df_up

    def create_year_file_source_table(self, df: pd.DataFrame, save: bool = True) -> None:
        """
        Creates, with option to save, a new year_file_source_table
        """
        m = pd.merge(
            self.table,
            df,
            on=["year_id", "file_source_id"],
            how="outer",
            suffixes=["_old", "_new"],
            indicator=True,
        )
        new_rows = self._clean_cols(m[m._merge.isin(["right_only", "both"])], "new")
        unchanged_rows = self._clean_cols(m[m._merge == "left_only"], "old")

        table = pd.concat([new_rows, unchanged_rows], sort=False)
        table.reset_index(drop=True, inplace=True)
        self._validate_table(table)

        if save:
            file_handlers.save_year_source_table(df=table)

    def save_best_history(self) -> None:
        """
        Concat best history on disk with best_list and save to disk.
        """
        if self.best_list:
            df = pd.concat(self.best_list)
            prev = file_handlers.pull_year_source_best_table()
            df = pd.concat([df, prev], sort=False)
            df.reset_index(drop=True, inplace=True)
            file_handlers.save_year_source_best_table(df)
