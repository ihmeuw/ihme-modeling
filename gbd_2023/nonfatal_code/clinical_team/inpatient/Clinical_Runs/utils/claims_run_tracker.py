from datetime import datetime
from functools import lru_cache
from getpass import getuser
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from crosscutting_functions.db_connector import database
from crosscutting_functions.validations.decorators import clinical_typecheck


class ClaimsRunTracker:
    def __init__(self):
        """
        Identifies which run_id has the best results for claims sources.
        Bested runs should be complete, having all data needed to Upload
        in the {claim_source}.csv and require no compilation.

        Methods:
        - best_source(): Main method for adding a bested source-run
            to the claims tracker.
        - rollback_best_source(): Revert the bested run_id for a source
            to the previously bested run_id.
        """
        self.bested_by = getuser()
        self.bested_on = datetime.today().strftime("%Y-%m-%d_%H:%M:%S")
        self.tracker_path = FILEPATH
        self.df = pd.read_csv(self.tracker_path, na_values=None)

    def _get_prior_best(self):
        """Gets the previous best run for an update."""

        tmp = self.df.loc[self.df["claim_source"] == self.claim_source]
        tmp = tmp.loc[tmp["is_best"] == 1].reset_index(drop=True)

        if tmp.shape[0] > 1:
            raise RuntimeError(f"{self.claim_source} has {tmp.shape[0]} bested run_id.")
        elif tmp.shape[0] == 0:
            return np.nan
        else:
            return tmp["run_id"][0]

    def _update_vars(self) -> Dict[str, Union[str, int, None]]:
        """Build update data from class attributes"""

        update_vars = {}
        for key, val in vars(self).items():
            if key not in ["df", "tracker_path"]:
                update_vars[key] = val

        return update_vars

    def _prep_update(self):
        """Preps update data to add to tracker.

        Raises:
            RuntimeError: Trying to best a currently bested run.
        """
        data = self._update_vars()
        data["is_best"] = 1
        if data["run_id"] == data["previous_best"]:
            raise RuntimeError(
                f"Run '{data['run_id']}' is already the best for {self.claim_source}."
            )
        row = pd.DataFrame.from_dict(data, orient="index").T
        row = row[list(self.df.columns)]

        self.update_row = row

    def _make_update(self):
        """Adds the update data to tracker."""
        self._prep_update()
        self.df.loc[
            (self.df["claim_source"] == self.claim_source)
            & (self.df["run_id"] == self.update_row["previous_best"][0]),
            "is_best",
        ] = 0
        self.df = pd.concat([self.update_row, self.df], ignore_index=True)
        for col in ["run_id", "is_best", "previous_best"]:
            self.df.loc[self.df[col].notnull(), col] = self.df.loc[
                self.df[col].notnull(), col
            ].astype(int)

    def _get_valid_run_id(self) -> List[int]:
        db = database.Database()
        db.load_odbc(odbc_profile="clinical")

        run_qu = QUERY
        # Can not be an estimiate that currently exists in the db.
        vaild_runs = db.query(run_qu)["run_id"].to_list()

        return vaild_runs

    def _table_validations(self):
        """Final validations of updated tracker.

        Raises:
            RuntimeError: All non-dev claim sources have exactly 1 best run.
            RuntimeError: A bested run_id not in prod DB.
        """
        tmp = self.df.loc[self.df["claim_source"] != "test_source"]
        if tmp["claim_source"].nunique() != tmp["is_best"].sum():
            raise RuntimeError("More than 1 bested run per source.")

        if tmp["run_id"].values.any() not in self._get_valid_run_id():
            raise RuntimeError("Bested run is not a valid prodcution run_id")

    @clinical_typecheck
    def best_source(
        self,
        claim_source: str,
        best_run_id: int,
        update_notes: Union[str, None] = None,
    ):
        """Main method for adding a bested source-run to the claims tracker.

        Args:
            claim_source (str): Claims source abbreviation to best. Ex: mng_h_info
            best_run_id (int): The run_id to label as best for the source.
            update_notes (Union[str, None]): Any notes around update/motivation.
                Defaults to None.
        """
        self.claim_source = claim_source.lower()
        self.run_id = best_run_id
        self.update_notes = update_notes
        self.previous_best = self._get_prior_best()

        self._make_update()
        self._table_validations()

        self.df.to_csv(self.tracker_path, index=False)
        print(f"Bested {claim_source} to run_id {best_run_id}")

    @clinical_typecheck
    def rollback_best_source(self, claim_source: str, update_notes: str = "rollback"):
        """Revert the bested run_id for a source to the previously bested run_id.

        Args:
            claim_source (str): Claims source abbreviation to best. Ex: mng_h_info
            update_notes (str, optional): Any notes around update/motivation..
                Defaults to "rollback".

        Raises:
            RuntimeError: Trying to rollback when no previous best exists.
        """
        tmp = self.df.loc[
            (self.df["claim_source"] == claim_source.lower()) & (self.df["is_best"] == 1)
        ]
        tmp.reset_index(drop=True, inplace=True)
        if tmp.shape[0] > 0 and not pd.isnull(tmp["previous_best"][0]):
            self.best_source(
                claim_source=claim_source,
                best_run_id=tmp["previous_best"][0],
                update_notes=update_notes,
            )
        else:
            raise RuntimeError(f"'{claim_source}' has no prior best run to rollback to.")


@lru_cache
def BestClaimsRuns() -> Dict[str, int]:
    """
    Creates dictionary of bested run_id's keyed by claims source.
    """

    crt = ClaimsRunTracker()
    best_runs = crt.df.loc[(crt.df["is_best"] == 1)].reset_index(drop=True)

    source_run_dict = {}
    for i, claim_source in enumerate(best_runs["claim_source"]):

        source_run_dict.update({claim_source: int(best_runs.loc[i, "run_id"])})

    return source_run_dict
