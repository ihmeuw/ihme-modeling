"""
Author: USERNAME
Date: 7/27/2021
Purpose: Creates new directories and updates injury metadata tables in the clinical db
"""


import re
from pathlib import Path

import pandas as pd


class InjMetadataManager:
    def __init__(self, InpatientWrapper):
        self.iw = InpatientWrapper

        self.update_dict = {
            "input_version_id": None,
            "cf_version_id": None,
            "input_dir": None,
            "cf_dir": None,
        }

    def _column_handler(self, df, keep):
        """Remove and rename columns after a merging cf input data"""
        drop = "old"
        if keep == "old":
            drop = "new"
        drop_list = [e for e in df.columns.tolist() if e.endswith(f"_{drop}")]
        update_dict = {
            e: e.replace(f"_{keep}", "") for e in df.columns.tolist() if e.endswith(f"_{keep}")
        }

        df.drop(drop_list + ["_merge"], axis=1, inplace=True)
        df.rename(update_dict, axis=1, inplace=True)

        assert df.isnull().sum().sum() == 0, "Data was not correctly appended"

        return df

    def _update_cf_input_data(self, df):
        """Identify new or updated rows of data"""

        # left_only indicator signals no matches between the data stored in flat file
        # and the data passed to prop_code_for_hosp_team.
        for e in df._merge.unique().tolist():
            keep = "new"
            if e == "left_only":
                keep = "old"
            yield self._column_handler(df[df._merge == e], keep)

    def _update_rmdata_inj_cf_tbls(self):
        """Update run_metadata and inj_cf_version tbls with new inj_cf_version_id"""
        name = "Updated for GBD 2021"
        description = "EN proprtion applied to Inj bundles"
        self.iw.insert_into_inj_cf_version(
            inj_cf_version_name=name,
            inj_cf_version_description=description,
            run_id=self.iw.run_id,
        )
        dql = QUERY
        version_id = pd.read_sql(dql, self.iw.ddl._create_connection(),)[
            "id"
        ][0]
        self.update_dict["cf_version_id"] = version_id
        self.iw.update_run_metadata(inj_cf_version_id=version_id, ignore_overwrite_errors=True)

    def pull_and_merge_previous_data(
        self, config_key, df, collapse_years_switch, indexcols, save=True
    ):
        """Fetch the data set from a previous run and merge it with the current input data"""
        input_dir = self.iw.pull_version_dir(config_key)

        # blindly assume that the previous version has he most up to date dataset
        if config_key == "inj_cf_en":
            dql = (QUERY
            )
            prev_version_id = pd.read_sql(dql, self.iw.ddl._create_connection())["id"][0]
            input_dir = Path(input_dir)
            version_dir = input_dir.parts[-1]
            prev_version_dir = re.sub("[-0-9]+", str(prev_version_id), version_dir)
            input_dir = input_dir.parent.joinpath(prev_version_dir)
            input_dir = str(input_dir)

        filename = set(
            [
                e.stem
                for e in Path(input_dir).glob("*.H5")
                if not e.stem.endswith("_collapsed_years")
            ]
        )

        # we dont need to combine the marginal_effect_value dataset
        if "marginal_effect_value" in filename:
            filename.remove("marginal_effect_value")

        assert len(filename) == 1, f"Unknown files: {filename}"
        filename = filename.pop()

        if collapse_years_switch:
            filename = f"{filename}_collapsed_years"
        versioned_df = pd.read_hdf(f"{input_dir}/{filename}.H5")

        # track when rows are added to the input data
        if config_key == "inj_cf_input":
            df["run_id"] = int(self.iw.run_id)

        # add or update rows from input df
        m = pd.merge(
            versioned_df,
            df,
            on=indexcols,
            how="outer",
            indicator=True,
            suffixes=["_old", "_new"],
        )

        assert all(m[m._merge == "left_only"]), "Merge failed"

        m = pd.concat(self._update_cf_input_data(m), sort=False)

        if "run_id" in m.columns:
            assert (
                m[m.run_id == int(self.iw.run_id)].shape[0] == df.shape[0]
            ), "Lost rows in update"

        assert (
            m[["location_id", "year_start"]].count().sum()
            >= df[["location_id", "year_start"]].count().sum()
        ), "Previous data was not correctly appended"

        return m

    def save_inj_cf(self, df, collapse_years_switch, indexcols):
        """Store the newly computed cf to disk and updated run_metadata and inj_cf_version tables"""
        if not self.update_dict["cf_version_id"]:
            self._update_rmdata_inj_cf_tbls()
            new_dir = self.iw.create_version_dir(
                config_key="inj_cf_en",
                version_id=self.update_dict["cf_version_id"],
                return_dir=True,
            )
            self.update_dict["cf_dir"] = new_dir

        self.pull_and_merge_previous_data(
            config_key="inj_cf_en",
            df=df,
            collapse_years_switch=collapse_years_switch,
            indexcols=indexcols,
        )

        suffix = ""
        if collapse_years_switch:
            suffix = "_collapsed_years"
        df.to_hdf(
            self.update_dict["cf_dir"] + f"/inj_factors{suffix}.H5",
            mode="w",
            format="table",
            key="key",
            data_columns=[
                "location_id",
                "year_start",
                "year_end",
                "nid",
                "facility_id",
            ],
        )

    def save_inj_cf_input(self, df_marg, df_input, collapse_years_switch):
        """Store updated dfs from calc_marg to disk"""

        # check to see if the new dir and cf input id row have already been created
        # if they have already been made then pull the current input path otherwise
        # create a new row DB and a new dir for the new
        # version_id
        if not self.update_dict["input_version_id"]:
            location_years = df_input[["location_id", "year_start"]].nunique().sum()
            name = "Updated for GBD 2021"
            description = f"{location_years} location years added or updated."
            self.iw.insert_into_inj_cf_input_version(
                inj_cf_input_version_name=name,
                inj_cf_input_version_description=description,
                run_id=self.iw.run_id,
            )
            # retrieve the new version_id that was just created
            dql = (
                QUERY
            )
            version_id = pd.read_sql(dql, self.iw.ddl._create_connection(),)[
                "id"
            ][0]
            self.update_dict["input_version_id"] = version_id
            new_dir = self.iw.create_version_dir(
                config_key="inj_cf_input", version_id=version_id, return_dir=True
            )
            self.update_dict["input_dir"] = new_dir

        suffix = ""
        if collapse_years_switch:
            suffix = "_collapsed_years"

        df_marg.to_hdf(
            self.update_dict["input_dir"] + f"/marginal_effect_value{suffix}.H5",
            key="df",
            format="table",
            complib="blosc",
            complevel=5,
            mode="w",
        )
        df_input.to_hdf(
            self.update_dict["input_dir"] + f"/multi_dx{suffix}.H5",
            key="df",
            format="table",
            complib="blosc",
            complevel=5,
            mode="w",
        )
