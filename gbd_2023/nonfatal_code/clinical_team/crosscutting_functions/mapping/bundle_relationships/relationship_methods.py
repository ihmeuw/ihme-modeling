"""Defines all the methods for bundle-to-bundle relationships."""

import warnings

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    ClaimsWrappers,
    InpatientWrappers,
    OutpatientWrappers,
)

from crosscutting_functions.mapping.bundle_relationships import relationship_tables


class RelationshipProcess:
    """Parent class to define and store bundle-to-bundle relationship data.

    Attributes:
        run_id: Used to pull a pipeline wrapper.
        clinical_data_type_id: Proxy for pipeline, used to pull a pipeline wrappers.
        multiple_ids: Does the relationship require more than a 1:1 mapping?
        relationship_id: An ID corresponding to the type of bundle relationship. Hardcoded
                         in child classes for maternal ratios, parent injuries and clones.
        pipeline_wrapper: A metadata pipeline wrapper.
        run_dtype_metadata: The run_metadata for the input run and clinical data type id.
    """

    def __init__(
        self,
        run_id: int,
        clinical_data_type_id: int,
        multiple_ids: bool,
        relationship_id: int,
        odbc_profile: str = "DATABASE",
    ):
        self.run_id = run_id
        self.clinical_data_type_id = clinical_data_type_id
        self.multiple_ids = multiple_ids
        self.relationship_id = relationship_id
        self.odbc_profile = odbc_profile

        self.get_ddl_wrapper
        self.run_dtype_metadata = self.ddl_wrapper.pull_run_metadata()

    @property
    def get_ddl_wrapper(self):
        if self.clinical_data_type_id == 1:
            self.ddl_wrapper = InpatientWrappers(
                run_id=self.run_id, odbc_profile=self.odbc_profile
            )
        elif self.clinical_data_type_id == 2:
            self.ddl_wrapper = OutpatientWrappers(
                run_id=self.run_id, odbc_profile=self.odbc_profile
            )
        elif self.clinical_data_type_id in [3, 5]:
            self.ddl_wrapper = ClaimsWrappers(
                run_id=self.run_id, odbc_profile=self.odbc_profile
            )
        else:
            raise ValueError(f"type id {self.clinical_data_type_id} is not supported")

    def get_tables(self):
        """Assigns the relationship table data to 2 class attributes.

        Attributes:
            bundle_relationship: Table of the bundle-to-bundle relationships.
            relationship_list: The relationship lookup table for relationship_id.
        """
        (self.bundle_relationship, self.relationship_list) = relationship_tables.get_tables()
        self._validate_relationship_id()
        self._filter_br_table()

    def _filter_br_table(self):
        """Remove the bundles that map to any other relationship_ids."""

        self._run_info()
        self.bundle_relationship = self.bundle_relationship.query(
            f"map_version == {self.map_version}"
        )
        if self.multiple_ids:
            mask = "self.bundle_relationship['relationship_id'].isin(self.relationship_id)"
        else:
            mask = "self.bundle_relationship['relationship_id'] == self.relationship_id"
        self.bundle_relationship = self.bundle_relationship[eval(mask)]
        if len(self.bundle_relationship) == 0:
            warnings.warn("There's no bundle-relationship data")

    def _validate_relationship_id(self):
        """Confirm that the relationship_id attribute is present in the lookup table."""
        ids = self.relationship_list["relationship_id"].unique().tolist()
        if self.multiple_ids:
            for id in self.relationship_id:
                if id not in ids:
                    raise ValueError(f"The relationship_id {id} has no lookup value.")
        else:
            if self.relationship_id not in ids:
                raise ValueError(
                    f"The relationship_id {self.relationship_id} has no lookup value."
                )

    def _run_info(self):
        """Pull in some of the settings we use in a given run.
        Note, removed decomp steps and gbd round id."""
        self.map_version = self.run_dtype_metadata["map_version"].item()


class MaternalRatios(RelationshipProcess):
    """Child class that stores maternal ratio specific data and methods."""

    def __init__(self, run_id, clinical_data_type_id):
        RelationshipProcess.__init__(
            self,
            run_id=run_id,
            clinical_data_type_id=clinical_data_type_id,
            multiple_ids=True,
            relationship_id=(1, 2),
        )

    def _validate_mapping(self):
        """Make sure there are only 2 input bundles for each 1 output bundle
        otherwise some of the logic will be incorrect."""
        for out in self.bundle_relationship["output_bundle_id"].unique():
            origin_bundles = self.bundle_relationship.loc[
                self.bundle_relationship["output_bundle_id"] == out, "origin_bundle_id"
            ]
            if len(origin_bundles) != 2:
                raise ValueError(f"Exactly 2 origin bundles expected {origin_bundles}")

    def _validate_position(self, inputs, ratio):
        """Just in case the ratio code which is index based performs oddly."""
        numer = inputs[0]
        denom = inputs[1]
        valid_numer = (
            1
            in self.bundle_relationship.query(
                f"origin_bundle_id == {numer}"
            ).relationship_id.tolist()
        )
        valid_denom = (
            2
            in self.bundle_relationship.query(
                f"origin_bundle_id == {denom}"
            ).relationship_id.tolist()
        )
        if not valid_numer:
            raise ValueError(
                (
                    f"The numeratorID {numer} for ratio bundle "
                    f"{ratio} is wrong. Review the tuple conversion"
                )
            )
        if not valid_denom:
            raise ValueError(
                (
                    f"The denominatorID {denom} for ratio bundle "
                    f"{ratio} is wrong. Review the tuple conversion"
                )
            )

    def convert_to_tuple_dict(self):
        """Convert the dataframe relationship to a dictionary of ratio:tuples
        which matches the method developed in uploader."""

        # This sort is REALLY important for the ratio method. It aligns the
        # numerator and denominator in the tuple correctly.
        self.get_tables()

        sortorder = ["relationship_update", "output_bundle_id", "relationship_id"]
        self.bundle_relationship = self.bundle_relationship.sort_values(sortorder)

        tmp = self.bundle_relationship.groupby("output_bundle_id").origin_bundle_id.unique()

        ratios = {}
        for ratio, inputs in tmp.iteritems():
            self._validate_position(inputs, ratio)
            ratios[ratio] = tuple(inputs)
        self.ratios = ratios

    def create_maternal_ratios(self):
        """the uploader calls MaternalRatioBuilder.builder and passes self.df
        and the ratio tuple to the it. """
        pass


class ParentInjuries(RelationshipProcess):
    """Child class that stores parent injury bundle specific relationship data and methods."""

    def __init__(self, run_id, clinical_data_type_id):
        RelationshipProcess.__init__(
            self,
            run_id=run_id,
            clinical_data_type_id=clinical_data_type_id,
            multiple_ids=False,
            relationship_id=3,
        )

    def append_parent_bundle(self, df):
        """Create the parent inj bundles by aggregating the children."""

        # All columns that aren't draws, mean or denoms
        groupby_columns = [
            "bundle_id",
            "location_id",
            "year_start",
            "year_end",
            "age_group_id",
            "sex_id",
            "nid",
            "representative_id",
            "estimate_id",
            "years",
            "source",
            "use_draws",
        ]
        draws = df.filter(regex="^draw", axis=1).columns.tolist()
        populations = ["maternal_sample_size", "population", "sample_size"]
        agg_dict = {0: {"mean": "sum"}, 1: dict.fromkeys(draws, "sum")}
        # This is now the relationship table
        self.get_tables()
        inj_map = self.bundle_relationship
        inj_map = inj_map[
            (inj_map.relationship_id == 3) & (inj_map.map_version == max(inj_map.map_version))
        ][["origin_bundle_id", "output_bundle_id"]]
        inj_bids = inj_map.origin_bundle_id.unique().tolist()

        # subset the input df to only child inj bundles
        df_inj = df[df.bundle_id.isin(inj_bids)]

        # df_inj should only be empty when running with sparse data (most likely with U1 ages)
        if df_inj.empty:
            print("No child injuries")
            return df
        df_inj = df_inj.merge(inj_map, left_on="bundle_id", right_on="origin_bundle_id")
        df_inj.drop(["bundle_id", "origin_bundle_id"], axis=1, inplace=True)

        # replace the child inj bundle ids with the parent inj bundles
        df_inj.rename({"output_bundle_id": "bundle_id"}, axis=1, inplace=True)

        use_draw_vals = df_inj.use_draws.unique().tolist()
        parent_dfs = []
        for e in use_draw_vals:
            temp = df_inj[df_inj.use_draws == e]
            if e == 1:
                # verify that there is only one population for a given location / year
                if not all(
                    temp.groupby(["location_id", "year_start"]).nunique()[populations] == 1
                ):
                    raise ValueError(
                        "There is more than one population value for a given location/year."
                    )
            temp = (
                temp.groupby(groupby_columns + populations, dropna=False)
                .agg(agg_dict[e])
                .reset_index()
            )
            parent_dfs.append(temp)

        parent_inj = pd.concat(parent_dfs, sort=False)
        result = pd.concat([df, parent_inj], sort=False)

        return result


class BundleClone(RelationshipProcess):
    """Child class that stores bundle cloning specific data and methods.

    Clones data using the bundle_relationship data, assigns it to a class attribute.

    NOTE this allows df to be whatever, could be from a different run_id than
    the map version!
    Example Usage:
        bundle_clone = BundleClone(run_id=23)
        bundle_clone.clone_data(df=df)
    Then the cloned data is in -- bundle_clone.cloned_df.
    Or you can just clone and return the df.
    Example:
        df = bundle_clone.clone_and_return(df)
    """

    def __init__(self, run_id, clinical_data_type_id):
        RelationshipProcess.__init__(
            self,
            run_id=run_id,
            clinical_data_type_id=clinical_data_type_id,
            multiple_ids=False,
            relationship_id=4,
        )

    def clone_data(self, df):
        """Given a dataframe and instantiated with a run id this method will
        make a copy of any bundles in the cloned relationship."""
        self.get_tables()

        if len(self.bundle_relationship) == 0:
            print(f"There are no bundles to clone for run {self.run_id}")
            return df

        clone_dict = self.bundle_relationship.set_index("origin_bundle_id")[
            "output_bundle_id"
        ].to_dict()
        if self.bundle_relationship["output_bundle_id"].duplicated().any():
            raise ValueError("this will overwrite an output value")

        exp_rows = len(df)
        tmp_list = []
        for origin, output in clone_dict.items():
            tmp = df[df["bundle_id"] == origin].copy()
            exp_rows += len(tmp)
            tmp["bundle_id"] = output
            tmp_list.append(tmp)
            del tmp

        if tmp_list:
            self.cloned_df = pd.concat(tmp_list, sort=False, ignore_index=True)
            output_diff = set(self.cloned_df["bundle_id"].unique()).symmetric_difference(
                clone_dict.values()
            )
            if output_diff:
                warnings.warn(f"These bundles didn't get cloned-{output_diff}")
        else:
            warnings.warn("None of the data was cloned. Was this expected?")
        if exp_rows != len(self.cloned_df) + len(df):
            raise ValueError(
                f"The rows for existing data ({len(df)} rows) plus cloned "
                f"data ({len(self.cloned_df)} rows) do not match what we "
                f"expect ({exp_rows} rows)."
            )

    def clone_and_return(self, df):
        """Given a dataframe clones the data to an attr and then concat and
        return."""

        self.clone_data(df)

        df = pd.concat([df, self.cloned_df], sort=False, ignore_index=True)
        return df
