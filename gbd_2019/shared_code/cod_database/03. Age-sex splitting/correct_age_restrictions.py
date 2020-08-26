# Purpose: Take age restricted deaths and move them somewhere specific
#   based on their age

import pandas as pd
import numpy as np

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.claude_io import Configurator
from cod_prep.downloaders import ages, causes, engine_room
from cod_prep.utils import misc


class AgeRestrictionsCorrector(CodProcess):
    standard_cache_options = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
    }

    allowed_code_system_ids = {1, 6}

    def __init__(self, code_system_id, cause_set_version_id):
        self.code_system_id = code_system_id

        misc.print_log_message("Getting metadata")
        CONF = Configurator()
        self.art_path = CONF.get_resource("age_restriction_targets")

        self.age_df = ages.get_cod_ages(**self.standard_cache_options)

        self.cause_meta_df = causes.get_current_cause_hierarchy(
            cause_set_version_id=cause_set_version_id,
            **self.standard_cache_options
        )

        if code_system_id in self.allowed_code_system_ids:
            misc.print_log_message(
                "Creating age restriction mapping dataframe")
            invalid_ages_df = self.get_invalid_ages_df()
            misc.print_log_message(
                "Creating for {}".format(
                    {1: "ICD10", 6: "ICD9_detail"}[code_system_id])
            )
            art_df = self.read_age_restriction_targets_df(code_system_id)
            self.art_mapping_df = self.get_age_restriction_target_mapping_df(
                art_df, invalid_ages_df
            )
        else:
            self.art_mapping_df = None

    def get_invalid_ages_df(self):
        """
        Returns a data frame specifying the age groups which
        lie outside of the allowed age ranges for the given cause.
        Has columns cause_id, age_group_id, and is_young_invalid,
        which specifies which side of the cause's age range the
        invalid age lies on.
        """
        new_rows = []
        for _, row in self.cause_meta_df.query("yld_only != 1").iterrows():
            # simple_age corresponds to the ages in the cause hierarchy
            invalid_start_ages = self.age_df[
                self.age_df.simple_age < row.yll_age_start
            ].age_group_id.tolist()
            invalid_end_ages = self.age_df[
                self.age_df.simple_age > row.yll_age_end
            ].age_group_id.tolist()
            new_rows.extend([(row.cause_id, age, True)
                             for age in invalid_start_ages])
            new_rows.extend([(row.cause_id, age, False)
                             for age in invalid_end_ages])

        invalid_ages_df = pd.DataFrame(
            new_rows, columns=["cause_id", "age_group_id", "is_young_invalid"]
        )
        return invalid_ages_df

    def expand_age_logic_to_age_group_id(self, df):
        assert "age_group" in df, \
            "Input data frame must contain an age group column specifying age logic"
        assert "age_group_id" not in df, \
            "Input data should not contain age_group_id"

        neonatal = [
            ["age < 1", x]
            for x in self.age_df.query("simple_age < 1").age_group_id
        ]
        preneonatal = [
            ["age == EN", x]
            for x in self.age_df.query("age_group_name_short == 'EN'").age_group_id
        ]
        over_45 = [
            ["age >= 45", x]
            for x in self.age_df.query("simple_age >= 45").age_group_id
        ]
        age_logic_to_id_df = pd.DataFrame(data=neonatal + preneonatal + over_45,
                                          columns=["age_group", "age_group_id"]
                                          )

        # some of age_group_id will be NaN (these will be the default mappings)
        return df.merge(age_logic_to_id_df, how="left")

    def get_code_id_for_package_name(self, df, code_system_id):
        assert "target_package" in df, \
            "Input data must have a 'target_package' column."

        cause_map = (
            engine_room.get_cause_map(
                code_system_id, **self.standard_cache_options)
            .loc[:, ["value", "code_id"]]
        )
        cause_map["value"] = misc.clean_icd_codes(cause_map.value, True)

        pkg = (
            engine_room.get_package_list(
                code_system_id,
                include_garbage_codes=True,
                **self.standard_cache_options)
            .pipe(
                engine_room.remove_five_plus_digit_icd_codes,
                "garbage_code",
                code_system_id=code_system_id)
            .assign(value=lambda d: misc.clean_icd_codes(d.garbage_code, True))
            .loc[:, ["package_name", "value"]]
            # pick first garbage code found in the package
            .drop_duplicates("package_name", keep="first")
            .merge(cause_map, how="left")
            .loc[:, ["package_name", "code_id"]]
        )

        df = df.merge(
            pkg, left_on="target_package", right_on="package_name", how="left"
        )

        # assert that we found a code for each package in target_package
        # where target_package is not NaN
        misc.report_if_merge_fail(
            df[df.target_package.notna()], "code_id", "target_package"
        )
        return df

    def read_age_restriction_targets_df(self, code_system_id):
        df = (
            pd.read_csv(self.art_path.format(code_system_id=code_system_id))
            .merge(
                self.cause_meta_df[["acause", "cause_id"]], how="left"
            )  # add cause_id for source
            .pipe(self.expand_age_logic_to_age_group_id)  # add age_group_id
            .pipe(self.get_code_id_for_package_name, code_system_id)  # add code_id
            .rename(columns={"code_id": "target_code_id"})
            # acause is NaN only when there is a GC in target_code_id
            .fillna({
                "target_acause": "_gc",
                "is_young_invalid": 0,
                "override_cause_hierarchy": 0,
                })
            .merge(
                self.cause_meta_df[["acause", "cause_id"]].rename(
                    columns={"acause": "target_acause",
                             "cause_id": "target_cause_id"}
                ),
                how="left",
            )
        )  # add cause_id for target

        misc.report_if_merge_fail(
            df, "cause_id", "acause"
        )

        misc.report_if_merge_fail(
            df, "target_cause_id", "target_acause"
        )

        return df[
            [
                "cause_id",
                "is_young_invalid",
                "override_cause_hierarchy",
                "age_group_id",
                "target_cause_id",
                "target_code_id",
            ]
        ]

    @staticmethod
    def get_age_restriction_target_mapping_df(art_df, invalid_ages_df):
        """
        Produces a dataframe that allows for getting the correct target causes
        and codes from input data.

        Args:
            art_df: the age restriction targets dataframe with cause_id,
                is_young_invalid, age_group_id, target_cause_id, and
                target_code_id columns. The age_group_id column can
                contain null values, which indicate the default mapping
                to use for a cause_id, is_young_invalid, age_group_id
                group.
            invalid_ages_df: the dataframe specifying the invalid age groups
                for each cause and whether the age group is invalid because
                it lies on the young or old end of the cause's age range.
                This provides the default age groups for cases where art_df
                null age_group_id.

        Returns:
            art_mapping_df: a dataframe ready for a left join onto
                data, which will label each invalid age-cause pair with
                the appropriate target_cause_id and target_code_id.
        """

        # Note: base_cols doesn't contain age_group_id
        base_cols = [
            "cause_id",
            "is_young_invalid",
            "override_cause_hierarchy",
            "target_cause_id",
            "target_code_id",
        ]
        index_cols = ["cause_id", "age_group_id"]
        default_idxs = art_df.age_group_id.isna()

        # basic procedure: get default values for unspecified age groups
        # (beyond invalid b/c young or b/c old) for each cause, then bring back
        # the data with specified age groups and use any data there instead
        # of the default.
        default_vals = art_df.loc[default_idxs, base_cols].merge(
            invalid_ages_df, on=["cause_id", "is_young_invalid"], how="left"
        )

        # choosing non-defaults is achieved with drop_duplicates
        art_mapping_df = (
            art_df.loc[~default_idxs, base_cols + ["age_group_id"]]
            .append(default_vals, sort=True)
            # choose specific over the default
            .drop_duplicates(index_cols + ["is_young_invalid"], keep="first")
        )

        final_mapping_df = art_mapping_df.drop_duplicates(index_cols, keep="first")

        assert len(final_mapping_df) == len(art_mapping_df), (
            "For some reason, there is an age group which is invalid because it is both "
            "too young and too old for a cause."
        )

        # The input data contains some cause-age pairs that are no longer restricted
        # (for example, noe_liver changed age start from 5 to 0). We do a join to
        # detect which cause-age pairs are actually invalid and then subset the data
        # to only include those and any overrides we have specified in the input.
        final_mapping_df = final_mapping_df.merge(
            invalid_ages_df[index_cols],
            on=index_cols,
            how="left",
            indicator=True,
        )
        final_mapping_df = final_mapping_df.loc[
            (final_mapping_df._merge == "both") | final_mapping_df.override_cause_hierarchy == 1,
            :
        ]

        assert not (
            final_mapping_df.target_cause_id.isna() & final_mapping_df.target_code_id.isna()
        ).any(), "Failed to parse the age restriction target data. " + \
            "Some rows don't have a cause_id or a code_id."

        out_cols = index_cols + ["target_cause_id", "target_code_id"]
        return final_mapping_df[out_cols]

    def get_diagnostic_dataframe(self):
        return self.art_mapping_df

    def get_computed_dataframe(self, df):
        if self.code_system_id not in self.allowed_code_system_ids:
            misc.print_log_message(
                "Skipping age restrictions for code_system_id: {}".format(
                    self.code_system_id
                )
            )
            return df

        deaths_before = df.deaths.sum()
        misc.print_log_message(
            "Deaths before age restriction mapping: {}".format(deaths_before)
        )

        in_cols = df.columns

        out_df = df.merge(
            self.art_mapping_df,
            on=["age_group_id", "cause_id"],
            how="left",
        )
        out_df["cause_id"].update(out_df.target_cause_id)
        out_df["code_id"].update(out_df.target_code_id)

        ar_deaths = out_df[
            out_df.target_cause_id.notna() | out_df.target_code_id.notna()
        ].deaths.sum()
        misc.print_log_message(
            "Total age restricted deaths found: {}".format(ar_deaths)
        )

        deaths_after = df.deaths.sum()
        misc.print_log_message(
            "Deaths after age restriction mapping: {}".format(deaths_after)
        )
        assert np.isclose(
            deaths_before, deaths_after
        ), ("Whoops, dropped or added some deaths. "
            "Before: {} After: {}".format(deaths_before, deaths_after)
            )
        return out_df[in_cols]
