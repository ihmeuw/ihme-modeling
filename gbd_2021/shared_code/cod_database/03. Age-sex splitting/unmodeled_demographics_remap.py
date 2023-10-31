import pandas as pd
import numpy as np

from cod_prep.claude.cod_process import CodProcess
from cod_prep.claude.claude_io import Configurator
from cod_prep.downloaders import ages, causes, engine_room
from cod_prep.utils import misc, CodSchema


"""Reassign causes for unmodeled ages and sexes

On the GBD, we have certain combinations of age/causes and sex/causes
which we do not model due to shortcomings in handling small sample sizes
on the GBD as well as lack of accurate input data. We do not change the age or sex
for these observations occur, but rather reassign their causes.

AgeRemap - works on age/causes
AgeSexRemap - works on age/causes and sex/causes
"""


class AgeRemap(CodProcess):
    """Remap unmodeled age / causes to specific causes and packages

    This method is more specific than the AgeSexRemap, and that class can be treated as a
    fallback for observations not handled here. We choose a package or GBD cause (as
    decided by expert opinion) based on:
    1) the cause of the incoming observation
    2) whether the observation falls below or above the GBD age threshold
    3) the specific age of the incoming observation

    Additionally, this class only modifies ICD9 and ICD10 data.
    """
    standard_cache_options = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
    }

    allowed_code_system_ids = {1, 6}

    def __init__(self, code_system_id, cause_set_version_id):
        self.code_system_id = code_system_id

        misc.print_log_message("Getting metadata")
        conf = Configurator()
        self.art_path = conf.get_resource("age_remap_targets")

        self.age_df = ages.get_cod_ages(**self.standard_cache_options)

        self.cause_meta_df = causes.get_current_cause_hierarchy(
            cause_set_version_id=cause_set_version_id, **self.standard_cache_options
        )

        self.cause_map = engine_room.get_cause_map(
            code_system_id, **self.standard_cache_options
        )

        if code_system_id in self.allowed_code_system_ids:
            misc.print_log_message("Creating age remap mapping dataframe")
            unmodeled_ages_df = self.get_unmodeled_ages_df()
            misc.print_log_message(
                "Creating for {}".format({1: "ICD10", 6: "ICD9_detail"}[code_system_id])
            )
            art_df = self.read_age_remap_targets_df(code_system_id)
            self.mapping_df = self.get_mapping_df(
                art_df, unmodeled_ages_df
            )
        else:
            self.mapping_df = None

    def get_unmodeled_ages_df(self):
        """
        Returns a data frame specifying the age groups which
        lie outside of the allowed age ranges for the given cause.
        Has columns cause_id, age_group_id, and is_unmodeled_young,
        which specifies which side of the cause's age range the
        unmodeled age lies on.
        """
        return (
            pd.merge(
                self.cause_meta_df.query("yld_only != 1")
                .loc[:, ["cause_id", "yll_age_start", "yll_age_end"]]
                .assign(tmp=1),
                self.age_df.loc[:, ["age_group_id", "simple_age"]].assign(tmp=1),
            )
            .query("simple_age < yll_age_start or simple_age > yll_age_end")
            .eval("is_unmodeled_young = simple_age < yll_age_start")
            .loc[:, ["cause_id", "age_group_id", "is_unmodeled_young"]]
        )

    def expand_age_logic_to_age_group_id(self, df):
        assert (
            "age_group" in df
        ), "Input data frame must contain an age group column specifying age logic"
        assert "age_group_id" not in df, "Input data should not contain age_group_id"

        neonatal = [
            ["age < 1", x] for x in self.age_df.query("simple_age < 1").age_group_id
        ]
        preneonatal = [
            ["age == EN", x]
            for x in self.age_df.query("age_group_name_short == 'EN'").age_group_id
        ]
        over_45 = [
            ["age >= 45", x] for x in self.age_df.query("simple_age >= 45").age_group_id
        ]
        age_logic_to_id_df = pd.DataFrame(
            data=neonatal + preneonatal + over_45, columns=["age_group", "age_group_id"]
        )

        return df.merge(age_logic_to_id_df, how="left")

    def get_package_to_code_id_map(self, code_system_id):
        cleaned_cause_map = self.cause_map.loc[:, ["value", "code_id"]].assign(
            value=lambda d: misc.clean_icd_codes(d["value"], remove_decimal=True)
        )

        return (
            engine_room.get_package_list(
                code_system_id,
                include_garbage_codes=True,
                **self.standard_cache_options
            )
            .pipe(
                engine_room.remove_five_plus_digit_icd_codes,
                "garbage_code",
                code_system_id=code_system_id,
            )
            # clean the ICD codes to match how garbage codes are matched against
            # code values
            .assign(value=lambda d: misc.clean_icd_codes(d.garbage_code, True))
            .loc[:, ["package_name", "value"]]
            # pick first garbage code found in the package
            .drop_duplicates("package_name", keep="first")
            .merge(cleaned_cause_map.loc[:, ["value", "code_id"]], how="left")
            .rename(
                columns={"package_name": "target_package", "code_id": "code_id_package"}
            )
            .loc[:, ["target_package", "code_id_package"]]
        )

    def get_acause_to_code_id_map(self, code_system_id):
        return (
            self.cause_map.pipe(
                causes.add_cause_metadata, "acause", cause_meta_df=self.cause_meta_df
            )
            .pipe(
                engine_room.remove_five_plus_digit_icd_codes,
                "value",
                code_system_id=code_system_id,
            )
            .drop_duplicates("acause", keep="first")
            .rename(columns={"acause": "target_acause", "code_id": "code_id_acause"})
            .loc[:, ["target_acause", "code_id_acause"]]
        )

    def read_age_remap_targets_df(self, code_system_id):
        df = (
            pd.read_csv(self.art_path.format(code_system_id=code_system_id))
            .fillna(
                {"is_unmodeled_young": 0, "override_cause_hierarchy": 0}, downcast="infer"
            )
            .merge(
                self.cause_meta_df[["acause", "cause_id"]], how="left"
            )  # add cause_id for source
            .pipe(self.expand_age_logic_to_age_group_id)  # add age_group_id
            .merge(self.get_acause_to_code_id_map(code_system_id), how="left")
            .merge(self.get_package_to_code_id_map(code_system_id), how="left")
            .assign(
                target_code_id=lambda d: d["code_id_acause"].fillna(
                    d["code_id_package"], downcast="infer"
                )
            )
            .merge(
                self.cause_map.loc[:, ["code_id", "cause_id"]].rename(
                    columns=lambda c: "target_" + c
                ),
                how="left",
            )
        )

        misc.report_if_merge_fail(df, "cause_id", "acause")
        misc.report_if_merge_fail(
            df, "target_code_id", ["target_acause", "target_package"]
        )
        misc.report_if_merge_fail(df, "target_cause_id", "target_code_id")

        return df[
            [
                "cause_id",
                "is_unmodeled_young",
                "override_cause_hierarchy",
                "age_group_id",
                "target_code_id",
                "target_cause_id",
            ]
        ]

    @staticmethod
    def get_mapping_df(art_df, unmodeled_ages_df):
        """
        Produces a dataframe that allows for getting the correct target causes
        and codes from input data.

        Args:
            art_df: the age remap targets dataframe with cause_id,
                is_unmodeled_young, age_group_id, target_cause_id, and
                target_code_id columns. The age_group_id column can
                contain null values, which indicates that the default age groups
                should be used for the cause_id / is_unmodeled_young group.
            unmodeled_ages_df: the dataframe specifying the unmodeled age groups
                for each cause and whether the age group is unmodeled because
                it lies on the young or old end of the cause's age range.
                This provides the default age groups for cases where art_df
                null age_group_id.

        Returns:
            art_mapping_df: a dataframe ready for a left join onto
                data, which will label each unmodeled age-cause pair with
                the appropriate target_cause_id and target_code_id.
        """

        # Note: base_cols doesn't contain age_group_id
        base_cols = [
            "cause_id",
            "is_unmodeled_young",
            "override_cause_hierarchy",
            "target_code_id",
            "target_cause_id",
        ]
        index_cols = ["cause_id", "age_group_id"]
        default_idxs = art_df.age_group_id.isna()

        # basic procedure: get default values for unspecified age groups
        # (beyond unmodeled b/c young or b/c old) for each cause, then bring back
        # the data with specified age groups and use any data there instead
        # of the default.
        default_vals = art_df.loc[default_idxs, base_cols].merge(
            unmodeled_ages_df, on=["cause_id", "is_unmodeled_young"], how="inner"
        )

        # choosing non-defaults is achieved with drop_duplicates
        art_mapping_df = (
            art_df.loc[~default_idxs, base_cols + ["age_group_id"]]
            .append(default_vals, sort=True)
            .astype({"age_group_id": int})
            # choose specific over the default
            .drop_duplicates(index_cols + ["is_unmodeled_young"], keep="first")
        )

        final_mapping_df = art_mapping_df.drop_duplicates(index_cols, keep="first")

        assert len(final_mapping_df) == len(art_mapping_df), (
            "For some reason, there is an age group which is unmodeled because it is both "
            "too young and too old for a cause."
        )

        # The input data contains some cause-age pairs that are now newly being modeled
        # (for example, noe_liver changed age start from 5 to 0). We do a join to
        # detect which cause-age pairs are currently unmodeled and then subset the data
        # to only include those and any overrides we have specified in the input.
        final_mapping_df = final_mapping_df.merge(
            unmodeled_ages_df[index_cols], on=index_cols, how="left", indicator=True
        ).query("_merge == 'both' or override_cause_hierarchy == 1")

        misc.report_if_merge_fail(final_mapping_df, "target_code_id", index_cols)

        return final_mapping_df.loc[
            :, index_cols + ["target_code_id", "target_cause_id"]
        ]

    def get_diagnostic_dataframe(self):
        return self.mapping_df

    def get_computed_dataframe(self, df):
        if self.code_system_id not in self.allowed_code_system_ids:
            misc.print_log_message(
                f"Skipping age remap for code_system_id: {self.code_system_id}"
            )
            return df

        deaths_before = df.deaths.sum()
        misc.print_log_message(
            f"Deaths before age remap: {deaths_before}"
        )

        in_cols = list(df)

        out_df = df.merge(
            self.mapping_df, on=["age_group_id", "cause_id"], how="left"
        )
        out_df["cause_id"].update(out_df["target_cause_id"])
        out_df["code_id"].update(out_df["target_code_id"])

        ar_deaths = out_df[
            out_df.target_cause_id.notna() | out_df.target_code_id.notna()
        ].deaths.sum()
        misc.print_log_message(
            f"Total deaths found to remap: {ar_deaths}"
        )

        deaths_after = df.deaths.sum()
        misc.print_log_message(
            f"Deaths after age remap: {deaths_after}"
        )
        assert np.isclose(deaths_before, deaths_after), (
            "Whoops, dropped or added some deaths. "
            f"Before: {deaths_before} After: {deaths_after}"
        )
        return out_df[in_cols]


class AgeSexRemap(CodProcess):
    """Remap unmodeled age/sex/causes to other causes and ZZZ

    For observations with unmodeled age/causes and sex/causes, we reassign the cause
    to a related ICD code (for some codes in ICD9 and ICD10) or ZZZ for other codes
    in ICD9 and ICD10 as well as code systems.
    """
    standard_cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_results': False,
        'cache_dir': "standard"
    }

    def __init__(self, code_system_id, cause_set_version_id, collect_diagnostics=False, verbose=False,
                 column_metadata=None):
        self.code_system_id = code_system_id
        self.cause_set_version_id = cause_set_version_id
        self.collect_diagnostics = collect_diagnostics
        self.verbose = verbose
        self.column_metadata = column_metadata

    def get_computed_dataframe(self, df):
        cause_meta_df = self.get_age_sex_metadata_for_causes(self.cause_set_version_id)

        # run the more code system specific remaps
        if self.verbose:
            misc.print_log_message("Assigning cause remap target")
        df = self.assign_remap_target_cause(df)

        if self.verbose:
            misc.print_log_message("Marking unmodeled age / sex / causes")
        df = self.assign_unmodeled_flag(df, cause_meta_df)

        # good place to save diagnostics
        if self.collect_diagnostics:
            self.diag_df = df.copy()

        # Remapping the cause
        if self.verbose:
            misc.print_log_message("Remapping cause")

        df.loc[df.unmodeled == 1, 'code_id'] = df['remap_target_code_id']
        df.loc[df.unmodeled == 1, 'cause_id'] = df['remap_target_cause_id']

        # then collapse
        if self.verbose:
            misc.print_log_message("Collapsing")
        df.drop(
            columns=[
               "male",
               "female",
               "unmodeled",
               "remap_target_cause_id",
               "remap_target_code_id",
               "simple_age",
               "yll_age_start",
               "yll_age_end",
               "raw_cause",
               "remap_target_cause",
               "numeric_cause",
               "acause",
            ],
            inplace=True,
            errors="ignore",
        )
        schema = CodSchema.infer_from_data(df, metadata=self.column_metadata)
        df = df.groupby(schema.id_cols, as_index=False)[schema.value_cols].sum()

        return df

    def get_diagnostic_dataframe(self):
        if self.diag_df is None:
            raise AssertionError(
                "Need to run get_computed_dataframe once "
                "with collect_diagnostics = True")
        return self.diag_df

    def get_age_sex_metadata_for_causes(self, cause_set_version_id):
        df = causes.get_current_cause_hierarchy(
            cause_set_version_id=cause_set_version_id,
            **self.standard_cache_options
        )
        df = df[['acause', 'cause_id', 'male',
                 'female', 'yll_age_start', 'yll_age_end']]
        return df

    def prep_code_metadata(self):
        df = engine_room.get_cause_map(
            self.code_system_id,
            **self.standard_cache_options
        )
        df = df[['code_id', 'value', 'cause_id']]
        df = df.rename(columns={'value': 'raw_cause'})
        return df

    def assign_remap_target_cause(self, df):
        """Run a set of manual replacements, according to expert opinion."""

        # based on first letter of icd code, certain values should be filled in
        mapping_icd10 = {'A': 'B99.9', 'B': 'B99.9', 'C': 'D49.9',
                         'D': 'D49.9', 'I': 'I99.9', 'J': 'J98.9',
                         'K': 'K92.9', 'V': 'Y89', 'Y': 'Y89'}

        # add value field
        df = engine_room.add_code_metadata(
            df, ['value'], self.code_system_id,
            **self.standard_cache_options
        )
        misc.report_if_merge_fail(df, 'value', 'code_id')
        df = df.rename(columns={'value': 'raw_cause'})

        # generate new column called "cause_remap_target"
        # ZZZ is the default for all code systems
        raw_causes = self.prep_code_metadata()
        assert "ZZZ" in raw_causes.raw_cause.unique(), \
            "ZZZ must be in the map"
        df['remap_target_cause'] = "ZZZ"
        df['remap_target_code_id'] = raw_causes.query(
            "raw_cause == 'ZZZ'")["code_id"].values[0]
        df['remap_target_cause_id'] = raw_causes.query(
            "raw_cause == 'ZZZ'")["cause_id"].values[0]

        if self.code_system_id == 1:
            for key in list(mapping_icd10.keys()):
                raw_cause = mapping_icd10[key]
                code_list = raw_causes.query(
                    "raw_cause == '{}'".format(raw_cause))
                assert len(code_list) == 1,  \
                    "Found more than one code with value {} in code " \
                    "system {}".format(raw_cause, self.code_system_id)
                new_code_id = code_list['code_id'].iloc[0]
                new_cause_id = code_list['cause_id'].iloc[0]
                df.loc[df['raw_cause'].str.startswith(key),
                       ['remap_target_cause', 'remap_target_code_id',
                        'remap_target_cause_id']] = [raw_cause,
                                                   new_code_id, new_cause_id]

            code_list = raw_causes.query('raw_cause == "acause_diarrhea"')
            assert len(code_list) == 1,  \
                "Found more than one code with value {} in code " \
                "system {}".format(raw_cause, self.code_system_id)
            new_code_id = code_list['code_id'].iloc[0]
            new_cause_id = code_list['cause_id'].iloc[0]
            df.loc[df['cause_id'] == 532,
                   ['remap_target_cause']] = raw_cause
            df.loc[df['cause_id'] == 532,
                   ['remap_target_code_id']] = new_code_id
            df.loc[df['cause_id'] == 532,
                   ['remap_target_cause_id']] = new_cause_id
            df.loc[df['cause_id'] == 533,
                   ['remap_target_cause']] = raw_cause
            df.loc[df['cause_id'] == 533,
                   ['remap_target_code_id']] = new_code_id
            df.loc[df['cause_id'] == 533,
                   ['remap_target_cause_id']] = new_cause_id

        if self.code_system_id == 6:
            df['numeric_cause'] = pd.to_numeric(
                df['raw_cause'], errors='coerce')

            new_code_id = raw_causes.query(
                "raw_cause == '139.8'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '139.8'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 1) & (df.numeric_cause < 140),
                ['remap_target_cause', 'remap_target_code_id',
                 'remap_target_cause_id']
            ] = "139.8", new_code_id, new_cause_id


            new_code_id = raw_causes.query(
                "raw_cause == '239.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '239.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 140) & (df.numeric_cause < 240),
                ['remap_target_cause', 'remap_target_code_id',
                 'remap_target_cause_id']
            ] = "239.9", new_code_id, new_cause_id


            new_code_id = raw_causes.query(
                "raw_cause == '459.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '459.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 390) & (df.numeric_cause < 460),
                ['remap_target_cause', 'remap_target_code_id',
                 'remap_target_cause_id']
            ] = "459.9", new_code_id, new_cause_id


            new_code_id = raw_causes.query(
                "raw_cause == '519.9'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '519.9'")["cause_id"].values[0]
            df.loc[
                (df.numeric_cause >= 460) & (df.numeric_cause < 520),
                ['remap_target_cause', 'remap_target_code_id',
                 'remap_target_cause_id']
            ] = "519.9", new_code_id, new_cause_id

            new_code_id = raw_causes.query(
                "raw_cause == '578'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == '578'")["cause_id"].values[0]
            df.loc[(df.numeric_cause >= 520) & (df.numeric_cause < 580),
                   ['remap_target_cause', 'remap_target_code_id',
                    'remap_target_cause_id']] = "578", new_code_id, new_cause_id

            new_code_id = raw_causes.query(
                "raw_cause == 'E989'")["code_id"].values[0]
            new_cause_id = raw_causes.query(
                "raw_cause == 'E989'")["cause_id"].values[0]
            df.loc[df['raw_cause'].str.startswith("E"),
                   ['remap_target_cause', 'remap_target_code_id',
                    'remap_target_cause_id']] = "E989", new_code_id, new_cause_id
        assert pd.notnull(df.remap_target_code_id).all()
        assert pd.notnull(df.remap_target_cause_id).all()
        return df

    def assign_unmodeled_flag(self, df, cause_meta_df):
        """Assign an unmodeled flag for observations that aren't modeled on the GBD"""
        df = df.merge(cause_meta_df, on='cause_id')

        df = ages.add_age_metadata(
            df,
            'simple_age',
            **self.standard_cache_options
        )
        df.fillna({'yll_age_start': 0, 'yll_age_end': 95, 'female': 1, 'male': 1}, inplace=True)


        offset = .0001
        unmodeled_male = (df.sex_id == 1) & (df.male == 0)
        unmodeled_female = (df.sex_id == 2) & (df.female == 0)
        unmodeled_young = df.simple_age + offset < df.yll_age_start
        unmodeled_old = df.simple_age - offset > df.yll_age_end
        all_unmodeled = unmodeled_male | unmodeled_female | unmodeled_young | unmodeled_old

        df.loc[all_unmodeled, 'unmodeled'] = 1

        return df
