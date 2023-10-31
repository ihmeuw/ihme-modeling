from typing import List

import numpy as np
import pandas as pd

from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import ages, causes, engine_room, locations
from cod_prep.utils import (
    data_transforms as dt,
    print_log_message,
    expand_to_u5_age_detail,
    CodSchema,
    misc,
)

CONF = Configurator()


def remap_causes(df, remap_codes, target_dict):
    assert (df.loc[df.code_id.isin(remap_codes)].cause_id == 743).all(), \
        'Map has changed, leukemia adjustment should only affect garbage'
    for key in list(target_dict.keys()):
        df.loc[
            (df.code_id.isin(remap_codes)) &
            (df.age_group_id.isin(target_dict[key])),
            'code_id'
        ] = key
    return df


def adjust_leukemia_subtypes(df, code_system_id, code_map_version_id):
    cause_map = engine_room.get_cause_map(code_system_id, code_map_version_id=code_map_version_id,
                              force_rerun=False, block_rerun=True)
    cause_map = engine_room.remove_five_plus_digit_icd_codes(cause_map, code_system_id=code_system_id)
    ll_acute = int(cause_map.query("cause_id == 845").iloc[0]['code_id'])
    ll_chronic = int(cause_map.query("cause_id == 846").iloc[0]['code_id'])
    ml_acute = int(cause_map.query("cause_id == 847").iloc[0]['code_id'])
    other = int(cause_map.query("cause_id == 943").iloc[0]['code_id'])

    # splitting between 0-19 and 20+
    age_meta_df = ages.get_cod_ages(force_rerun=False, block_rerun=True)
    under_19 = age_meta_df.loc[age_meta_df.age_group_years_start < 20].age_group_id.tolist()
    twenty_plus = age_meta_df.loc[age_meta_df.age_group_years_start >= 20].age_group_id.tolist()

    # remapping
    if code_system_id == 1:
        df = remap_causes(df,
                          remap_codes=[2747],
                          target_dict={ll_acute: under_19, ll_chronic: twenty_plus}
                          )
        df = remap_causes(df,
                          remap_codes=[2756, 2760, 2768, 2769, 2770],
                          target_dict={ll_acute: under_19, other: twenty_plus}
                          )
        df = remap_causes(df,
                          remap_codes=[2804, 2803, 2805, 2818, 2824, 2826],
                          target_dict={ml_acute: under_19, other: twenty_plus}
                          )
    elif code_system_id == 6:
        df = remap_causes(df,
                          remap_codes=[50966],
                          target_dict={ll_acute: under_19, ll_chronic: twenty_plus}
                          )
        df = remap_causes(df,
                          remap_codes=[50974, 50975, 50979],
                          target_dict={ll_acute: under_19, other: twenty_plus}
                          )
        df = remap_causes(df,
                          remap_codes=[51000, 51017, 51021, 51004, 51025],
                          target_dict={ml_acute: under_19, other: twenty_plus}
                          )
    df.loc[df.code_id == ll_acute, 'cause_id'] = 845
    df.loc[df.code_id == ll_chronic, 'cause_id'] = 846
    df.loc[df.code_id == ml_acute, 'cause_id'] = 847
    df.loc[df.code_id == other, 'cause_id'] = 943
    return df

def adjust_dsp_liver_cancer(df, code_system_id, code_map_version_id):
    start_deaths = df.deaths.sum()
    start_columns = df.columns.tolist()
    liver_cancer_props = pd.read_csv(CONF.get_resource('china_dsp_liver_cancer_props'))
    cause_map = engine_room.get_cause_map(code_system_id=code_system_id,
        code_map_version_id=code_map_version_id, force_rerun=False, block_rerun=True)
    cause_meta = causes.get_current_cause_hierarchy(cause_set_id=CONF.get_id('cause_set'),
        cause_set_version_id=CONF.get_id('cause_set_version'), force_rerun=False, block_rerun=True)
    neo_liver_cause_id = cause_meta.loc[cause_meta.acause == 'neo_liver'].cause_id.iloc[0]
    neo_liver_code_id = cause_map.loc[cause_map.cause_id == neo_liver_cause_id].code_id.iloc[0]

    if code_system_id == 40:
        adjust_code = cause_map.loc[cause_map.value == '095'].code_id.iloc[0]
        liver_cancer_props['code_id'] = adjust_code
        df = df.merge(liver_cancer_props, on=['age_group_id', 'sex_id', 'code_id'], how='left')
        df.loc[df.prop.notnull(), 'deaths'] = df.deaths * df.prop
        df.loc[df.target == 'liver_cancer', 'code_id'] = neo_liver_code_id
        df.loc[df.target == 'liver_cancer', 'cause_id'] = neo_liver_cause_id

    elif code_system_id == 6:
        adjust_locations = [354, 361]
        adjust_code = cause_map.loc[cause_map.value == '155.2'].code_id.iloc[0]
        liver_cancer_props['code_id'] = adjust_code
        df = df.merge(liver_cancer_props, on=['age_group_id', 'sex_id', 'code_id'], how='left')
        df.loc[(df.prop.notnull()) & (df.location_id.isin(adjust_locations)), 'deaths'] = df.deaths * df.prop
        df.loc[df.target == 'liver_cancer', 'code_id'] = neo_liver_code_id
        df.loc[df.target == 'liver_cancer', 'cause_id'] = neo_liver_cause_id

    # check no deaths added/dropped, and return df to incoming columns
    assert np.allclose(start_deaths, df.deaths.sum()), \
        "Deaths added or dropped in China DSP liver cancer correction."
    df = df[start_columns]
    assert df.notnull().values.all()
    return df



def manually_reallocate_gbdcauses(df, code_map_version_id, **cache_kwargs):
    orig_deaths = df.deaths.sum()

    # reading in cleaned prop_df
    prop_df = pd.read_excel(CONF.get_resource('manual_gbdcause_reallocation'))
    prop_df = expand_to_u5_age_detail(prop_df)
    # patch digest_ibd children
    digest_ibd = prop_df.query('cause_id == 532')
    prop_df = pd.concat(
        [
            prop_df,
            # colitis
            digest_ibd.replace({'cause_id': {532: 1024}, 'target': {156952: 160790}}),
            # crohns
            digest_ibd.replace({'cause_id': {532: 1025}, 'target': {156952: 160791}}),
        ]
    )

    # merge prop_df onto existing data frame
    # changing code_ids to target and scaling deaths by prop where merge successful
    df = df.merge(prop_df, on=['sex_id', 'cause_id', 'age_group_id'], how='left', indicator=True)

    df.loc[df.target.notnull(), 'code_id'] = df['target']
    assert df.code_id.notnull().all()

    df.loc[df.prop.notnull(), 'deaths'] = df['deaths'] * df['prop']
    assert np.allclose(df.deaths.sum(), orig_deaths)

    # remap causes
    df = (
        df.pipe(
            dt.with_subset,
            "_merge == 'both'",
            lambda d: (
                d.drop(columns='cause_id')
                .merge(
                    engine_room.get_cause_map(code_map_version_id=code_map_version_id, **cache_kwargs)
                    .loc[:, ['code_id', 'cause_id']],
                    how='left',
                )
            )
        )
        .drop(columns=['target', 'prop', '_merge'])
    )

    assert df.notnull().all().all()

    return df


class PoliceConflictCorrector:
    police_conflict_cause_type = "police_conflict"
    cause_type_fallback_chain = ["homicide", "ill-defined", "X59", "fall", "mechanical"]

    def __init__(
        self, code_map_version_id: int, tolerance: float = 0.0, **cache_kwargs
    ):
        self.tolerance = tolerance
        cause_map = engine_room.get_cause_map(
            code_map_version_id=code_map_version_id,
            **cache_kwargs,
        )
        cause_meta_df = causes.get_current_cause_hierarchy(
            cause_set_version_id=CONF.get_id('cause_set_version'),
            **cache_kwargs
        )

        self.code_to_cause_type = self.get_code_to_cause_type(
            cause_map, cause_meta_df, **cache_kwargs
        )
        self.police_conflict_cause_id = cause_meta_df.set_index("acause")[
            "cause_id"
        ].loc["inj_war_execution"]
        self.police_conflict_code_id = cause_map.loc[
            lambda d: d["cause_id"] == self.police_conflict_cause_id, "code_id"
        ].iat[0]

        possible_cols = {
            "location_id",
            "year_id",
            "age_group_id",
            "sex_id",
            "population_group_id",
            "deaths_adjusted",
        }

        self.crosswalk = (
            pd.read_csv(
                CONF.get_resource("us_nvss_police_conflict_crosswalk"),
            )
            .query("sub_source == 'NVSS'")
            .pipe(lambda d: d.loc[:, set(d) & possible_cols])
            .rename(columns={"location_id": "state_id"})  # assume we're at state level
            .assign(cause_type=self.police_conflict_cause_type)
        )
        self.crosswalk_id_cols = CodSchema.infer_from_data(
            self.crosswalk,
            {
                "state_id": {"col_type": "demographic"},
                "cause_type": {"col_type": "cause"},
            },
        ).id_cols
        misc.report_duplicates(self.crosswalk, self.crosswalk_id_cols)

        self.location_to_state_id = (
            locations.get_current_location_hierarchy(
                location_set_version_id=CONF.get_id('location_set_version'),
                **cache_kwargs
            )
            .query('iso3 == "USA" and level > 3')
            .assign(
                state_id=lambda d: d.query("level >= 4")["path_to_top_parent"]
                .str.split(",", expand=True)[4]
                .astype(int)
            )
            .loc[:, ["location_id", "state_id"]]
        )

    @staticmethod
    def _get_package_code_ids(cause_map: pd.DataFrame, **cache_kwargs) -> pd.DataFrame:
        """Get all the code_ids in a package"""
        code_system_id = cause_map["code_system_id"].iat[0]
        cleaned_cause_map = (
            cause_map.loc[:, ["value", "code_id"]]
            .assign(
                value=lambda d: misc.clean_icd_codes(d["value"], remove_decimal=True)
            )
            .drop_duplicates("value")
        )
        return (
            # we want to use get_package_list rather than get_package_map
            # since we're before redistribution and packages may not have been
            # downloaded yet
            engine_room.get_package_list(
                code_system_id, include_garbage_codes=True, **cache_kwargs
            )
            .loc[:, ["package_name", "garbage_code"]]
            .pipe(
                engine_room.remove_five_plus_digit_icd_codes,
                "garbage_code",
                code_system_id=code_system_id,
            )
            .assign(value=lambda d: misc.clean_icd_codes(d.garbage_code, True))
            .drop_duplicates("value")
            .loc[:, ["package_name", "value"]]
            .merge(
                cleaned_cause_map.loc[:, ["value", "code_id"]],
                how="inner",
                validate="one_to_one",
            )
            .loc[:, ["package_name", "code_id"]]
        )

    @classmethod
    def _get_map_id_to_code_id(
        cls, cause_map: pd.DataFrame, cause_meta_df: pd.DataFrame, **cache_kwargs
    ) -> pd.DataFrame:
        return (
            cause_map.pipe(
                causes.add_cause_metadata, "acause", cause_meta_df=cause_meta_df
            )
            .query('acause != "_gc"')
            .rename(columns={"acause": "map_id"})
            .loc[:, ["map_id", "code_id"]]
            .append(
                cls._get_package_code_ids(cause_map, **cache_kwargs).rename(
                    columns={"package_name": "map_id"}
                ),
                sort=False,
            )
            # drop_duplicates because sometimes a code is erroneously both a real cause and garbage
            .drop_duplicates("code_id")
        )

    @classmethod
    def get_code_to_cause_type(
        cls, cause_map: pd.DataFrame, cause_meta_df: pd.DataFrame, **cache_kwargs
    ) -> pd.DataFrame:
        """Get a mapping from code_id to cause_type

        GBD causes in the cause_type_sources input file are expanded to their children
        before mapping each package name or acause to all the codes that map to it.
        """
        code_system_id = cause_map["code_system_id"].iat[0]
        return (
            pd.read_csv(
                CONF.get_resource(
                    "police_conflict_corrector_cause_type_sources"
                ).format(code_system_id=code_system_id)
            )
            .pipe(causes.expand_causes, cause_meta_df, cause_col="source")
            .merge(
                cls._get_map_id_to_code_id(
                    cause_map, cause_meta_df, **cache_kwargs
                ).rename(columns={"map_id": "source"}),
                how="inner",
                validate="one_to_many",
            )
            .loc[:, ["code_id", "cause_type"]]
        )

    def get_deficit(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_crosswalk = dt.filter_df(self.crosswalk, misc.get_constant_values(df))
        deficit = (
            # right join: allow the crosswalk to create observations
            df.merge(filtered_crosswalk, how="right", validate="one_to_one")
            .fillna({"deaths": 0})
            .eval("deaths_deficit = deaths_adjusted - deaths")
            .drop(columns=["deaths_adjusted", "deaths"])
        )
        negative_deficit = deficit.eval("deaths_deficit < -1e7")
        if negative_deficit.any():
            raise RuntimeError(
                "There are negative police conflict deficits"
                " the crosswalk may need to be reran.\n"
                f"{deficit.loc[negative_deficit, :]}"
            )
        return deficit

    def _get_weights_unchecked(self, df: pd.DataFrame, cause_type: str) -> pd.DataFrame:
        """Calculate movement weights using the current deficit and cause type

        Returns weights data frame with columns crosswalk_id_cols, cause_type,
        weight, and target_cause_type.
        """
        return (
            # inner join: while the crosswalk can create observations for
            # police conflict, we can only create movement weights
            # if we have deaths to draw from in the first place
            pd.merge(
                df.loc[lambda d: d["cause_type"] == cause_type, :],
                df.pipe(self.get_deficit).assign(cause_type=cause_type),
                validate="one_to_one",
            )
            # do the deficit correction math
            .assign(
                # can be <0 if we don't need to add deaths (don't need to do anything)
                # can be >1 if we can't add enough deaths (need to use fallback)
                weight=lambda d: np.clip(d.eval("deaths_deficit / deaths"), 0, 1),
                target_cause_type=self.police_conflict_cause_type,
            )
            .drop(columns=["deaths_deficit", "deaths"])
            .pipe(
                lambda d: d.append(
                    d.assign(
                        weight=1 - d["weight"],
                        target_cause_type=d["cause_type"],
                    )
                )
            )
        )

    def _apply_weights(self, df: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
        """
        Weight application specific to the target columns used in this class
        Action: crosswalk_id_cols, cause_type -> cause_type
        """
        return (
            df.merge(weights, how="left", on=self.crosswalk_id_cols + ["cause_type"])
            .fillna({"weight": 1})
            .eval("deaths = deaths * weight")
            .assign(cause_type=lambda d: d["target_cause_type"].fillna(d["cause_type"]))
            .drop(columns=["target_cause_type", "weight"])
        )

    @staticmethod
    def _collapse(df: pd.DataFrame, id_cols: List[str]) -> pd.DataFrame:
        return df.groupby(id_cols, as_index=False).agg({"deaths": sum})

    def _assert_no_deficit(self, df: pd.DataFrame) -> None:
        """Check there's no more deficit within the tolerance"""
        deficit = self.get_deficit(df)
        total_expected = dt.filter_df(self.crosswalk, misc.get_constant_values(df))[
            "deaths_adjusted"
        ].sum()
        percent_shortage = deficit["deaths_deficit"].sum() / total_expected
        if percent_shortage > self.tolerance:
            indices = deficit["deaths_deficit"] > 0
            raise RuntimeError(
                f"There remain {deficit['deaths_deficit'].sum()} "
                f"police conflict deaths to move ({percent_shortage * 100:.2f}%):\n"
                f"{deficit[indices].sort_values('deaths_deficit')}"
            )

    def get_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._collapse(df, self.crosswalk_id_cols)
        all_weights = []
        for cause_type in self.cause_type_fallback_chain:
            print_log_message(f"Getting movement weights for cause type {cause_type}")
            weights = self._get_weights_unchecked(df, cause_type)
            # use the weights to take deaths away from current cause type:
            # the next cause type in the fallback chain will then have an
            # up-to-date deficit with which to calculate its own weights
            df = df.pipe(self._apply_weights, weights).pipe(
                self._collapse, self.crosswalk_id_cols
            )
            all_weights.append(weights)

        self._assert_no_deficit(df)
        return pd.concat(all_weights, ignore_index=True)

    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add state_id and cause_type columns"""
        return (
            df.merge(self.location_to_state_id, how="left", validate="many_to_one")
            .merge(self.code_to_cause_type, how="left", validate="many_to_one")
            .assign(
                cause_type=lambda d: np.where(
                    d["cause_type"].isna()
                    & (d["cause_id"] == self.police_conflict_cause_id),
                    self.police_conflict_cause_type,
                    d["cause_type"],
                )
            )
        )

    def get_computed_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        print_log_message("Adjusting police conflict deaths for US NVSS")
        schema = CodSchema.infer_from_data(df)
        df = self.add_metadata(df)
        return (
            self._apply_weights(df, self.get_weights(df))
            .assign(
                cause_id=lambda d: np.where(
                    d["cause_type"] == self.police_conflict_cause_type,
                    self.police_conflict_cause_id,
                    d["cause_id"],
                ),
                code_id=lambda d: np.where(
                    d["cause_type"] == self.police_conflict_cause_type,
                    self.police_conflict_code_id,
                    d["code_id"],
                ),
            )
            .pipe(self._collapse, schema.id_cols)
        )
