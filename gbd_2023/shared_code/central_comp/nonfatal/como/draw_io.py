import os
from copy import deepcopy
from typing import Callable, Final, List, Tuple

import numpy as np
import pandas as pd

import get_draws.sources.epi as get_draws_epi
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_validation import check_no_duplicates
from gbd import wormhole as gbd_wormhole
from gbd.constants import columns, measures, metrics
from get_draws.base.exceptions import DrawException
from get_draws.base.formula import BaseFormula, mark_xform
from get_draws.transforms.wormhole_transforms import add_wormhole_draws
from ihme_dimensions import dfutils, gbdize

from como.legacy.common import apply_restrictions
from como.lib import constants, utils
from como.lib.version import ComoVersion

_UNUSED_COLUMNS: Final[List[str]] = [
    columns.METRIC_ID,
    columns.MODEL_VERSION_ID,
    "wormhole_model_version_id",
]


def square_data(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Square the data."""
    if df.empty:
        return df

    # collect draw source attributes
    filters = ds.content_kwargs["filters"].copy()
    model_version_id = ds.params["model_version_id"]
    modelable_entity_id = ds.params["modelable_entity_id"]
    dim = deepcopy(ds.params["dimensions"])

    # construct the dimensions object
    for key, val in list(filters.items()):
        dim.index_dim.replace_level(key, val)
    dim.index_dim.add_level("modelable_entity_id", [modelable_entity_id])
    dim.index_dim.add_level("model_version_id", [model_version_id])
    dim.index_dim.replace_level("year_id", df.year_id.unique().tolist())

    # fill in empty
    gbdizer = gbdize.GBDizeDataFrame(dim)
    df = gbdizer.add_missing_index_cols(df)
    df = gbdizer.fill_empty_indices(df, 0)
    return df


def downsample(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Downsample draws, if appropriate and possible."""
    if df.empty:
        return df

    df_draw_cols = [col for col in df.columns if "draw_" in col]
    ndraws_min = len(df_draw_cols)
    expected_draw_cols = ds.params["dimensions"].data_list()
    n_draws = len(expected_draw_cols)

    if n_draws > ndraws_min:
        raise DrawException(
            f"n_draws {n_draws} is greater than the minimum number "
            f"of draws in the data, {ndraws_min}. Upsampling of draws "
            "is not allowed."
        )

    df = dfutils.resample(df, n_draws, "draw_")
    return df


def attach_sequela_metadata_and_restrict(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Merge sequelae and apply cause restrictions."""
    if df.empty:
        return df
    sequela_list = ds.params["sequela_list"]
    dimensions = ds.params["dimensions"]
    cause_restrictions = ds.params["cause_restrictions"]
    df = df.merge(sequela_list, on="modelable_entity_id", how="left")
    df = apply_restrictions(cause_restrictions, df, dimensions.data_list())
    return df[
        ["sequela_id", "cause_id", "healthstate_id"]
        + dimensions.index_names
        + dimensions.data_list()
    ]


def restrict_cancer_incidence(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Sets cancer incidence to zero for certain MEs.

    Refer to docs re: why cancer team wants these incidences set to zero
    - 18780 - Mastectomy due to breast cancer, beyond 10 years
    - 18784 - Impotence due to prostate cancer, beyond 10 years
    - 18785 - Incontinence due to prostate cancer, beyond 10 years
    - 18788 - Laryngectomy due to larynx cancer, beyond 10 years
    - 18791 - Stoma due to colon and rectum cancers, beyond 10 years
    - 18794 - Incontinence due to bladder cancer, beyond 10 years
    """
    if df.empty:
        return df

    r_bool = (df.modelable_entity_id.isin([18794, 18785, 18784, 18791, 18788, 18780])) & (
        df.measure_id == measures.INCIDENCE
    )
    df.loc[r_bool, ds.params["dimensions"].data_list()] = 0
    return df


def attach_birth_metadata(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Merge in birth sequelae."""
    if df.empty:
        return df

    birth_sequela = ds.params["birth_prev"]
    dimensions = ds.params["dimensions"]
    df = df.merge(birth_sequela, on="modelable_entity_id", how="left")
    return df[["sequela_id", "cause_id"] + dimensions.index_names + dimensions.data_list()]


def attach_injuries_metadata_and_restrict(df: pd.DataFrame, ds: DrawSource) -> pd.DataFrame:
    """Merge in inuries and apply cause restrictions."""
    if df.empty:
        return df

    injury_sequela = ds.params["injury_sequela"]
    dimensions = ds.params["dimensions"]
    cause_restrictions = ds.params["cause_restrictions"]
    df = df.merge(injury_sequela, on="modelable_entity_id", how="left")
    df = apply_restrictions(cause_restrictions, df, dimensions.data_list())
    return df[
        ["modelable_entity_id", "cause_id", "rei_id", "sequela_id", "healthstate_id"]
        + dimensions.index_names
        + dimensions.data_list()
    ]


def attach_sexual_violence_metadata_and_restrict(
    df: pd.DataFrame, ds: DrawSource
) -> pd.DataFrame:
    """Add sexual violence cause_id and apply cause restrictions."""
    if df.empty:
        return df

    # cause_id 941 - Sexual violence
    df["cause_id"] = 941
    dimensions = ds.params["dimensions"]
    cause_restrictions = ds.params["cause_restrictions"]
    df = apply_restrictions(cause_restrictions, df, dimensions.data_list())
    df = df[["cause_id"] + dimensions.index_names + dimensions.data_list()]
    df = df.groupby(["cause_id"] + dimensions.index_names).sum()
    return df.reset_index()


def pre_wormhole_adjustments(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the following adjustments to the data before applying the Wormhole transform:
    1) Adds on metric_id.
    """
    if columns.METRIC_ID not in df.columns:
        df[columns.METRIC_ID] = metrics.RATE
    return df


def post_wormhole_adjustments(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the following adjustments to the data after applying the Wormhole transform:
    1) Removes unused columns, including columns added by the Wormhole transform.
    2) Fills the modelable entity ID column if present.
    3) Resets the DF index.
    """
    df = df.drop(_UNUSED_COLUMNS, axis=1, errors="ignore")
    if columns.MODELABLE_ENTITY_ID in df.columns:
        df[columns.MODELABLE_ENTITY_ID] = (
            df[columns.MODELABLE_ENTITY_ID].ffill().bfill().astype(int)
        )
    df = df.reset_index(drop=True)
    return df


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Raise an error if there are duplicate rows in the df."""
    non_draw_columns = [col for col in df.columns if not col.startswith("draw")]
    check_no_duplicates(df=df, cols=non_draw_columns)
    return df


def convert_small_numbers_to_zero(df: pd.DataFrame) -> pd.DataFrame:
    """Converts small numbers in draw columns to zero.
    This silences a warning from scipy's piecewise cubic interpolation when some
    draw values are very near, but not actually zero.
    """
    draw_cols = utils.ordered_draw_columns(df=df)
    vals = df[draw_cols].to_numpy()
    # Convert to zero if less than or equal to some very small number
    vals[np.isclose(vals, 0.0, rtol=0.0, atol=np.finfo(float).eps)] = 0.0
    df[draw_cols] = vals
    return df


class ComoFormula(BaseFormula):
    """COMO base formula for use with custom transforms."""

    def __init__(
        self,
        draw_source: DrawSource,
        release_id: int,
        best_release_id: int,
        version_id: int,
        diff_cache_dir: str,
    ):
        super().__init__(draw_source=draw_source)
        self.release_id = release_id
        self.best_release_id = best_release_id
        self.version_id = version_id
        self.diff_cache_dir = diff_cache_dir


class SequelaModelableEntityFormula(ComoFormula):
    """Formula with custom transforms."""

    @mark_xform(0)
    def xform_hazard_to_inc(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return get_draws_epi.convert_hazard_to_inc, (self.draw_source,), {}

    @mark_xform(1)
    def pre_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return pre_wormhole_adjustments, (), {}

    @mark_xform(2)
    def wormhole_draws(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource, int, int, int, int, str], dict]:
        """DrawSource transform."""
        return (
            add_wormhole_draws,
            (
                self.draw_source,
                self.release_id,
                self.best_release_id,
                self.version_id,
                gbd_wormhole.BASE_MODEL_TYPES.EPI,
                self.diff_cache_dir,
            ),
            {},
        )

    @mark_xform(3)
    def post_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return post_wormhole_adjustments, (), {}

    @mark_xform(4)
    def xform_downsample(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return downsample, (self.draw_source,), {}

    @mark_xform(5)
    def xform_square_data(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return square_data, (self.draw_source,), {}

    @mark_xform(6)
    def xform_restrict_cancer_incidence(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return restrict_cancer_incidence, (self.draw_source,), {}

    @mark_xform(7)
    def xform_attach_sequela_metadata_and_restrict(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return attach_sequela_metadata_and_restrict, (self.draw_source,), {}

    @mark_xform(8)
    def xform_check_duplicates(self) -> Tuple[Callable, Tuple, dict]:
        """Check for duplicates transform."""
        return check_duplicates, (), {}

    @mark_xform(9)
    def xform_convert_small_numbers_to_zero(self) -> Tuple[Callable, Tuple, dict]:
        """Convert small numbers to zero transform."""
        return convert_small_numbers_to_zero, (), {}


class BirthPrevModelableEntityFormula(ComoFormula):
    """Formula with custom transforms."""

    @mark_xform(0)
    def pre_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return pre_wormhole_adjustments, (), {}

    @mark_xform(1)
    def wormhole_draws(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource, int, int, int, int, str], dict]:
        """DrawSource transform."""
        return (
            add_wormhole_draws,
            (
                self.draw_source,
                self.release_id,
                self.best_release_id,
                self.version_id,
                gbd_wormhole.BASE_MODEL_TYPES.EPI,
                self.diff_cache_dir,
            ),
            {},
        )

    @mark_xform(2)
    def post_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return post_wormhole_adjustments, (), {}

    @mark_xform(3)
    def xform_downsample(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return downsample, (self.draw_source,), {}

    @mark_xform(4)
    def xform_square_data(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return square_data, (self.draw_source,), {}

    @mark_xform(5)
    def xform_attach_birth_metadata(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return attach_birth_metadata, (self.draw_source,), {}

    @mark_xform(6)
    def xform_check_duplicates(self) -> Tuple[Callable, Tuple, dict]:
        """Check for duplicates transform."""
        return check_duplicates, (), {}

    @mark_xform(7)
    def xform_convert_small_numbers_to_zero(self) -> Tuple[Callable, Tuple, dict]:
        """Convert small numbers to zero transform."""
        return convert_small_numbers_to_zero, (), {}


class InjuriesModelableEntityFormula(ComoFormula):
    """Formula with custom transforms."""

    @mark_xform(0)
    def pre_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return pre_wormhole_adjustments, (), {}

    @mark_xform(1)
    def wormhole_draws(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource, int, int, int, int, str], dict]:
        """DrawSource transform."""
        return (
            add_wormhole_draws,
            (
                self.draw_source,
                self.release_id,
                self.best_release_id,
                self.version_id,
                gbd_wormhole.BASE_MODEL_TYPES.EPI,
                self.diff_cache_dir,
            ),
            {},
        )

    @mark_xform(2)
    def post_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return post_wormhole_adjustments, (), {}

    @mark_xform(3)
    def xform_downsample(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return downsample, (self.draw_source,), {}

    @mark_xform(4)
    def xform_square_data(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return square_data, (self.draw_source,), {}

    @mark_xform(5)
    def attach_injuries_metadata_and_restrict(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return attach_injuries_metadata_and_restrict, (self.draw_source,), {}

    @mark_xform(6)
    def xform_check_duplicates(self) -> Tuple[Callable, Tuple, dict]:
        """Check for duplicates transform."""
        return check_duplicates, (), {}

    @mark_xform(7)
    def xform_convert_small_numbers_to_zero(self) -> Tuple[Callable, Tuple, dict]:
        """Convert small numbers to zero transform."""
        return convert_small_numbers_to_zero, (), {}


class SexualViolenceModelableEntityFormula(ComoFormula):
    """Formula with custom transforms."""

    @mark_xform(0)
    def pre_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return pre_wormhole_adjustments, (), {}

    @mark_xform(1)
    def wormhole_draws(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource, int, int, int, int, str], dict]:
        """DrawSource transform."""
        return (
            add_wormhole_draws,
            (
                self.draw_source,
                self.release_id,
                self.best_release_id,
                self.version_id,
                gbd_wormhole.BASE_MODEL_TYPES.EPI,
                self.diff_cache_dir,
            ),
            {},
        )

    @mark_xform(2)
    def post_wormhole_adjustments(self) -> Tuple[Callable, Tuple, dict]:
        """DrawSource transform."""
        return post_wormhole_adjustments, (), {}

    @mark_xform(3)
    def xform_downsample(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return downsample, (self.draw_source,), {}

    @mark_xform(4)
    def xform_square_data(self) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return square_data, (self.draw_source,), {}

    @mark_xform(5)
    def attach_sexual_violence_metadata_and_restrict(
        self,
    ) -> Tuple[Callable, Tuple[DrawSource], dict]:
        """DrawSource transform."""
        return (attach_sexual_violence_metadata_and_restrict, (self.draw_source,), {})

    @mark_xform(6)
    def xform_check_duplicates(self) -> Tuple[Callable, Tuple, dict]:
        """Check for duplicates transform."""
        return check_duplicates, (), {}

    @mark_xform(7)
    def xform_convert_small_numbers_to_zero(self) -> Tuple[Callable, Tuple, dict]:
        """Convert small numbers to zero transform."""
        return convert_small_numbers_to_zero, (), {}


class SourceSinkFactory:
    """General-purpose class for sourcing and sinking draws for a variety of components."""

    _file_pattern = "FILEPATH"

    def __init__(self, como_version: ComoVersion):
        self.como_version = como_version

    def get_params_by_component(self, component: str, source_type: str = "draws") -> dict:
        """Gets parameters as dictionary based on component and source_type."""
        if source_type not in ["draws", "inputs"]:
            raise ValueError(
                f"'source_type' must be either 'draws' or 'inputs'. Received {source_type}"
            )
        dim = self.como_version.nonfatal_dimensions
        if component != "sexual":
            dim = dim.get_dimension_by_component(component=component, measure_id=measures.YLD)
        else:
            dim = dim.get_dimension_by_component(
                component="cause", measure_id=measures.PREVALENCE
            )
        if source_type == "inputs":
            data_cols = dim.data_list() + dim.index_names
            index_cols = None
        else:
            data_cols = dim.data_list()
            index_cols = dim.index_names

        params = {
            "draw_dir": os.path.join(self.como_version.como_dir, source_type, component),
            "file_pattern": self._file_pattern,
            "data_cols": data_cols,
            "index_cols": index_cols,
        }
        return params

    def get_sequela_modelable_entity_source(
        self, modelable_entity_id: int, model_version_id: int, n_workers: int = 1
    ) -> DrawSource:
        """DrawSource getter."""
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {
            "sequela_list": self.como_version.sequela_list,
            "cause_restrictions": self.como_version.cause_restrictions,
            "dimensions": dim,
        }
        return self._get_modelable_entity_source(
            formula=SequelaModelableEntityFormula,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            release_id=self.como_version.release_id,
            diff_cache_dir=(
                "FILEPATH"
            ),
            extra_params=extra_params,
        )

    def get_birth_prev_modelable_entity_source(
        self, modelable_entity_id: int, model_version_id: int, n_workers: int = 1
    ) -> DrawSource:
        """DrawSource getter."""
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {"birth_prev": self.como_version.birth_prev, "dimensions": dim}
        return self._get_modelable_entity_source(
            formula=BirthPrevModelableEntityFormula,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            release_id=self.como_version.release_id,
            diff_cache_dir=(
                "FILEPATH"
            ),
            extra_params=extra_params,
        )

    def get_en_injuries_modelable_entity_source(
        self, modelable_entity_id: int, model_version_id: int, n_workers: int = 1
    ) -> DrawSource:
        """DrawSource getter."""
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {
            "injury_sequela": self.como_version.injury_sequela,
            "cause_restrictions": self.como_version.cause_restrictions,
            "dimensions": dim,
        }
        return self._get_modelable_entity_source(
            formula=InjuriesModelableEntityFormula,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            release_id=self.como_version.release_id,
            diff_cache_dir=(
                "FILEPATH"
            ),
            extra_params=extra_params,
        )

    def get_sexual_violence_modelable_entity_source(
        self, modelable_entity_id: int, model_version_id: int, n_workers: int = 1
    ) -> DrawSource:
        """DrawSource getter."""
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {
            "cause_restrictions": self.como_version.cause_restrictions,
            "dimensions": dim,
        }
        return self._get_modelable_entity_source(
            formula=SexualViolenceModelableEntityFormula,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            release_id=self.como_version.release_id,
            diff_cache_dir=(
                "FILEPATH"
            ),
            extra_params=extra_params,
        )

    @property
    def sequela_result_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("sequela", "draws"))

    @property
    def sequela_result_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("sequela", "draws"))

    @property
    def cause_result_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("cause", "draws"))

    @property
    def cause_result_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("cause", "draws"))

    @property
    def impairment_result_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("impairment", "draws"))

    @property
    def impairment_result_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("impairment", "draws"))

    @property
    def injuries_result_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("injuries", "draws"))

    @property
    def injuries_result_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("injuries", "draws"))

    @property
    def sequela_input_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("sequela", "inputs"))

    @property
    def sequela_input_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("sequela", "inputs"))

    @property
    def injuries_input_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("injuries", "inputs"))

    @property
    def injuries_input_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("injuries", "inputs"))

    @property
    def sexual_violence_input_source(self) -> DrawSource:
        """DrawSource property."""
        return DrawSource(self.get_params_by_component("sexual", "inputs"))

    @property
    def sexual_violence_input_sink(self) -> DrawSink:
        """DrawSink property."""
        return DrawSink(self.get_params_by_component("sexual", "inputs"))

    def _get_modelable_entity_source(
        self,
        formula: BaseFormula,
        modelable_entity_id: int,
        model_version_id: int,
        n_workers: int,
        release_id: int,
        diff_cache_dir: str,
        extra_params: dict,
    ) -> DrawSource:
        filters = {"modelable_entity_id": modelable_entity_id}
        get_draws_source = get_draws_epi.Epi(**filters)
        mvid_list = self.como_version.mvid_list
        try:
            best_release_id = mvid_list.loc[
                mvid_list["model_version_id"] == model_version_id, "release_id"
            ].values[0]
        except KeyError:
            best_release_id = release_id
        draw_source = get_draws_source.create_modelable_entity_draw_source(
            formula_instance=formula(
                draw_source=get_draws_source,
                release_id=release_id,
                best_release_id=best_release_id,
                version_id=model_version_id,
                diff_cache_dir=diff_cache_dir,
            ),
            n_workers=n_workers,
            modelable_entity_id=modelable_entity_id,
            release_id=release_id,
            model_version_id=model_version_id,
            diff_cache_dir=diff_cache_dir,
            skip_validation=True,
            best_release_id=best_release_id,
            extra_params=extra_params,
        )
        return draw_source
