import os
import itertools
from loguru import logger
from copy import deepcopy
import numpy as np

from dataframe_io.exceptions import InvalidSpec
from draw_sources.draw_sources import DrawSink, DrawSource
from gbd.constants import measures
from gbd.decomp_step import decomp_step_from_decomp_step_id
from gbd_artifacts.artifact import Artifact
from get_draws.base.exceptions import DrawException
from get_draws.base.formula import BaseFormula, mark_xform
from get_draws.sources.epi import convert_hazard_to_inc
from ihme_dimensions import gbdize, dfutils

from como.legacy.common import apply_restrictions


def square_data(df, ds):
    if df.empty:
        return df

    filters = ds.content_kwargs["filters"].copy()
    model_version_id = ds.params["model_version_id"]
    modelable_entity_id = ds.params["modelable_entity_id"]
    dim = deepcopy(ds.params["dimensions"])

    for key, val in list(filters.items()):
        dim.index_dim.replace_level(key, val)
    dim.index_dim.add_level("modelable_entity_id", [modelable_entity_id])
    dim.index_dim.add_level("model_version_id", [model_version_id])
    dim.index_dim.replace_level("year_id", df.year_id.unique().tolist())

    gbdizer = gbdize.GBDizeDataFrame(dim)
    df = gbdizer.add_missing_index_cols(df)
    df = gbdizer.fill_empty_indices(df, 0)
    return df


def downsample(df, ds):
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


def attach_sequela_metadata_and_restict(df, ds):
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


def restrict_cancer_incidence(df, ds):
    if df.empty:
        return df

    r_bool = (df.modelable_entity_id.isin([18794, 18785, 18784, 18791, 18788, 18780])) & (
        df.measure_id == measures.INCIDENCE
    )
    df.loc[r_bool, ds.params["dimensions"].data_list()] = 0
    return df


def attach_birth_metadata(df, ds):
    if df.empty:
        return df

    birth_sequela = ds.params["birth_prev"]
    dimensions = ds.params["dimensions"]
    df = df.merge(birth_sequela, on="modelable_entity_id", how="left")
    return df[["sequela_id", "cause_id"] + dimensions.index_names + dimensions.data_list()]


def attach_injuries_metadata_and_restict(df, ds):
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


def attach_sexual_violence_metadata_and_restrict(df, ds):
    if df.empty:
        return df

    df["cause_id"] = 941
    dimensions = ds.params["dimensions"]
    cause_restrictions = ds.params["cause_restrictions"]
    df = apply_restrictions(cause_restrictions, df, dimensions.data_list())
    df = df[["cause_id"] + dimensions.index_names + dimensions.data_list()]
    df = df.groupby(["cause_id"] + dimensions.index_names).sum()
    return df.reset_index()


class SequelaModelableEntityFormula(BaseFormula):
    @mark_xform(0)
    def xform_downsample(self):
        return downsample, (self.draw_source,), {}

    @mark_xform(1)
    def xform_hazard_to_inc(self):
        return convert_hazard_to_inc, (self.draw_source,), {}

    @mark_xform(2)
    def xform_square_data(self):
        return square_data, (self.draw_source,), {}

    @mark_xform(3)
    def xform_restrict_cancer_incidence(self):
        return restrict_cancer_incidence, (self.draw_source,), {}

    @mark_xform(4)
    def xform_attach_sequela_metadata_and_restict(self):
        return attach_sequela_metadata_and_restict, (self.draw_source,), {}


class BirthPrevModelableEntityFormula(BaseFormula):
    @mark_xform(0)
    def xform_downsample(self):
        return downsample, (self.draw_source,), {}

    @mark_xform(1)
    def xform_square_data(self):
        return square_data, (self.draw_source,), {}

    @mark_xform(2)
    def xform_attach_birth_metadata(self):
        return attach_birth_metadata, (self.draw_source,), {}


class InjuriesModelableEntityFormula(BaseFormula):
    @mark_xform(0)
    def xform_downsample(self):
        return downsample, (self.draw_source,), {}

    @mark_xform(1)
    def xform_square_data(self):
        return square_data, (self.draw_source,), {}

    @mark_xform(2)
    def attach_injuries_metadata_and_restict(self):
        return attach_injuries_metadata_and_restict, (self.draw_source,), {}


class SexualViolenceModelableEntityForumla(BaseFormula):
    @mark_xform(0)
    def xform_downsample(self):
        return downsample, (self.draw_source,), {}

    @mark_xform(1)
    def xform_square_data(self):
        return square_data, (self.draw_source,), {}

    @mark_xform(2)
    def attach_sexual_violence_metadata_and_restrict(self):
        return (attach_sexual_violence_metadata_and_restrict, (self.draw_source,), {})


class SourceSinkFactory:

    _file_pattern = "{location_id}/{measure_id}_{year_id}_{sex_id}.h5"

    def __init__(self, como_version):
        self.como_version = como_version

    def get_params_by_component(self, component, source_type="draws"):
        if source_type not in ["draws", "inputs"]:
            raise ValueError(
                f"source_type' must be either 'draws' or 'inputs' received {source_type}"
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
        self, modelable_entity_id, model_version_id, n_workers=1
    ):
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
            extra_params=extra_params,
        )

    def get_birth_prev_modelable_entity_source(
        self, modelable_entity_id, model_version_id, n_workers=1
    ):
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {
            "birth_prev": self.como_version.birth_prev,
            "dimensions": dim,
        }
        return self._get_modelable_entity_source(
            formula=BirthPrevModelableEntityFormula,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            extra_params=extra_params,
        )

    def get_en_injuries_modelable_entity_source(
        self, modelable_entity_id, model_version_id, n_workers=1
    ):
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
            extra_params=extra_params,
        )

    def get_sexual_violence_modelable_entity_source(
        self, modelable_entity_id, model_version_id, n_workers=1
    ):
        dim = self.como_version.nonfatal_dimensions.get_simulation_dimensions(
            self.como_version.measure_id
        )
        extra_params = {
            "cause_restrictions": self.como_version.cause_restrictions,
            "dimensions": dim,
        }
        return self._get_modelable_entity_source(
            formula=SexualViolenceModelableEntityForumla,
            modelable_entity_id=modelable_entity_id,
            model_version_id=model_version_id,
            n_workers=n_workers,
            extra_params=extra_params,
        )

    @property
    def sequela_result_source(self):
        return DrawSource(self.get_params_by_component("sequela", "draws"))

    @property
    def sequela_result_sink(self):
        return DrawSink(self.get_params_by_component("sequela", "draws"))

    @property
    def cause_result_source(self):
        return DrawSource(self.get_params_by_component("cause", "draws"))

    @property
    def cause_result_sink(self):
        return DrawSink(self.get_params_by_component("cause", "draws"))

    @property
    def impairment_result_source(self):
        return DrawSource(self.get_params_by_component("impairment", "draws"))

    @property
    def impairment_result_sink(self):
        return DrawSink(self.get_params_by_component("impairment", "draws"))

    @property
    def injuries_result_source(self):
        return DrawSource(self.get_params_by_component("injuries", "draws"))

    @property
    def injuries_result_sink(self):
        return DrawSink(self.get_params_by_component("injuries", "draws"))

    @property
    def sequela_input_source(self):
        return DrawSource(self.get_params_by_component("sequela", "inputs"))

    @property
    def sequela_input_sink(self):
        return DrawSink(self.get_params_by_component("sequela", "inputs"))

    @property
    def injuries_input_source(self):
        return DrawSource(self.get_params_by_component("injuries", "inputs"))

    @property
    def injuries_input_sink(self):
        return DrawSink(self.get_params_by_component("injuries", "inputs"))

    @property
    def sexual_violence_input_source(self):
        return DrawSource(self.get_params_by_component("sexual", "inputs"))

    @property
    def sexual_violence_input_sink(self):
        return DrawSink(self.get_params_by_component("sexual", "inputs"))

    def _get_modelable_entity_source(
        self, formula, modelable_entity_id, model_version_id, n_workers, extra_params
    ):
        if extra_params is None:
            extra_params = {}
        extra_params["model_version_id"] = model_version_id
        extra_params["modelable_entity_id"] = modelable_entity_id

        decomp_step_id = (
            self.como_version.mvid_list.loc[
                self.como_version.mvid_list.model_version_id == model_version_id,
                "decomp_step_id",
            ]
            .unique()
            .tolist()[0]
        )
        decomp_step = decomp_step_from_decomp_step_id(decomp_step_id)

        arti = Artifact(
            identifier=modelable_entity_id,
            artifact_type_id=1,
            gbd_round_id=self.como_version.gbd_round_id,
            decomp_step=decomp_step,
        )
        metadata_version = arti.get_metadata_version(model_version_id)

        directories = metadata_version.directory()
        untried_specs = np.atleast_1d(metadata_version.known_specs()).tolist()
        dirs_and_specs = list(itertools.product(directories, untried_specs))

        while dirs_and_specs:
            directory, spec = dirs_and_specs.pop()
            try:
                f = formula()
                f.build_standard_draw_source(
                    directory, spec, n_workers, extra_params=extra_params
                )
                dirs_and_specs = []

            except InvalidSpec:
                f = None

        if f is None:
            logger.error(
                f"get_modelable_entity_source({formula}, {modelable_entity_id}, {model_version_id}, {n_workers}, {extra_params}); f is None - {f}"
            )
            raise

        f.add_transforms()
        return f.draw_source