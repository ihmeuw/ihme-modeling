import numpy as np
import pandas as pd

from core_maths.scale_split import merge_scale
from dataframe_io.pusher import SuperPusher
from db_queries import get_age_spans
from db_queries.api.internal import get_age_group_set as get_age_group_set_id
from epic.util.common import (
    get_best_model_version_and_decomp_step,
    group_and_downsample
)

from draw_sources.draw_sources import DrawSource
import epic.util.constants as epic_constants
import gbd.constants as gbd_constants
from get_draws.sources.epi import Epi
from get_draws.transforms.automagic import automagic_age_sex_agg
from hierarchies import dbtrees
from ihme_dimensions import dimensionality


class CovidScaler(object):

    def __init__(
            self, modelable_entity_id, output_modelable_entity_id,
            output_dir, decomp_step, cause_id, scalar_dir, location_id=None,
            year_id=None, age_group_id=None, sex_id=None,
            measure_id=None, location_set_id=35,
            gbd_round_id=gbd_constants.GBD_ROUND_ID, n_draws=1000
    ):

        # static ids
        self.modelable_entity_id = modelable_entity_id
        self.output_modelable_entity_id = output_modelable_entity_id
        self.output_dir = output_dir
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.year_id = year_id
        self.cause_id = cause_id
        self.scalar_dir = scalar_dir

        self.draw_columns = ['draw_{}'.format(i) for i in range(n_draws)]
        self.model_draws = None
        self.scalar_draws = self.get_scalar_draws()

        if not location_id:
            location_id = [
                node.id for node in dbtrees.loctree(
                    location_set_id=location_set_id,
                    gbd_round_id=gbd_round_id
                ).leaves()
            ]
        self.location_id = location_id

        if not self.year_id:
            raise RuntimeError(
                "EMR Scaling should only be run for specific years!"
            )
        if not age_group_id:
            # this has the advantage of instantiating the lru cache in the main
            # process before multiprocessing
            age_group_set_id = get_age_group_set_id(gbd_round_id)
            age_group_id = get_age_spans(age_group_set_id)["age_group_id"].tolist()
            if self.modelable_entity_id in epic_constants.MEIDS.INCLUDE_BIRTH_PREV:
                age_group_id.append(gbd_constants.age.BIRTH)
        if not sex_id:
            sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]
        if not measure_id:
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]

        data_dict = {"data": self.draw_columns}
        index_dict = {
            "location_id": self.location_id,
            "year_id": self.year_id,
            "age_group_id": age_group_id,
            "sex_id": sex_id,
            "measure_id": measure_id
        }
        self.model_dimensions = dimensionality.DataFrameDimensions(
            index_dict, data_dict
        )
        scalar_index_dict = {
            "location_id": self.location_id,
            "year_id": self.year_id,
            "age_group_id": age_group_id,
            "sex_id": sex_id,
        }
        self.scalar_dimensions = dimensionality.DataFrameDimensions(
            scalar_index_dict, data_dict
        )

        self.model_version_id, dstep = (
            get_best_model_version_and_decomp_step(output_dir, self.modelable_entity_id)
        )
        # model draw source
        model_src = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=self.modelable_entity_id,
            model_version_id=self.model_version_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=dstep
        )

        try:
            model_src.remove_transform(automagic_age_sex_agg)
        except ValueError:
            pass
        # add downsampling transform if necessary
        if n_draws < 1000:
            model_src.add_transform(group_and_downsample, n_draws)
        self._draw_source = model_src

        self.pusher = SuperPusher(
            spec={'file_pattern': "{modelable_entity_id}/{location_id}.h5",
                  'h5_tablename': 'draws'},
            directory=output_dir
        )

    def demo_filters(self, dimensions):
        return {
            "location_id": dimensions.index_dim.get_level("location_id"),
            "age_group_id": dimensions.index_dim.get_level("age_group_id"),
            "year_id": dimensions.index_dim.get_level("year_id"),
            "sex_id": dimensions.index_dim.get_level("sex_id"),
            "measure_id": dimensions.index_dim.get_level("measure_id")
        }

    def run_scaling(self):
        self.model_draws = self._draw_source.content(
            filters=self.demo_filters(self.model_dimensions).copy()
        )
        self.model_draws = self.model_draws.drop(
            ['model_version_id', 'modelable_entity_id'], axis=1)
        self.scalar_draws = self.scalar_draws.drop(['cause_id'], axis=1)

        # run validations
        self.validate_scaling()

        expanded_scalar_draws = []
        for measure in self.model_dimensions.index_dim.get_level("measure_id"):
            scalar_copy = self.scalar_draws.copy()
            scalar_copy["measure_id"] = measure
            expanded_scalar_draws.append(scalar_copy)
        expanded_scalar_draws = pd.concat(expanded_scalar_draws)

        group_cols = [col for col in self.model_draws if 'draw_' not in col]
        value_cols = [col for col in self.model_draws if 'draw_' in col]

        # add metric column
        expanded_scalar_draws['metric_id'] = gbd_constants.metrics.RATE

        scaled_draws = merge_scale(
            self.model_draws,
            expanded_scalar_draws,
            group_cols=group_cols,
            value_cols=value_cols
        )
        # Set output modelable_entity_id
        scaled_draws['modelable_entity_id'] = self.output_modelable_entity_id

        self.validate_results(scaled_draws=scaled_draws)
        print("Pushing draws")
        self.pusher.push(scaled_draws, append=False)

    def get_scalar_draws(self):
        num_workers = 1
        scalar_draws = DrawSource({
            'draw_dir': self.scalar_dir,
            'file_pattern': '{cause_id}_{{sex_id}}_{{location_id}}.h5'.format(
                cause_id=self.cause_id),
            'num_workers': num_workers
        }).content()

        return scalar_draws

    def validate_scaling(self):
        for draw_col in self.draw_columns:
            if not all((self.scalar_draws[draw_col] >= 0) & (self.scalar_draws[draw_col] < np.inf)):
                raise ValueError(
                    f"Scalar is negative or inf, please review scalars "
                    f"in column: {draw_col}"
                )

    def validate_results(self, scaled_draws):
        for draw_col in self.draw_columns:
            prev_df = scaled_draws[
                scaled_draws.measure_id == gbd_constants.measures.PREVALENCE]
            if not all(prev_df[draw_col] <= 1):
                raise ValueError(
                    f"Prevalence is greater than 1 in column: {draw_col}, review"
                    f"results"
                )
