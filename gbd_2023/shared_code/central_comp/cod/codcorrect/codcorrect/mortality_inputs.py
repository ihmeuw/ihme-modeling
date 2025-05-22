import os
import numpy as np
import pandas as pd
from typing import List

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
import db_queries
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from get_draws.base.utils import ndraw_grouper
from get_draws.api import _downsample
from hierarchies import dbtrees

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils.constants import (
    Columns,
    Draws,
    FilePaths,
    MortalityEnvelope,
    MortalityInputs
)
from codcorrect.lib.validations import data_validations


class ValidMortalityProcessInputs:
    ENVELOPE_DRAWS: str = 'envelope_draws'
    ENVELOPE_SUMMARY: str = 'envelope_summary'
    POPULATION: str = 'population'

    OPTIONS: List[str] = [ENVELOPE_SUMMARY, ENVELOPE_DRAWS, POPULATION]


def validate_mortality_process_argument(mort_process: str) -> None:
    """
    Raises an error if the mort_process is not one of 'population',
    'envelope_draws', or 'envelope_summary'.

    Raises:
        RuntimeError
    """
    if mort_process not in ValidMortalityProcessInputs.OPTIONS:
        raise RuntimeError(
            f"Cannot cache mortality input type: {mort_process}. Valid "
            "mortality process types are: "
            f"{', '.join(MortalityInputs.ALL_INPUTS)}."
        )


def cache_data(mort_process: str, version: MachineParameters) -> None:
    """
    Read in mortality data wherever it lives and caches it to the CodCorrect filesystem.

    Using the input_type argument, we decide the proper 'read' function and the cache function.

    Arguments:
         mort_process (str): the mortality process to save to cache. One of 'envelope_draws'
             'envelope_summary', 'population'
         version (MachineParameters): machinery parameters for the run.
    """
    read_function_map = {
        MortalityInputs.ENVELOPE_DRAWS: _get_envelope_draws,
        MortalityInputs.ENVELOPE_SUMMARY: _get_envelope_summary,
        MortalityInputs.POPULATION: _get_population,
    }
    cache_function_map = {
        MortalityInputs.ENVELOPE_DRAWS: version.file_system.cache_envelope_draws,
        MortalityInputs.ENVELOPE_SUMMARY: version.file_system.cache_envelope_summary,
        MortalityInputs.POPULATION: version.file_system.cache_population,
    }

    # Read in data based on the mortality process
    data = read_function_map[mort_process](version)
    data_validations.check_duplicates(data)

    # Cache data
    cache_function_map[mort_process](data)


def _get_envelope_summary(version: MachineParameters) -> pd.DataFrame:
    """
    Wrapper around get_envelope; used to unpack arguments from version object
    as well as rename columns and sub-select dataframe columns.

    Arguments:
        version (MachineParameters): object containing all the demographic
            and fauxcorrect configuration data needed to query envelope
            estimates.

    Return:
        pd.DataFrame
    """
    env = db_queries.get_envelope(
        age_group_id=version.all_age_group_ids,
        location_id=version.location_ids,
        year_id=version.year_ids,
        sex_id=version.all_sex_ids,
        release_id=version.release_id,
        run_id=version.envelope_version_id,
        use_rotation=False,
        with_shock=0,
        with_hiv=MortalityEnvelope.WITH_HIV
    )
    env.rename(
        columns={Columns.MEAN: Columns.ENVELOPE},
        inplace=True
    )
    keep_cols = (
        Columns.DEMOGRAPHIC_INDEX + [Columns.ENVELOPE]
    )
    return env[keep_cols]


def _get_envelope_draws(version: MachineParameters) -> pd.DataFrame:
    """
    Used to unpack arguments from version object as well as rename columns
    and sub-select dataframe columns.

    Arguments:
        version (MachineParameters): object containing all the demographic
            and codcorrect configuration data needed to query envelope
            draws.

    Return:
        pd.DataFrame
    """
    file_path = FilePaths.MORT_ENVELOPE_DRAW_DIR.format(
        version_id=version.envelope_version_id)
    all_files = (
        [
            os.path.join(
                file_path,
                FilePaths.MORT_ENVELOPE_FILE.format(year_id=year))
            for year in version.year_ids
        ]
    )
    downsample = False
    if version.n_draws < Draws.MAX_DRAWS:
        downsample = True

    rename_dict = {
        key: value for (key, value) in zip(
            [
                f'{Columns.ENV_PREFIX}{draw_num}'
                for draw_num in range(Draws.MAX_DRAWS)
            ],
            [
                f'{Columns.DRAW_PREFIX}{draw_num}'
                for draw_num in range(Draws.MAX_DRAWS)
            ]
        )
    }

    envelope_list = []
    for f in all_files:
        df = pd.read_hdf(f, key='data')
        df = df.rename(columns=rename_dict)
        if downsample:
            groups = ndraw_grouper(df, "year_id")
            if len(groups) > 2:
                raise RuntimeError("More than two draw lengths in data: {}. "
                                    "Unable to downsample while retaining "
                                    "draw correlation.".format(list(groups)))
            df = _downsample(downsample, version.n_draws, groups)
        envelope_list.append(df)
    envelope = pd.concat(envelope_list)
    envelope = (
        envelope[Columns.DEMOGRAPHIC_INDEX +
                 version.draw_cols]
    )

    # Filter to just the most-detailed
    envelope = envelope.loc[
        (envelope['location_id'].isin(version.most_detailed_location_ids)) &
        (envelope['year_id'].isin(version.year_ids)) &
        (envelope['sex_id'].isin(version.sex_ids)) &
        (envelope['age_group_id'].isin(version.most_detailed_age_group_ids))
    ].reset_index(drop=True)

    return envelope


def _get_population(
        version: MachineParameters
) -> pd.DataFrame:
    """
    Unpacks arguments from version object to use with get_population
    function. Requests most detailed ages and most detailed sexes because
    age-sex population aggregates are created in the summarize module.
    Dependant on demographics team to upload population for majority of
    aggregate locations but currently uses AggSynchronous to create population
    information for select Norway locations in LocationSetId.OUTPUTS.

    Arguments:
        version (MachineParameters): object containing all the demographic
            and configuration data needed to query population
            estimates.

    Return:
        pd.DataFrame
    """
    pop = db_queries.get_population(
        age_group_id=version.most_detailed_age_group_ids,
        location_id=version.location_ids,
        year_id=version.year_ids,
        sex_id=version.sex_ids,
        run_id=version.population_version_id,
        release_id=version.release_id
    )
    io_mock = {}
    source = DrawSource(
        {
            "draw_dict": io_mock,
            "name": "tmp"
        },
        mem_read_func
    )
    sink = DrawSink(
        {
            "draw_dict": io_mock,
            "name": "tmp"
        },
        mem_write_func
    )
    index_cols = Columns.DEMOGRAPHIC_INDEX
    data_cols = [Columns.POPULATION]
    sink.push(pop[index_cols + data_cols])

    loc_sets_to_aggregate = (
        version.aggregation_location_set_ids + [version.base_location_set_id]
    )

    if loc_sets_to_aggregate:
        # The base location set MUST be last in the list, otherwise bad aggregates
        # will be used to create special locations where there is overlap
        # with location ids in the base set.
        assert len(loc_sets_to_aggregate) == len(set(loc_sets_to_aggregate))
        assert loc_sets_to_aggregate[-1] == version.base_location_set_id

        for set_id in loc_sets_to_aggregate:
            loc_trees = dbtrees.loctree(
                location_set_id=set_id,
                release_id=version.release_id,
                return_many=True,
            )
            for tree in loc_trees:
                operator = Sum(
                    index_cols=([col for col in index_cols if col != Columns.LOCATION_ID]),
                    value_cols=data_cols
                )
                aggregator = AggSynchronous(
                    draw_source=source,
                    draw_sink=sink,
                    index_cols=([col for col in index_cols if col != Columns.LOCATION_ID]),
                    aggregate_col=Columns.LOCATION_ID,
                    operator=operator
                )
                aggregator.run(tree)
        special_locations = source.content()
    else:
        special_locations = pd.DataFrame({
            Columns.LOCATION_ID: [np.nan]
        })

    return pd.concat(
        [
            pop,
            special_locations.loc[
                ~special_locations.location_id.isin(
                    pop.location_id.unique()
                )]
        ],
        ignore_index=True
    )
