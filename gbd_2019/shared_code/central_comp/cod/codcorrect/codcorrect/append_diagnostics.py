import argparse
import logging
import os
import subprocess

from draw_sources.draw_sources import DrawSource
from gbd import constants as gbd
import pandas as pd

from fauxcorrect.utils.logging import setup_logging
from fauxcorrect.utils.constants import (
    FilePaths, Measures, Columns, Years, DAG, Diagnostics
)
from fauxcorrect.parameters.master import (
    CoDCorrectParameters, MachineParameters
)


"""
    Appends all diagnostics together that already exist from the most-detailed
    location level, and creates diagnostics for location aggregate level.
    All diagnostics only exist for deaths, not YLLs
"""


def parse_args():
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--version_id',
        type=int,
        help="The CodCorrect version we are uploading diagnostics for.",
        required=True
    )
    return parser.parse_args()


def read_aggregated_unscaled(parent_dir, location_id, diag_years):
    """ Read in location aggregates of unscaled draws for deaths only"""
    unscaled_params = {
        'draw_dir': os.path.join(parent_dir,
                                 FilePaths.AGGREGATED_DIR,
                                 FilePaths.UNSCALED_DIR),
        'file_pattern': FilePaths.AGGREGATED_UNSCALED_FILE_PATTERN
    }
    ds = DrawSource(unscaled_params)
    unscaled_draws = ds.content(filters={'location_id': location_id,
                                         'year_id': diag_years,
                                         'measure_id': Measures.Ids.DEATHS})
    return unscaled_draws


def read_aggregated_rescaled(parent_dir, location_id, diag_years):
    """ Read in location aggregates of rescaled draws for deaths only"""
    rescaled_params = {
        'draw_dir': os.path.join(parent_dir, FilePaths.DRAWS_DIR),
        'file_pattern': FilePaths.DRAWS_FILE_PATTERN
    }
    ds = DrawSource(rescaled_params)
    rescaled_draws = ds.content(filters={'location_id': location_id,
                                         'year_id': diag_years,
                                         'measure_id': Measures.Ids.DEATHS})
    return rescaled_draws


def create_diagnostics(before_data, after_data):
    before_data[Columns.DIAGNOSTICS_BEFORE] = before_data[Columns.DRAWS].mean(
        axis=1)
    after_data[Columns.DIAGNOSTICS_AFTER] = after_data[Columns.DRAWS].mean(
        axis=1)

    data = pd.merge(before_data[Columns.INDEX + [Columns.DIAGNOSTICS_BEFORE]],
                    after_data[Columns.INDEX + [Columns.DIAGNOSTICS_AFTER]],
                    on=Columns.INDEX, how='outer')
    # unscaled data will have NaN's for any shocks
    data.fillna(0, inplace=True)
    return data


def calculate_diagnostics(parent_dir, location_id, diag_years):
    """ Calculates diagnostics, as the difference between scaled and unscaled
    and formats it for the codcorrect.diagnostics table schema"""
    logging.info("Calculating diagnostics for {}".format(location_id))
    unscaled = read_aggregated_unscaled(parent_dir, location_id, diag_years)
    rescaled = read_aggregated_rescaled(parent_dir, location_id, diag_years)
    return create_diagnostics(unscaled, rescaled)


def compile_and_compute_diagnostics(version: MachineParameters):
    """Read in the diagnostic files precomputed for most-detailed
    locations and causes.

    For aggregate locations, compute new diagnostic datasets on the fly
    from draws

    Returns:
        pd.DataFrame
    """

    location_ids = version.location_ids
    most_detailed_locations = version.most_detailed_location_ids
    diag_years = Years.ESTIMATION

    logging.info('Reading in diagnostic files and creating ones that '
                 'dont exist')
    data = []
    for location_id in location_ids:
        if location_id in most_detailed_locations:
            for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                # all most detailed locations already have diagnostics created
                file_path = (
                    os.path.join(
                        version.parent_dir,
                        FilePaths.DIAGNOSTICS_DIR,
                        FilePaths.DIAGNOSTICS_DETAILED_FILE_PATTERN.format(
                            location_id=location_id, sex_id=sex_id))
                )
                logging.info("Reading in {}".format(file_path))
                df = pd.read_csv(file_path)
                df = df.loc[df['year_id'].isin(diag_years)]
                data.append(df)
        # all aggregated locations need diagnostics created
        else:
            data.append(calculate_diagnostics(version.parent_dir, location_id,
                                              diag_years))

    logging.info("Concatenating in diagnostic files")
    diag = pd.concat(data)

    return diag


def format_for_upload_and_save(df: pd.DataFrame, version: MachineParameters):
    # Format for upload
    df['output_version_id'] = version.version_id
    df = df[Diagnostics.DataBase.COLUMNS]

    # Save
    logging.info("Saving single diagnostic file")
    file_path = os.path.join(
        version.parent_dir,
        FilePaths.DIAGNOSTICS_DIR, FilePaths.DIAGNOSTICS_UPLOAD_FILE)
    df.to_csv(file_path, index=False)
    permissions_change = ['chmod', '775', file_path]
    subprocess.check_output(permissions_change)

    logging.info('All done!')


if __name__ == '__main__':

    args = parse_args()
    version = CoDCorrectParameters.recreate_from_version_id(
         version_id=args.version_id
    )

    setup_logging(
        version.parent_dir,
        DAG.Tasks.Type.DIAGNOSTIC,
        args)

    df = compile_and_compute_diagnostics(version)
    format_for_upload_and_save(df, version)
