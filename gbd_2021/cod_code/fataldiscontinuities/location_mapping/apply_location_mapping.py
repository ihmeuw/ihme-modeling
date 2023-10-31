import numpy as np
import os

from shock_prep.etl.apply_location_mapping import (
    read_maps as rm,
    apply_map_hierarchy as amh,
    location_side_resolution as lsr,
)

LOC_COLS = ['location_id', 'side_a', 'side_b']

LOCATION_FOLDER = "FILEPATH"
LOCATION_FOLDER_ARCHIVE = os.path.join(LOCATION_FOLDER, "archive")


def get_location_map(source):

    df = rm.create_location_mapping_dataframe(source)
    df = amh.apply_mapping_hierarchy(df)
    df = lsr.resolve_location_and_sides(df)
    return df


def assert_correctness_of_formatted(df):
    has_location = df['location_id'].notna()
    has_side_a = df['side_a'].notna()
    has_side_b = df['side_b'].notna()
    correct = (
        (has_location & ~(has_side_a | has_side_b)) ^
        (~has_location & (has_side_a & has_side_b))
    )
    assert correct.all(), (
        f"Some data (size={(~correct).sum()}) don't specify location_id "
        f"or both side_a and side_b:\n{df[~correct]}"
    )


def format_location_map(df):
    loc_map = (
        df
        .loc[
            df['location_side_merge_kept'].fillna(False),
            ['source_event_id', 'dest_col', 'location_id']
        ]
        .drop_duplicates()
        .assign(location_id=lambda d: d.location_id.astype(str))
        .pivot_table(index='source_event_id',
                     columns='dest_col',
                     values='location_id',
                     aggfunc=lambda l: ', '.join(l))
        .rename_axis(columns=None)
        .reset_index()
    )
    for col in LOC_COLS:
        if col not in loc_map:
            loc_map[col] = np.nan
    assert_correctness_of_formatted(loc_map)
    return loc_map


def save_location_map(df, source, version, save_diag=False):
    if save_diag:
        diag = "_diag"
    else:
        diag = ""
    current_folder = LOCATION_FOLDER.format(source=source)
    archive_folder = LOCATION_FOLDER_ARCHIVE.format(source=source)
    location_map_current_path = os.path.join(current_folder, "location_map{}.csv".format(diag))
    location_map_archive_path = os.path.join(
        archive_folder, "location_map{}_{}.csv".format(diag, version))
    df.to_csv(location_map_current_path)
    df.to_csv(location_map_archive_path)
    return df


def run_location_matching(df, source, version):
    assert not df['source_event_id'].duplicated().any()
    location_map = (get_location_map(source)
                    .pipe(save_location_map, source, version, save_diag=True)
                    .pipe(format_location_map)
                    .pipe(save_location_map, source, version)
                    )

    data_size_start = df.shape[0]

    df = (df
          .drop(columns=LOC_COLS, errors='ignore')
          .merge(location_map,
                 on='source_event_id',
                 how='left'
                 )
          )

    data_size_end = df.shape[0]
    assert data_size_start == data_size_end, (
        f"The data size changed during location mapping from "
        f"{data_size_start} to {data_size_end}."
    )
    return df
