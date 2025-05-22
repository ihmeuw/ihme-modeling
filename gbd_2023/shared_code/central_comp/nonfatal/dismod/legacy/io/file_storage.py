import logging
import pathlib
import re
import shutil
from typing import Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from cascade_ode.legacy import demographics
from cascade_ode.legacy.io import dataset as dataset_module

ALL_DATASETS = dataset_module.ALL_DATASETS


def consolidate_all_submodel_files(submodel_dir: str) -> None:
    """
    After all submodels have run, we want to save additional space by consolidating
    each logical dataset into one physical file. This function runs through all
    ~20k files produced by a full cascade and saves one file per type of data.

    After each dataset is consolidated into one large new file, the original
    inputs are moved to a 'consolidated' dir, where they can be deleted.
    """
    log = logging.getLogger(__name__)

    # make a dir to move files after we're finished with them. We use this to
    # track consolidation progress and for error recovery
    consolidated_dir = 
    consolidated_dir.mkdir(exist_ok=True)
    submodel_files = _list_submodel_files_by_type(submodel_dir)

    # if a mv was interrupted, we don't want to rerun consolidation for that
    # dataset (it would be incomplete)
    already_consolidated_files = _list_submodel_files_by_type(consolidated_dir)
    for unconsolidated_inputs, consolidated_inputs in zip(
        submodel_files.values(), already_consolidated_files.values()
    ):
        if consolidated_inputs:
            # in this case the consolidation already finished and it was just
            # the mv that was interrupted. So we can just resume the mv
            for finished in unconsolidated_inputs:
                shutil.move(str(finished), str(consolidated_dir))
            unconsolidated_inputs.clear()

    # if this is a retried job, some of these files might already have been
    # consolidated
    submodel_files = {
        dataset_type: filelist
        for (dataset_type, filelist) in submodel_files.items()
        if len(filelist) > 0
    }
    for dataset_type, filelist in submodel_files.items():
        file_name = ALL_DATASETS[dataset_type].file_name
        outfile_path = get_consolidated_file_path(submodel_dir, file_name)
        log.info(f"Consolidating {len(filelist)} {dataset_type} files into {file_name}")
        _consolidate_files(filelist, outfile_path)
        log.info(f"Marking {dataset_type} files as finished")
        for finished in filelist:
            shutil.move(str(finished), str(consolidated_dir))


def read_consolidated_data_pred_files(submodel_dir: str) -> pd.DataFrame:
    """
    When we compute fit stats we need to read the datapredout dataset
    """
    files_by_type = _list_consolidated_submodel_files_by_type(submodel_dir)
    data_pred_path = files_by_type["datapredout"]
    df = pq.read_pandas(data_pred_path).drop(demographics.CONTEXT_COLS).to_pandas()
    df = df[df.a_data_id.notnull()]
    df["cv_id"] = "full"
    return df


def export_csvs_to_upload(submodel_dir: str) -> Dict[str, str]:
    """
    After we've consolidated submodel data into large files, we need to upload
    a few types of data for EpiViz.

    We currently upload the following data:
        all fits, all priors
        Global adjusted data, global effects
    """
    files_by_type = _list_consolidated_submodel_files_by_type(submodel_dir)
    fits = files_by_type["posterior_upload"]
    priors = files_by_type["prior_upload"]
    adjusted = files_by_type["adj_data_upload"]
    effects = files_by_type["effect_upload"]

    output_csvs = {}
    fit_data = pq.read_pandas(fits).drop(demographics.CONTEXT_COLS).to_pandas()
    output_csv = 
    fit_data.to_csv(output_csv, index=False)
    output_csvs["posterior_upload"] = str(output_csv)

    prior_data = pq.read_pandas(priors).drop(demographics.CONTEXT_COLS).to_pandas()
    output_csv = 
    prior_data.to_csv(output_csv, index=False)
    output_csvs["prior_upload"] = str(output_csv)

    adjusted_data = (
        pq.read_pandas(adjusted, filters=[("context_location_id", "=", 1)])
        .drop(demographics.CONTEXT_COLS)
        .to_pandas()
    )
    output_csv = 
    adjusted_data.to_csv(output_csv, index=False)
    output_csvs["adj_data_upload"] = str(output_csv)

    effects_data = (
        pq.read_pandas(effects, filters=[("context_location_id", "=", 1)])
        .drop(demographics.CONTEXT_COLS)
        .to_pandas()
    )
    output_csv = 
    effects_data.to_csv(output_csv, index=False)
    output_csvs["effect_upload"] = str(output_csv)

    return output_csvs


def delete_unconsolidated_files(submodel_dir: str):
    """
    After submodel datasets are consolidated into larger files, we can
    clean up the original smaller files
    """
    files_by_type = 
    for files in files_by_type.values():
        for f in files:
            pathlib.Path(f).unlink()


def _consolidate_files(
    files: List[str], outfile_path: pathlib.Path, batch_size: int = 200_000
) -> None:
    """
    Take a list of file paths and save one new file that is the concatenation
    of input files.

    Argument:
        files: list of filepaths to consolidate
        outfile_path: new filepath to save
        batch_size: how many rows to buffer before saving to disk
    """
    batches = []
    # lets ignore files that are empty -- otherwise we hit casting errors
    # inside scanner
    non_empty_files = [f for f in files if pq.read_metadata(f).num_rows > 0]
    dataset = ds.dataset(non_empty_files)
    scanner = dataset.scanner(use_threads=False)
    writer = None
    for batch in scanner.to_batches():
        batches.append(batch)
        if sum(b.num_rows for b in batches) >= batch_size:
            writer = _write_arrow_batch(writer, batches, str(outfile_path))
            batches = []
    if batches:
        writer = _write_arrow_batch(writer, batches, str(outfile_path))
    writer.close()


def _list_submodel_files_by_type(submodel_dir: str) -> Dict[str, List[str]]:
    """
    Returns a dictionary mapping dataset type to list of files saved by
    datastore.save_datastores
    """
    datasets = ALL_DATASETS.copy()
    dataset_files = {}
    for dataset_type, dataset in datasets.items():
        matching_files = [
            f.absolute()
            for f in pathlib.Path(submodel_dir).iterdir()
            if re.search(f"[1,2,3]_{dataset.file_name}$", str(f))
        ]
        dataset_files[dataset_type] = matching_files
    return dataset_files


def _list_consolidated_submodel_files_by_type(submodel_dir: str) -> Dict[str, str]:
    """
    Returns a dictionary mapping dataset type to filepath produce by
    file_storage.consolidate_all_submodel_files
    """
    # 'drawout' dataset is never saved/uploaded
    datasets = {
        dataset_key: dataset
        for (dataset_key, dataset) in ALL_DATASETS.items()
        if dataset_key not in ["drawout"]
    }
    dataset_files = {}
    for dataset_type, dataset in datasets.items():
        matching_files = 
        if len(matching_files) != 1:
            raise RuntimeError(
                "Did not find expected single filepath for consolidated "
                f"{dataset_type} file: {matching_files}"
            )
        dataset_files[dataset_type] = matching_files[0]
    return dataset_files


def _write_arrow_batch(
    writer: Optional[pq.ParquetWriter], data: List[pa.RecordBatch], file_name: str
) -> pq.ParquetWriter:
    """append a list of RecordBatches to a parquet file"""
    table = pa.Table.from_batches(data)
    logger = logging.getLogger(__name__)
    logger.info(f"Writing {len(table)} rows to {file_name}")
    if writer is None:
        writer = pq.ParquetWriter(file_name, table.schema, compression="brotli")
    writer.write_table(table)
    return writer


def write_dataframe_batch(
    writer: Optional[pq.ParquetWriter],
    data: List[pd.DataFrame],
    file_name: Union[str, pathlib.Path],
) -> pq.ParquetWriter:
    """Concatenate and append a list of dataframes to a parquet file"""
    to_write = pa.Table.from_pandas(
        _standardize_columns(pd.concat(data)),
        schema=writer.schema if writer is not None else None,
        preserve_index=False,
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Writing {len(to_write)} rows to {file_name}")
    if writer is None:
        writer = pq.ParquetWriter(file_name, to_write.schema, compression="brotli")
    writer.write_table(to_write)
    return writer


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Before appending to parquet file, we need to make sure the data types per
    field are constant.
    """
    original_order = list(df.columns)
    mixed_dtypes = df.select_dtypes(include=["object"])
    for col in mixed_dtypes:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            df[col] = df[col].astype(str)
    loc_lvls = ["atom", "region", "subreg", "super"]
    for col in loc_lvls:
        if col in df:
            df[col] = df[col].astype(str)
    # convert all int cols to float -- using pd.concat is faster than df.astype
    # because it sidesteps BlockManager shenanigans
    int_cols = df.select_dtypes(include=["int64"]).columns
    converted_int_vals = df[int_cols].astype(float)
    df = df.drop(int_cols, axis=1)
    df = pd.concat([df, converted_int_vals], axis=1)
    df = df[original_order]
    return df


def _get_free_gb_on_tmp():
    free_bytes = 
    return free_bytes / 1024**3


def tmpdir_root():
    """
    if a node has 1gb of free space or more on , return . Otherwise
    return None
    """
    free_gb = _get_free_gb_on_tmp()
    if free_gb > 1:
        return 


def get_file_name(
    rootdir: str, dataset_file_name: str, context: demographics.DemographicContext
) -> pathlib.Path:
    """
    The file that a dataset is saved to depends on the demographics of the
    submodel.
    """
    if context.parent_location_id is None:
        prefix = 
    else:
        prefix = 
    fpath = 
    return fpath


def get_consolidated_file_path(rootdir: str, dataset_file_name: str) -> pathlib.Path:
    """Returns path of file for dataset post-consolidation."""
    return 


def read_data(
    rootdir: str, dataset_file_name: str, context: demographics.DemographicContext
) -> pd.DataFrame:
    """
    Given a dataset and demographic context, return a dataframe containing
    data for that dataset and demographic
    """
    file_path = get_file_name(rootdir, dataset_file_name, context)
    # the file may have already been consolidated (ie we're restarting model)
    if not file_path.exists():
        file_path = get_consolidated_file_path(rootdir, dataset_file_name)
    filters = [
        ("context_location_id", "=", context.location_id),
        ("context_year_id", "=", context.year_id),
        ("context_sex_id", "=", context.sex_id),
    ]
    table = pq.read_pandas(file_path, filters=filters)
    table = table.drop(["context_location_id", "context_year_id", "context_sex_id"])
    return table.to_pandas()
