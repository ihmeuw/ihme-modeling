import atexit
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, List

import pandas as pd

from cascade_ode.legacy.demographics import DemographicContext
from cascade_ode.legacy.io import dataset, file_storage


class Datastore:
    _files_at_cascade_root = {ds.name: ds for ds in dataset.CASCADE_ROOT_DATASETS}
    _all_datasets = dataset.ALL_DATASETS

    def __init__(self, submodel_dir: str, cascade_root_dir: str, context: DemographicContext):
        """
        A datastore is used to store and retrieve datasets for a submodel
        in the cascade.

        It can also export data to csv (used when interacting with dismod). These
        paths are automatically cleaned up.

        Arguments:
            submodel__dir: where all final inputs/outputs are  written to
                with save method (except for inputs that exist at cascade root)
            cascade_root_dir: the model's root directory
            context: what submodel are we saving data for?

        """
        self.submodel_dir = submodel_dir
        self.cascade_root_dir = cascade_root_dir
        self.context = context

        # We write csvs here, if export method is used
        self._tmpdir = None

        # We avoid immediately writing to disk or reading same data twice with
        # an internal cache
        self._cache = {}

    def __getitem__(self, key: str) -> pd.DataFrame:
        """
        Retrieve data from in-memory if we've already read from disk. Otherwise
        read from disk
        """
        if key not in self._all_datasets.keys():
            raise RuntimeError(f"{key} is not a known dataset")
        dataset = self._all_datasets[key]
        filename = dataset.file_name + "." + dataset.extension
        try:
            df = self._cache[key]
        except KeyError:
            if key in self._files_at_cascade_root:
                df = 
            else:
                df = file_storage.read_data(
                    rootdir=self.submodel_dir,
                    dataset_file_name=self._all_datasets[key].file_name,
                    context=self.context,
                )
        self._cache[key] = df
        return df.copy()

    def __setitem__(self, key: str, data: pd.DataFrame) -> None:
        """Add data to datastore for given key. Store in memory"""
        if key not in self._all_datasets.keys():
            raise RuntimeError(f"{key} is not a known dataset")
        self._cache[key] = data.copy()

    def keys(self) -> List[str]:
        """
        Returns the list of dataset names that have been stored so far
        """
        return list(self._cache.keys())

    def _root_dir(self, key: str) -> str:
        if key in self._files_at_cascade_root:
            return self.cascade_root_dir
        elif key in self._all_datasets.keys():
            return self.submodel_dir
        else:
            raise RuntimeError(f"datastore key {key} is not an input or output dataset")

    def _create_tmpdir(self) -> None:
        # We use this to ensure exported csvs are deleted during cleanup
        node_tmp = file_storage.tmpdir_root()
        tmpdir_root = node_tmp if node_tmp else self.submodel_dir
        self._tmpdir = tempfile.mkdtemp(dir=tmpdir_root)
        Path(self._tmpdir).chmod(0o755)
        atexit.register(lambda d: shutil.rmtree(d, ignore_errors=True), self._tmpdir)

    def clear(self) -> None:
        """Delete any cached data, and any temporary files saved to disk"""
        self._cache = {}
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def path_to_csv(self, key: str) -> Path:
        """
        Returns the path to a file for a dataset that has been stored.

        These paths are deleted during datastore.save, if they exist.
        """
        if self._tmpdir is None:
            self._create_tmpdir()

        dataset = self._all_datasets[key]
        filename = 
        return 

    def export_data_to_csv(self, names: List[str]) -> List[str]:
        """
        Given a list of dataset names, write each dataset to csv in a temporary
        directory. Return a list of filepaths.
        """
        if self._tmpdir is None:
            self._create_tmpdir()
        output_paths = []
        for dataset_name in names:
            data = self[dataset_name]
            dataset = self._all_datasets[dataset_name]
            output_path = 
            data.to_csv(output_path, index=False)
            output_path.chmod(0o744)
            output_paths.append(str(output_path.absolute()))

        return output_paths


def save_datastores(datastores: Iterable[Datastore], outdir: str, batchsize: int) -> None:
    """
    Take a collection of datastores and write their results to the file system.

    Submodel results are stored by parent location id, year, sex, and dataset type.
    It is assumed that all datastores are from submodels with the same parent location,
    year, and sex (ie how  works). So there is one file generated
    per dataset type.

    Arguments:
        datastores: iterable collection of datastores with data to save to disk
        outdir: folder to save files
        batchsize: number of rows to buffer before writing to disk
    """
    writers = {}
    batches = defaultdict(list)
    for store in datastores:
        for dataset_name in store.keys():
            df = store[dataset_name]
            df = df.assign(**store.context.identifiers())
            batches[dataset_name].append(df)
            if sum(len(t) for t in batches[dataset_name]) >= batchsize:
                writer = file_storage.write_dataframe_batch(
                    writer=writers.get(dataset_name),
                    data=batches[dataset_name],
                    file_name=file_storage.get_file_name(
                        outdir, dataset.ALL_DATASETS[dataset_name].file_name, store.context
                    ),
                )
                writers[dataset_name] = writer
                batches[dataset_name] = []
        store.clear()
    for dataset_name, remaining in batches.items():
        writer: Any = writers.get(dataset_name)
        if remaining:
            writer = file_storage.write_dataframe_batch(
                writer=writer,
                data=remaining,
                file_name=file_storage.get_file_name(
                    outdir, dataset.ALL_DATASETS[dataset_name].file_name, store.context
                ),
            )
        writer.close()
