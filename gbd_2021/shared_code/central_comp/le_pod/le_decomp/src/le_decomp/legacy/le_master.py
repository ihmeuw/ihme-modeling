import argparse
import os
from numpy import atleast_1d
from datetime import datetime
from multiprocessing import Queue, Process, Manager
from queue import Empty
import sys
import traceback
from typing import List, Tuple, Union

import pandas as pd

import db_queries
from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles
from gbd import constants as gbd
from gbd_outputs_versions.gbd_process import GBDProcessVersion
from cluster_utils.io import makedirs_safely

from le_decomp.legacy.le_parallel import run_le_parallel
from le_decomp.legacy import validations
from le_decomp.lib import files, utils
from le_decomp.lib.constants import AgeGroup, Database, Filepaths, Versioning


class LEMaster:

    _schema = "gbd"

    def __init__(self,
                 cause_set_id: int,
                 cause_level: Union[int, str],
                 location_set_id: int,
                 compare_version_id: int,
                 gbd_round_id: int,
                 decomp_step: str,
                 sex_ids: List[int] = [1, 2, 3],
                 start_years: List[int] = None,
                 end_years: List[int] = None,
                 year_ids: List[int] = None,
                 env: str = 'prod',
                 verbose: bool = False):

        self.version_id = files.get_new_version_id()
        self.output_dir = files.get_output_dir(self.version_id)
        self.file_system = files.LeDecompFileSystem(self.output_dir)
        self.cause_set_id = cause_set_id
        self.cause_level = cause_level
        self.location_set_id = location_set_id
        self.compare_version_id = compare_version_id
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.sex_ids = list(atleast_1d(sex_ids))
        self.env = validations.validate_env(env)
        self.verbose = verbose
        self._process_version_id = None
        self._conn_def = None

        self.location_set_version_id, self.location_ids = self._set_up_location_metadata()
        self.cause_set_version_id, self.cause_ids = self._set_up_cause_metadata(cause_level)
        self.start_years, self.end_years = utils.generate_paired_years(
            start_years, end_years, year_ids
        )
        self.life_table_version_id = self._get_lifetable_version_id()

    def _set_up_location_metadata(self) -> Tuple[int, List[int]]:
        """Set up location metadata: location set version id and location ids.

        Returns:
            tuple: location set version id, list of location ids.
        """
        location_metadata = db_queries.get_location_metadata(
            location_set_id=self.location_set_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step
        )
        location_set_version_id = location_metadata["location_set_version_id"].iat[0]
        location_ids = location_metadata["location_id"].unique().tolist()

        print(
            f"Found {len(location_ids)} location_ids, location_set_id {self.location_set_id}"
        )
        return location_set_version_id, location_ids

    def _set_up_cause_metadata(self, cause_level: Union[int, str]) -> Tuple[int, List[int]]:
        """Returns cause set version id and list of cause ids at a certain level of
        cause hierarchy, or, alternatively, all most detailed causes.

        In order for cause decomposition to work properly, these causes
        must satisfy the classic GBD 'mutually exclusive and collectively
        exhaustive' rules for cause lists.

        Returns:
            tuple: cause set version id, list of cause ids
        """
        cause_metadata = db_queries.get_cause_metadata(
            cause_set_id=self.cause_set_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step
        )
        cause_set_version_id = cause_metadata["cause_set_version_id"].iat[0]
        validations.validate_cause_level(cause_level, cause_metadata)

        if cause_level == "most_detailed":
            cause_ids = cause_metadata.loc[
                cause_metadata[cause_level] == 1, "cause_id"
            ].unique().tolist()
        else:
            cause_ids = cause_metadata[
                ((cause_metadata["level"] == cause_level) |
                ((cause_metadata["level"] < cause_level) &
                 (cause_metadata["most_detailed"] == 1)))
            ]["cause_id"].unique().tolist()

        print(
            f"Found {len(cause_ids)} cause_ids, cause_set_id "
            f"{self.cause_set_id} at cause level {cause_level}"
        )
        return cause_set_version_id, cause_ids

    def _get_lifetable_version_id(self) -> int:
        """Returns the life table with shock version (run) id to be used in this run.

        Ask for the least amount of data; we only care about the active version.
        """
        return db_queries.get_life_table_with_shock(
            location_id=1,
            year_id=self.start_years[0],
            sex_id=self.sex_ids[0],
            age_group_id=AgeGroup.OLDEST_LIFE_TABLE_AGE_ID,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
        )["run_id"].iat[0]

    @property
    def conn_def(self):
        if not self._conn_def:
            self._conn_def = Database.conn_def[self.env.value]
        return self._conn_def

    @conn_def.setter
    def conn_def(self, val):
        self._conn_def = val

    def create_process_version(self):
        """
        Returns a process version id as a result of
        calling the gbd.new_process_version sproc.  No
        results can be uploaded until a process_version is
        created.
        """
        self._process_version_id = GBDProcessVersion.add_new_version(
            gbd_process_id=Versioning.DECOMP_PROCESS_ID,
            gbd_process_version_note=Versioning.VERSION_NOTE.format(
                version_id=self.version_id,
                life_table_version_id=self.life_table_version_id,
            ),
            code_version=utils.get_code_version(),
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata={},
            env=self.env,
            compare_version_id=self.compare_version_id,
        ).gbd_process_version_id

        return self._process_version_id

    def cache_input_parameters(self) -> None:
        """Caches the input parameters for this run.

        This involves throwing all the parameters of interest into a dictionary,
        converting the dictionary to a dataframe, and saving.
        """
        parameters = {}
        parameters["version_id"] = self.version_id
        parameters["gbd_round_id"] = self.gbd_round_id
        parameters["decomp_step"] = self.decomp_step
        parameters["cause_set_id"] = self.cause_set_id
        parameters["cause_set_version_id"] = self.cause_set_version_id
        parameters["cause_level"] = self.cause_level
        parameters["location_set_id"] = self.location_set_id
        parameters["location_set_version_id"] = self.location_set_version_id
        parameters["compare_version_id"] = self.compare_version_id
        parameters["process_version_id"] = self._process_version_id
        parameters["sex_ids"] = ", ".join([str(sex_id) for sex_id in self.sex_ids])
        parameters["env"] = self.env
        parameters["start_years"] = ", ".join([str(year) for year in self.start_years])
        parameters["end_years"] = ", ".join([str(year) for year in self.end_years])
        parameters["life_table_version_id"] = self.life_table_version_id

        self.file_system.cache_inputs(pd.DataFrame([parameters]))

    def run_location_sex_decomp(self, location_id, sex_id):
        if self.verbose:
            print(f'Decomp location_id {location_id}, sex_id {sex_id}')
        run_le_parallel(self.file_system, location_id, sex_id,
                        self.start_years, self.end_years, self.cause_ids,
                        self.compare_version_id, self.gbd_round_id,
                        self.decomp_step)

    def _q_decomp_parallel(self, inq, outq, error_list):

        for params in iter(inq.get, None):
            try:
                self.run_location_sex_decomp(*params)
                outq.put((False, params))
            except Exception as e:
                tb = traceback.format_exc()
                outq.put((ExceptionWrapper(e), tb, params))
                error_list.append(1)

    def run_decomp(self, n_processes: int=8):
        """
        Runs life-expectancy decomposition in parallel over
        each location-sex combination, saving .csv files of the results
        in the directory format:
        /{output_dir}/results/{location_id}_{sex_id}.csv
        """
        print(f"Starting file creation at {datetime.now().time()}")
        inq = Queue()
        outq = Queue()
        mng = Manager()
        error_list = mng.list()

        procs = []
        for i in range(n_processes):
            p = Process(target=self._q_decomp_parallel,
                        args=(inq, outq, error_list))
            procs.append(p)
            p.start()

        run_jobs = 0
        for location_id in self.location_ids:
            for sex_id in self.sex_ids:
                inq.put((location_id, sex_id,))
                run_jobs += 1

        for _ in procs:
            inq.put(None)

        results = []

        for job_num in range(run_jobs):
            proc_result = outq.get()
            results.append(proc_result)
            jobs_left = run_jobs - job_num
            if len(error_list) > 1:
                for p in procs:
                    p.terminate()
                inq.close()
                for _ in range(jobs_left):
                    try:
                        results.append(outq.get(False))
                    except Empty:
                        pass
                break

        for result in results:
            if result[0]:
                print(result[2:])
                raise result[0].re_raise()
        print(f"File creation finished at {datetime.now().time()}")

    def upload(self):
        """
        Attempts to infile any .csv files found in the
        output_dir directory to gbd.output_le_decomp_v{self.process_version_id}.
        Any .csv files not to be uploaded should be stashed in subfolders.
        """
        if not self._process_version_id:
            raise RuntimeError("A process version must be created "
                               "before results can be uploaded.")
        table_name = list(GBDProcessVersion(
            self._process_version_id, env=self.env).tables)[0]
        session = get_session(conn_def=self.conn_def)

        infiler = Infiles(table=table_name,
                          schema=self._schema,
                          session=session)

        print(f"Starting upload at {datetime.now().time()}")
        infiler.indir(path=self.file_system.get_directory_path(Filepaths.RESULTS),
                      partial_commit=True,
                      sort_files=True)
        session.close()
        print(f"Finished upload at {datetime.now().time()}")

    def run_all(self, n_processes: int=8):
        """
        Creates a process_version (with db tables), performs
        cause decomposition, saves to file, and uploads results to
        the db. Use the n_processes argument to determine how many
        parallel processes to run simultaneously, keeping in mind
        that each hits the database a couple of times.
        """
        pvid = self.create_process_version()
        print(f"Process version id {pvid} created on the "
              f"gbd {self.env.value} database.")

        self.file_system.make_file_system()
        print(f"Output directory {self.output_dir} created.")

        self.cache_input_parameters()
        print(f"Cached input parameters.")

        self.run_decomp(n_processes=n_processes)
        self.upload()
        process_version = GBDProcessVersion(pvid, env=self.env)
        process_version._update_status(
            gbd.gbd_process_version_status['ACTIVE'])
        print(f"Process version id {pvid} marked active. Run completed")


class ExceptionWrapper:

    def __init__(self, ee):
        self.ee = ee
        _, _, self.tb = sys.exc_info()

    def re_raise(self):
        raise self.ee.with_traceback(self.tb)
