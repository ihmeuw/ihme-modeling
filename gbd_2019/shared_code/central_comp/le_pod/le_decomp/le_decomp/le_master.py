import argparse
import os
from numpy import atleast_1d
from datetime import datetime
from multiprocessing import Queue, Process, Manager
from queue import Empty
import sys
import traceback
from typing import List, Union

from db_queries import (get_location_metadata,
                        get_cause_metadata)
from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles
from gbd import constants as gbd
from gbd_outputs_versions.gbd_process import GBDProcessVersion
from cluster_utils.io import makedirs_safely

import le_decomp
from le_decomp.le_parallel import run_le_parallel
from le_decomp.helper_functions import generate_paired_years
from le_decomp import validations
from le_decomp.constants import Database, Versioning


class LEMaster:

    _schema = "gbd"

    def __init__(self,
                 version_id: int,
                 output_dir: str,
                 cause_set_id: int,
                 cause_level: Union[int, str],
                 location_set_id: int,
                 compare_version_id: int,
                 gbd_round_id: int,
                 decomp_step: str,
                 sex_ids: List[str] = [1, 2, 3],
                 start_years: List[int] = None,
                 end_years: List[int] = None,
                 year_ids: List[int] = None,
                 env: str = 'prod',
                 verbose: bool = False):

        self.version_id = version_id
        self.output_dir = output_dir
        self.write_dir = os.path.join(output_dir, str(version_id))
        self.cause_set_id = cause_set_id
        self.location_set_id = location_set_id
        self.compare_version_id = compare_version_id
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.cause_ids = self.get_cause_ids(cause_level)
        self.sex_ids = list(atleast_1d(sex_ids))
        self.start_years, self.end_years = generate_paired_years(
            start_years, end_years, year_ids)
        self.env = validations.validate_env(env)
        self.verbose = verbose
        self._location_ids = None
        self._process_version_id = None
        self._conn_def = None

    @property
    def location_ids(self):

        if not self._location_ids:
            self._location_ids = get_location_metadata(
                location_set_id=self.location_set_id,
                gbd_round_id=self.gbd_round_id,
                decomp_step=self.decomp_step)[
            'location_id'].unique().tolist()
            # these print statements (see get_cause_ids below) also
            # serve the purpose of confirming that we're not calling
            # these methods in parallel
            print(f"Found {len(self._location_ids)} location_ids, "
                  f"location_set_id {self.location_set_id}")
        return self._location_ids

    def get_cause_ids(self, cause_level):
        """
        Returns a list of cause ids at a certain level of cause hierarchy,
        or, alternatively, all most detailed causes.
        In order for cause decomposition to work properly these causes
        must satisfy the classic GBD 'mutually exclusive and collectively
        exhaustive' rules for cause lists.
        """
        ch = get_cause_metadata(
            cause_set_id=self.cause_set_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step)
        validations.validate_cause_level(cause_level, ch)
        if cause_level == "most_detailed":
            cause_ids = ch.loc[ch[cause_level] == 1,
                               "cause_id"].unique().tolist()
        else:
            cause_ids = ch[((ch["level"] == cause_level) |
                            ((ch["level"] < cause_level) &
                             (ch["most_detailed"] == 1)))][
                "cause_id"].unique().tolist()
        print(f"Found {len(cause_ids)} cause_ids, cause_set_id "
              f"{self.cause_set_id} at cause level {cause_level}")
        return cause_ids

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
            gbd_process_version_note=(
                f"{datetime.now()} life expectancy decomp"),
            code_version=le_decomp.__version__,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata={},
            env=self.env).gbd_process_version_id
        return self._process_version_id

    def run_location_sex_decomp(self, location_id, sex_id):
        if self.verbose:
            print(f'Decomp location_id {location_id}, sex_id {sex_id}')
        run_le_parallel(self.write_dir, location_id, sex_id,
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
        /{output_dir}/{version_id}/{location_id}_{sex_id}.csv
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
        # get results
        results = []

        for job_num in range(run_jobs):
            proc_result = outq.get()
            results.append(proc_result)
            jobs_left = run_jobs - job_num
            if len(error_list) > 1:
                # kill processes and exit
                for p in procs:
                    p.terminate()
                inq.close()
                for _ in range(jobs_left):
                    try:
                        results.append(outq.get(False))
                    except Empty:
                        pass
                break

        # check for errors
        for result in results:
            if result[0]:
                print(result[2:])
                raise result[0].re_raise()
        print(f"File creation finished at {datetime.now().time()}")

    def upload(self):
        """
        Attempts to infile any .csv files found in the
        /{output_dir}/{version_id} directory to
        gbd.output_le_decomp_v{self.process_version_id}. Any
        .csv files not to be uploaded should be stashed in subfolders.
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
        infiler.indir(path=self.write_dir,
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

        makedirs_safely(self.write_dir)
        print(f"Output directory {self.write_dir} created.")

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


if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ver",
        "--version_id",
        help="LE decomp version to upload to",
        type=int)
    parser.add_argument(
        "-out",
        "--output_dir",
        help="root directory to save files to",
        type=str)
    parser.add_argument(
        "-clev",
        "--cause_level",
        help="cause level to use",
        default='most_detailed',
        type=str)
    parser.add_argument(
        "-cset",
        "--cause_set_id",
        help="cause set id to use",
        default=2,
        type=int)
    parser.add_argument(
        "-locs",
        "--location_set_id",
        help="location set id to use",
        default=35,
        type=int)
    parser.add_argument(
        "-sy",
        "--start_years",
        help="start years",
        nargs='+',
        default=[],
        type=int)
    parser.add_argument(
        "-ey",
        "--end_years",
        help="end years",
        nargs='+',
        default=[],
        type=int)
    parser.add_argument(
        "-y",
        "--year_ids",
        help="Will calculate all possible year ranges",
        nargs='+',
        default=[],
        type=int)
    parser.add_argument(
        "-cv",
        "--compare_version",
        help="compare version to pull cause specific mx",
        type=int)
    parser.add_argument(
        "-gr",
        "--gbd_round_id",
        help="gbd_round_id to run on",
        default=gbd.GBD_ROUND_ID,
        type=int)
    parser.add_argument(
        "-ds",
        "--decomp_step",
        help="string identifier for gbd decomp step",
        type=str)
    parser.add_argument(
        "-s",
        "--sex_ids",
        help="sex_ids",
        nargs='+',
        default=[1, 2, 3],
        type=int)
    parser.add_argument(
        "-env",
        "--environment",
        help="dev or prod db",
        default='dev',
        type=str)
    parser.add_argument(
        "-v",
        "--verbose",
        help="Keep track of file writing progress",
        default=False,
        action='store_true')
    parser.add_argument(
        "-n",
        "--n_processes",
        help="Number of jobs by location/sex to run in parallel",
        default=8,
        type=int)

    args = parser.parse_args()
    version_id = args.version_id
    output_dir = args.output_dir
    try:
        cause_level = int(args.cause_level)
    except ValueError:
        cause_level = args.cause_level
    cause_set_id = args.cause_set_id
    location_set_id = args.location_set_id
    start_years = args.start_years
    end_years = args.end_years
    year_ids = args.year_ids
    sex_ids = args.sex_ids
    compare_version = args.compare_version
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    env = args.environment
    verbose = args.verbose
    n_processes = args.n_processes

    lem = LEMaster(version_id, output_dir, cause_set_id, cause_level,
                   location_set_id, compare_version, gbd_round_id,
                   decomp_step, sex_ids=sex_ids,
                   start_years=start_years, end_years=end_years,
                   year_ids=year_ids, env=env, verbose=verbose)
    lem.run_all(n_processes)
