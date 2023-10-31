from loguru import logger
from multiprocessing import Queue, Process
from sqlalchemy.exc import IntegrityError, OperationalError, ResourceClosedError

from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles

from como.legacy.common import ExceptionWrapper


class LoadNonfatal:

    single_year = {
        "cause": "output_epi_single_year_v{}",
        "impairment": "output_impairment_single_year_v{}",
        "injuries": "output_injury_single_year_v{}",
        "sequela": "output_sequela_single_year_v{}",
    }
    multi_year = {
        "cause": "output_epi_multi_year_v{}",
        "impairment": "output_impairment_multi_year_v{}",
        "injuries": "output_injury_multi_year_v{}",
        "sequela": "output_sequela_multi_year_v{}",
    }

    def __init__(self, como_version, location_id):
        self.como_version = como_version
        self.location_id = sorted(location_id)

    @property
    def components(self):
        return self.como_version.components

    def make_partitions(self, year_type):
        sesh = get_session("gbd")
        for component in self.components:
            table_tmp = getattr(self, year_type)[component]
            table = table_tmp.format(self.como_version.gbd_process_version_id)
            for loc in self.location_id:
                try:
                    sesh.execute(
                        "CALL gbd.add_partition('gbd', :table, :location_id)",
                        params={"table": table, "location_id": loc},
                    )
                    sesh.commit()
                except Exception as e:
                    logger.error(
                        f"make_partitions(CALL gbd.add_parition('gbd' table: {table}, location_id: {loc}). Exception: {e}"
                    )
                    pass
        sesh.close()

    def load_component_location(self, component, location_id, year_type):
        table_tmp = getattr(self, year_type)[component]
        table = table_tmp.format(self.como_version.gbd_process_version_id)
        sesh = get_session("gbd")
        infiler = Infiles(table, "gbd", sesh)
        if year_type == "single_year":
            indir_glob = f"FILEPATH"
        else:
            indir_glob = f"FILEPATH"
        infiler.indir(
            path=indir_glob,
            commit=True,
            partial_commit=True,
            rename_cols={"mean": "val"},
            no_raise=(IntegrityError, OperationalError, ResourceClosedError),
        )

    def _q_upload_component_location(self, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                self.load_component_location(*params)
                out_q.put((False, params))
            except Exception as e:
                logger.error(
                    f"q_upload_component_location({in_q}, {out_q}); params: {params}. Exception: {e}"
                )
                out_q.put((ExceptionWrapper(e), params))

    def run_all_uploads_mp(self, n_processes):

        inq = Queue()
        outq = Queue()

        total_procs = len(self.components) * len(self.location_id)
        if self.como_version.change_years:
            total_procs = total_procs * 2

        procs = []
        min_procs = min([n_processes, total_procs])
        for i in range(min_procs):
            p = Process(target=self._q_upload_component_location, args=(inq, outq))
            procs.append(p)
            p.start()

        for component in self.components:
            for location_id in self.location_id:
                logger.info(f"uploading {component} location_id: {location_id} summaries")
                inq.put((component, location_id, "single_year"))
                if self.como_version.change_years:
                    inq.put((component, location_id, "multi_year"))

        for _ in procs:
            inq.put(None)

        results = []
        for _ in self.components:
            for _ in self.location_id:
                proc_result = outq.get()
                results.append(proc_result)
                if self.como_version.change_years:
                    proc_result = outq.get()
                    results.append(proc_result)

        for result in results:
            if result[0]:
                logger.info(f"error in upload result: {result[1:]}")
                raise result[0].re_raise()


def run_upload(como_version, location_id, n_processes=8):
    loader = LoadNonfatal(como_version, location_id)
    loader.make_partitions("single_year")
    if como_version.change_years:
        loader.make_partitions("multi_year")
    loader.run_all_uploads_mp(n_processes)
