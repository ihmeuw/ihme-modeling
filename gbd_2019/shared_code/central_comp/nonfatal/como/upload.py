from multiprocessing import Queue, Process
from sqlalchemy.exc import IntegrityError

from db_tools.ezfuncs import get_session
from db_tools.loaders import Infiles

from como.common import ExceptionWrapper


class LoadNonfatal:

    single_year = {
        "cause": "output_epi_single_year_v{}",
        "impairment": "output_impairment_single_year_v{}",
        "injuries": "output_injury_single_year_v{}",
        "sequela": "output_sequela_single_year_v{}"
    }
    multi_year = {
        "cause": "output_epi_multi_year_v{}",
        "impairment": "output_impairment_multi_year_v{}",
        "injuries": "output_injury_multi_year_v{}",
        "sequela": "output_sequela_multi_year_v{}"
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
                        params={
                            'table': table,
                            'location_id': loc
                            })
                    sesh.commit()
                except:
                    pass
        sesh.close()

    def load_component_location(self, component, location_id, year_type):
        table_tmp = getattr(self, year_type)[component]
        table = table_tmp.format(self.como_version.gbd_process_version_id)
        sesh = get_session("gbd")
        infiler = Infiles(table, "gbd", sesh)
        if year_type == "single_year":
            indir_glob = f"{self.como_version.como_dir}/summaries/{component}/*/{year_type}/{location_id}/*.csv"
        else:
            indir_glob = f"{self.como_version.como_dir}/summaries/{component}/*/{year_type}/{location_id}.csv"
        infiler.indir(path=indir_glob, commit=True, partial_commit=True,
                      rename_cols={"mean": "val"}, no_raise=(IntegrityError))

    def _q_upload_component_location(self, in_q, out_q):
        for params in iter(in_q.get, None):
            try:
                self.load_component_location(*params)
                out_q.put((False, params))
            except Exception as e:
                out_q.put((ExceptionWrapper(e), params))

    def run_all_uploads_mp(self, n_processes=20):

        # spin up xcom queues
        inq = Queue()
        outq = Queue()

        # figure out how many processes we need. if we did pct change then we
        # need double
        total_procs = len(self.components) * len(self.location_id)
        if self.como_version.change_years:
            total_procs = total_procs * 2

        # spin up the processes
        procs = []
        min_procs = min([n_processes, total_procs])
        for i in range(min_procs):
            # run upload in parallel
            p = Process(target=self._q_upload_component_location,
                        args=(inq, outq))
            procs.append(p)
            p.start()

        # upload summaries
        for component in self.components:
            for location_id in self.location_id:
                inq.put((component, location_id, "single_year"))
                if self.como_version.change_years:
                    inq.put((component, location_id, "multi_year"))

        # make the workers die after
        for _ in procs:
            inq.put(None)

        # get results
        results = []
        for _ in self.components:
            for _ in self.location_id:
                proc_result = outq.get()
                results.append(proc_result)
                if self.como_version.change_years:
                    proc_result = outq.get()
                    results.append(proc_result)

        # check for errors
        for result in results:
            if result[0]:
                print(result[1:])
                raise result[0].re_raise()


def run_upload(como_version, location_id, n_processes=20):
    loader = LoadNonfatal(como_version, location_id)
    loader.make_partitions("single_year")
    if como_version.change_years:
        loader.make_partitions("multi_year")
    loader.run_all_uploads_mp(n_processes)
    # run 2 times to confirm all uploaded
    loader.run_all_uploads_mp(n_processes)
