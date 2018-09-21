import time

from hierarchies import dbtrees
from jobmon.sge import true_path
from jobmon.qmaster import JobQueue
from jobmon.central_job_monitor import CentralJobMonitor
from jobmon.executors.sge_exec import SGEExecutor
from jobmon.schedulers import RetryScheduler


from como.version import ComoVersion
from como.nonfatal import ComputeNonfatal
from como.cause import agg_causes
from como.sequela import agg_sequelae
from como.impairments import agg_impairment
from como.injuries import agg_injuries
from como.summarize import summ
from como.upload import (upload_cause_summaries, upload_sequela_summaries,
                         upload_rei_summaries, upload_inj_summaries)


def run_pipeline_nonfatal(
        como_dir, location_id=[], year_id=[], sex_id=[], age_group_id=[],
        measure_id=[], n_processes=23, n_simulants=40000, *args, **kwargs):
    """run the nonfatal calculation on most detailed demographics

    Args:
        como_dir (str):
        location_id (list, optional):
        year_id (list, optional):
        sex_id (list, optional):
        measure_id (list, optional):
        n_processes (int, optional):
        n_simulants (int, optional):

        *args and **kwargs are passed into the simulation as parameters
    """
    # resume the como version with stored parameters
    cv = ComoVersion(como_dir)
    cv.load_cache()

    # set up the nonfatal computation object for our demographic set
    cnf = ComputeNonfatal(cv, location_id=location_id, year_id=year_id,
                          sex_id=sex_id, age_group_id=age_group_id,
                          measure_id=measure_id)

    # import data
    cnf.import_data(n_processes=n_processes)

    # compute all results
    cnf.compute_results(n_simulants=n_simulants, n_processes=n_processes,
                        *args, **kwargs)

    # write results to disk
    cnf.write_results()


def run_pipeline_aggregate_locations(
        como_dir, component, year_id, sex_id, measure_id, location_set_id):
    # resume the como version with stored parameters
    cv = ComoVersion(como_dir)
    cv.load_cache()

    if component == "cause":
        agg_causes(cv, year_id, sex_id, measure_id, location_set_id)

    if component == "sequela":
        agg_sequelae(cv, year_id, sex_id, measure_id, location_set_id)

    if component == "impairment":
        agg_impairment(cv, year_id, sex_id, measure_id, location_set_id)

    if component == "injuries":
        agg_injuries(cv, year_id, sex_id, measure_id, location_set_id)


def run_pipeline_summarize(como_dir, component, location_id):
    cv = ComoVersion(como_dir)
    cv.load_cache()
    summ(cv, location_id, component)


def run_pipeline_upload(como_dir, component, location_id):
    cv = ComoVersion(como_dir)
    cv.load_cache()

    if component == 'cause':
        upload_cause_summaries(cv, location_id)
    elif component == 'sequela':
        upload_sequela_summaries(cv, location_id)
    elif component == 'impairment':
        upload_rei_summaries(cv, location_id)
    elif component == "injuries":
        upload_inj_summaries(cv, location_id)


def run_pipeline_como(
        root_dir,
        gbd_round_id=4,
        location_id=[],
        year_id=[],
        sex_id=[],
        age_group_id=[],
        measure_id=[],
        n_draws=1000,
        n_simulants=20000,
        components=["sequela", "cause", "impairment", "injuries"]):

    cv = ComoVersion.new(
        root_dir, gbd_round_id, location_id, year_id, sex_id, age_group_id,
        measure_id, n_draws, components)

    try:
        cjm = CentralJobMonitor(cv.como_dir, persistent=False)
        time.sleep(5)
    except Exception as e:
        raise e
    else:
        executor_params = {"request_timeout": 10000}
        jobq = JobQueue(cv.como_dir, scheduler=RetryScheduler,
                        executor=SGEExecutor, executor_params=executor_params)

        # run nonfatal pipeline by location/year/sex
        parallelism = ["location_id", "sex_id"]
        for slices in cv.dimensions.index_slices(parallelism):
            jobname = "como_e_sim_{location_id}_{sex_id}".format(
                location_id=slices[0], sex_id=slices[1])
            job = jobq.create_job(
                jobname=jobname,
                runfile=true_path(executable="compute_nonfatal"),
                parameters=[
                    "--como_dir", cv.como_dir,
                    "--location_id", str(slices[0]),
                    "--sex_id", str(slices[1]),
                    "--n_processes", "23",
                    "--n_simulants", str(n_simulants)
                ])
            jobq.queue_job(
                job,
                slots=50,
                memory=400,
                project="proj_como",
                process_timeout=(60 * 180))
        jobq.block_till_done(stop_scheduler_when_done=False)

        # run aggregation by year/sex/measure
        parallelism = ["year_id", "sex_id", "measure_id"]
        for slices in cv.dimensions.index_slices(parallelism):
            for component in cv.components:
                if component != "sequela":
                    loc_sets = [35, 40]
                else:
                    loc_sets = [35]
                for location_set_id in loc_sets:
                    jobname = (
                        "como_e_agg_{component}_{year_id}_{sex_id}"
                        "_{measure_id}_{location_set_id}").format(
                            component=component,
                            year_id=slices[0],
                            sex_id=slices[1],
                            measure_id=slices[2],
                            location_set_id=location_set_id)
                    job = jobq.create_job(
                        jobname=jobname,
                        runfile=true_path(executable="aggregate_nonfatal"),
                        parameters=[
                            "--como_dir", cv.como_dir,
                            "--component", component,
                            "--year_id", str(slices[0]),
                            "--sex_id", str(slices[1]),
                            "--measure_id", str(slices[2]),
                            "--location_set_id", str(location_set_id)
                        ])
                    jobq.queue_job(
                        job,
                        slots=25,
                        memory=200,
                        project="proj_como",
                        process_timeout=(60 * 600))
        jobq.block_till_done(stop_scheduler_when_done=False)

        # run summaries by component/location
        lt = dbtrees.loctree(None, 35)
        sdi_lts = dbtrees.loctree(None, 40, return_many=True)
        locs = [l.id for l in lt.nodes]
        sdi_locs = [l.root.id for l in sdi_lts]
        for component in cv.components:
            if component != "sequela":
                summ_locs = locs + sdi_locs
            else:
                summ_locs = locs[:]
            for location_id in summ_locs:
                jobname = "como_e_summ_{component}_{location_id}".format(
                    component=component,
                    location_id=location_id)
                job = jobq.create_job(
                    jobname=jobname,
                    runfile=true_path(executable="summarize_nonfatal"),
                    parameters=[
                        "--como_dir", cv.como_dir,
                        "--component", component,
                        "--location_id", str(location_id)
                    ])
                jobq.queue_job(
                    job,
                    slots=48,
                    memory=96,
                    project="proj_como",
                    process_timeout=(60 * 240))
        jobq.block_till_done(stop_scheduler_when_done=False)

        for component in cv.components:
            jobname = "como_e_upload_{component}".format(component=component)
            job = jobq.create_job(
                jobname=jobname,
                runfile=true_path(executable="upload_nonfatal"),
                parameters=[
                    "--como_dir", cv.como_dir,
                    "--component", component,
                    "--location_id", " ".join(
                        [str(l) for l in locs + sdi_locs])
                ])
            jobq.queue_job(
                job,
                slots=20,
                memory=40,
                project="proj_como",
                process_timeout=(60 * 720))
        jobq.block_till_done()

    finally:
        cjm.generate_report()
        cjm.stop_responder()
        cjm.stop_publisher()
