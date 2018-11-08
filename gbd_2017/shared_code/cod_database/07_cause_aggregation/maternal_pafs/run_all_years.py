import sys
import getpass
USER = getpass.getuser()
repo_dir = "FILEPATH"
sys.path.append(repo_dir)
from cod_prep.utils.cluster_tools import submit_cod


def run_all_years(process, year, rerun_pafs=False):
    slots = 5
    language = "python"
    holds = []
    if rerun_pafs:
        worker = "FILEPATH"
        jobname = "pafs_{}".format(year)
        params = [year]
        print("Submitting jobs to create PAFs")
        pafs_jid = submit_cod(jobname, slots, language, worker, params, logging=True)
        holds.append(pafs_jid)

    jobname = "{p}_{y}".format(p=process, y=year)
    worker = "FILEPATH"
    params = [year, process]
    print("Submitting jobs for {}".format(process))
    submit_cod(jobname, slots, language, worker, params, holds, logging=True)


if __name__ == "__main__":
    process = sys.argv[1]
    rerun_pafs = raw_input("Create new PAFs?: ")
    rerun_pafs = (rerun_pafs[0].lower() == 'y')
    assert process in ["cod_props", "maternal_hiv"], \
        "invalid process, please enter one of cod_props or maternal_hiv"
    for year in range(1980, 2018):
        run_all_years(process, year, rerun_pafs)
