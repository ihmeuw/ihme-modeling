"""
run_inpatient.py
"""
import os
import shutil
import sys
from pathlib import Path

from crosscutting_functions.clinical_metadata_utils.api.pipeline_wrappers import (
    InpatientWrappers,
)

from inpatient.Clinical_Runs import inpatient
from inpatient.Clinical_Runs.utils.constants import InpRunSettings, RunDBSettings


def instantiate_inp_wrappers(run_id, odbc_profile=RunDBSettings.db_profile):
    """Internal method to instantiate the inpatient wrappers"""
    return InpatientWrappers(run_id, odbc_profile)


def validate_metadata_is_initialized(run_id):
    """Internal method to ensure that run_metadata has been
    initialized for this run_id"""

    iw = instantiate_inp_wrappers(run_id)
    run_metadata = iw.pull_run_metadata()
    if len(run_metadata) == 0:
        raise ValueError(
            "Supplied run_id has not yet been instantiated in the "
            "database. Please use iw.initialize_run_metadata before "
            "retrying to run the pipeline."
        )


def make_dir(run_id):
    """
    creates the new run_id folder tree using the structure from the last run
    excludes data files stored as csv, hdf and dta
    """
    bases = [
        FILEPATH
    ]
    for base in bases:
        if Path(base).joinpath(f"run_{run_id}").is_dir():
            print(f"Run direcotry {base} is present")
        else:
            dir_creator(base, run_id)
            print(f"New run created in {base}")


def dir_creator(base, run_id):
    src = "{}/run_{}".format(base, run_id - 1)
    dst = "{}/run_{}".format(base, run_id)
    ign = shutil.ignore_patterns(
        "*.csv",
        "*.H5",
        "*.dta",
        "*.pkl",
        "*.p",
        "*.log",
        "*.parquet",
        "*.e*",
        "*.o*",
        "*.clinical",
        "*.pdf",
        "*.PDF",
        "*.jpeg",
        "*.png",
        "*.yaml",
    )
    shutil.copytree(src, dst, ignore=ign)

    # only adjust perms in original run structure for now
    if base == FILEPATH:
        adjust_perms(run_id)


def adjust_perms(run):
    """There doesnt seem to be a recursive option in os.chmod so we'll glob all
    the paths we need and then modify them 1 by 1 in a for loop"""

    # get a list of paths to modify
    paths = sorted(Path(FILEPATH).rglob("*"))

    # loop over and set to 775 one by one
    for p in paths:
        os.chmod(p, 0o775)


def setup_run(run_metadata):
    # Instantiate the inpatient class
    inp = inpatient.Inpatient(
        run_id=int(run_metadata.run_id[0]),
        clinical_age_group_set_id=InpRunSettings.clinical_age_group_set_id,
        map_version=int(run_metadata.map_version[0]),
    )

    # Define some model version IDs located in inp_run_metadata
    inp.central_lookup = {
        "haqi": {
            "covariate_id": 1099,
            "model_version_id": int(run_metadata.haqi_model_version_id[0]),
        },
        "live_births_by_sex": {
            "covariate_id": 1106,
            "model_version_id": (int(run_metadata.live_births_by_sex_model_version_id[0])),
        },
        "ASFR": {
            "covariate_id": 13,
            "model_version_id": int(run_metadata.asfr_model_version_id[0]),
        },
        "IFD_coverage_prop": {
            "covariate_id": 51,
            "model_version_id": (int(run_metadata.ifd_coverage_prop_model_version_id[0])),
        },
        "population": {
            "run_id": int(run_metadata.population_run_id[0]),
        },
    }

    inp.automater()
    return inp


def main(run_id, inp=None, pick_up_step=None):
    # Instantiate the InpatientWrappers
    iw = instantiate_inp_wrappers(run_id)

    # Pull run metadata
    run_metadata = iw.pull_run_metadata()

    if not Path(FILEPATH).is_dir():
        # Make directories and copy files
        print("Making directories and copying required data")
        make_dir(run_id)

    print("Setting up the run")
    inp = setup_run(run_metadata)

    print(inp)

    # arg=False allows the automater method to choose where to start
    print(f"Beginning run with step {inp.begin_with}")
    inp.main(control_begin_with=False)


if __name__ == "__main__":
    run_id = int(sys.argv[1])

    # Ensure that the run_metadata exists for the
    # provided run_id
    validate_metadata_is_initialized(run_id)

    main(run_id)
