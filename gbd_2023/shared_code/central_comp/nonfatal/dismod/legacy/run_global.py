import logging
import os
import sys
import warnings
from argparse import ArgumentParser, Namespace

import numpy as np
import pandas as pd

from cascade_ode.legacy import standard_locations
from cascade_ode.legacy.argument_parser import cascade_parser
from cascade_ode.legacy.constants import CLIArgs, Methods
from cascade_ode.legacy.dag import check_error_msg_for_sigkill
from cascade_ode.legacy.drill import Cascade, Cascade_loc
from cascade_ode.legacy.emr.data import NoNonZeroValues
from cascade_ode.legacy.emr.emr import InsufficientInputs, NoEMRCalculated, dismod_emr
from cascade_ode.legacy.io import datastore
from cascade_ode.legacy.patch_io import setup_io_patches
from cascade_ode.legacy.release import get_release_method_from_mv_and_flags
from cascade_ode.legacy.run_all import prepare_directories
from cascade_ode.legacy.settings import load as load_settings
from cascade_ode.legacy.setup_logger import setup_logger
from cascade_ode.legacy.upload import update_run_time

log = logging.getLogger(__name__)
log.info("run_global started")

# Get configuration options
settings = load_settings()

# Set default file mask to readable-for all users
os.umask(0o0002)


# Disable warnings
def nowarn(message, category, filename, lineno, file=None, line=None):
    pass


warnings.showwarning = nowarn


# CUSTOM EXCEPTIONS
class InvalidSettings(Exception):
    pass


def generate_world(cascade, reimport, feature_flags=None):
    """
    generate global cascade_loc. Optionally reimport to refresh
    data/files
    """
    cl = Cascade_loc(
        1, 0, 2000, cascade, timespan=50, reimport=reimport, feature_flags=feature_flags
    )
    cl.initialize()
    return cl


def get_SL_betas_and_run_world(cascade, disable_nonstandard_locations=False):
    """
    Compute standard location betas and then run global DisMod
    with those SL betas applied as priors.

    Arguments:
        cascade (cascade_ode.legacy.drill.Cascade)
        disable_nonstandard_locations (bool): Whether to add betas for
            standard locations.

    Returns:
        None
    """
    log = logging.getLogger(__name__)
    cascade_loc = generate_world(cascade, reimport=True)

    if not disable_nonstandard_locations:
        log.info("Extracting standard location betas")
        betas = standard_locations.extract_betas(cascade_loc=cascade_loc)
        # lets save them for reference
        betas.to_csv(, index=False)

        # after beta generation, we need to regenerate cascade_loc because
        # we stripped non-standard locations
        cascade_loc = generate_world(cascade, reimport=True)
        standard_locations.apply_betas(betas=betas, cascade_loc=cascade_loc)

        # now that we have applied betas, we can re-run the world with full
        # location set
        update_run_time(cascade.model_version_id)
        log.info("Rerunning world with Standard Location betas")
    else:
        log.info("Skipping standard locations")
    run_world(cascade_loc)
    log.info("world done rerunning")


def run_world(cascade_loc):
    """
    run DisMod for a global cascade_loc object

    Since we're running DisMod, that means this function reads from and writes
    to file system.

    Args:
        cascade_loc (drill.cascade.Cascade_loc): global Cascade_loc object of
            the model_version_id we're running DisMod for

    Returns:
        None
    """
    log = logging.getLogger(__name__)

    log.info("Starting global DisMod")
    cascade_loc.run_dismod()
    log.info("DisMod finished")
    log.info("summarizing posterior")
    cascade_loc.summarize_posterior()
    log.info("summarizing posterior finished")
    log.info("begin draw")
    cascade_loc.draw()
    log.info("draw finished")
    log.info("beginning predict")
    cascade_loc.predict()
    log.info("predict finished")
    log.info("saving outputs")
    datastore.save_datastores(
        [cascade_loc.datastore], outdir=cascade_loc.out_dir, batchsize=10_000
    )


class GlobalCascade(object):
    """
    The GlobalCascade object is responsible for running the initial
    global DisMod model.
    """

    def __init__(
        self, mvid, cv_iter_id, no_upload=False, disable_nonstandard_locations=False
    ):
        """
        Initializing the GlobalCascade involves instantiating drill.Cascade
        for the first time, which means this method writes to the file system
        and potentially promotes data t2 -> t3 .

        Args:
            mvid (int): model version ID
            cv_iter_id (int): Integer in range 0-10. 0 if no cross validation.
            no_upload (bool): Turn off EMR upload because it's harder to mock.
            disable_nonstandard_locations (bool): Whether to add
                betas to locations.
        """
        self.mvid = mvid
        self.cv_iter_id = cv_iter_id
        self.no_upload = no_upload
        self.disable_nonstandard_locations = disable_nonstandard_locations

        self.logdir = 
        log = logging.getLogger(__name__)
        log.info(
            "Beginning GlobalCascade with mvid {} and cv_iter_id {}".format(mvid, cv_iter_id)
        )

        log.info("Beginning cascade creation")
        self.cascade = Cascade(mvid, reimport=True, cv_iter=cv_iter_id)
        log.info("Done with cascade creation")

        self.meid = self.cascade.model_version_meta.modelable_entity_id.values[0]
        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.is_tb = True
        else:
            self.is_tb = False
        self.has_csmr = "mtspecific" in self.cascade.data.integrand.unique()

    def run_global(self, feature_flags=None):
        """
        The initial global cascade run is actually a series of runs.

        2 runs are always going to happen -- 1 run with non-standard locations
        removed in order to get betas, and then another run involving all
        locations. The 2nd run will use betas calculated from the first as
        priors.

        Optionally, there is a 3rd run that can occur before the other 2. If
        EMR calculation is specified in settings, we'll run an initial global
        DisMod model to generate adjusted inputs into the EMR calculation.

        If EMR runs, that means this method uploads EMR results to Epi db.

        Args:
            feature_flags (SimpleNamespace): Flags to send to computation.
        """

        log = logging.getLogger(__name__)
        log.info("Starting GlobalCascade.run_global")

        csmr_cause_id = self.cascade.model_version_meta.add_csmr_cause.values[0]
        if csmr_cause_id is None:
            csmr_cause_id = np.nan
        ccvid = self.cascade.model_version_meta.csmr_cod_output_version_id
        ccvid = ccvid.values[0]
        remdf = self.cascade.model_params.query("parameter_type_id == 1 & measure_id == 7")
        if len(remdf) > 0:
            remdf = remdf[
                [
                    "parameter_type_id",
                    "measure_id",
                    "age_start",
                    "age_end",
                    "lower",
                    "mean",
                    "upper",
                ]
            ]
        else:
            remdf = None

        should_run_emr = (
            self.cv_iter_id == 0
            and (not np.isnan(csmr_cause_id) or self.has_csmr)
            and (not self.is_tb)
        )
        emr_disabled_setting = self.cascade.model_version_meta.get("disable_emr", pd.Series())
        emr_is_disabled = emr_disabled_setting.unique().tolist() == [1]

        if should_run_emr and not emr_is_disabled:

            # Check whether there is a value constraint on EMR (in which case
            # we cannot compute EMR)
            # TODO: Move input parameter checking to its own module
            emr_prior = self.cascade.model_params.query(
                "parameter_type_id == 1 & measure_id == 9"
            )
            if len(emr_prior) == 1:
                zero_EMR_prior = (
                    emr_prior.lower.squeeze() == 0
                    and emr_prior.upper.squeeze() == 0
                    and emr_prior.age_start.squeeze() == 0
                    and emr_prior.age_end.squeeze() >= 100
                )
                if zero_EMR_prior:
                    raise InvalidSettings(
                        "Cannot set a value prior of 0 for "
                        "EMR for ages 0-100 while also "
                        "triggering EMR calculation via "
                        "cause/remission settings"
                    )

            # Use CSMR data from codcorrect if requested, otherwise
            # use the user-provided data
            if np.isnan(csmr_cause_id):
                csmr_type = "custom"
            else:
                csmr_type = "cod"

            update_run_time(self.mvid)

            # remove EMR data from global cascade_loc before running DisMod
            cascade_loc = generate_world(
                self.cascade, reimport=True, feature_flags=feature_flags
            )
            sex_id_is_both = 0
            cascade_loc.gen_data(sex_id_is_both, drop_emr=True)
            run_world(cascade_loc)

            # try to compute/upload EMR
            release_id = self.cascade.model_version_meta.release_id.values[0]
            try:
                dismod_emr(
                    self.mvid,
                    envr=settings["env_variables"]["ENVIRONMENT_NAME"],
                    remission_df=remdf,
                    csmr_type=csmr_type,
                    release_id=release_id,
                    no_upload=self.no_upload,
                )
            except (NoNonZeroValues, InsufficientInputs, NoEMRCalculated) as e:
                log.info(
                    (
                        "EMR calculation started but did not complete due to "
                        "insufficient inputs. Skipping. Error was '{}'".format(e)
                    )
                )
            else:
                log.info("Emr done")

            # after EMR run, we need to regenerate cascade so that new EMR data
            # is present in files
            self.cascade = Cascade(
                self.mvid, reimport=True, cv_iter=self.cv_iter_id, feature_flags=feature_flags
            )

            get_SL_betas_and_run_world(self.cascade, self.disable_nonstandard_locations)
        else:
            # if we shouldn't run EMR we should still run world
            # with SL betas
            get_SL_betas_and_run_world(self.cascade, self.disable_nonstandard_locations)


def parse_args(args=None) -> Namespace:
    parser = get_arg_parser()
    return parser.parse_args(args=args)


def get_arg_parser() -> ArgumentParser:
    parser = cascade_parser(description="Launch initial global DisMod model.")
    parser.add_argument(
        CLIArgs.MVID.FLAG, type=CLIArgs.MVID.TYPE, help=CLIArgs.MVID.DESCRIPTION
    )
    parser.add_argument("--cv_iter", type=int, help="cross-validation", default=0)
    return parser


def main() -> None:
    """Parses command line to launch the initial global DisMod model."""
    args = parse_args()
    dirs = prepare_directories(args.mvid, create_directories=False)
    logging_filepath = 
    setup_logger(logging_filepath, level=args.quiet - args.verbose)
    log = logging.getLogger(__name__)
    log.debug("main started")

    setup_io_patches(args.no_upload)

    try:
        release_method = get_release_method_from_mv_and_flags(args.mvid, args)
        global_model = GlobalCascade(
            args.mvid,
            args.cv_iter,
            args.no_upload,
            release_method[Methods.DISABLE_NON_STANDARD_LOCATIONS],
        )
        global_model.run_global(feature_flags=args)
    except Exception as e:
        log.exception("error in main run_global with args {}".format(str(args)))
        if args.pdb:
            # This invokes a live pdb debug session when an uncaught
            # exception makes it here.
            import pdb
            import traceback

            traceback.print_exc()
            pdb.post_mortem()
        elif check_error_msg_for_sigkill(str(e)):
            log.error(
                "Found error SIGKILL:9, assuming kernel failed from "
                "memory overages, returning code 137."
            )
            sys.exit(137)
        else:
            raise e


if __name__ == "__main__":
    main()
