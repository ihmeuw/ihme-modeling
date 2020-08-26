import logging
import numpy as np
import pandas as pd
import os
import sys
import warnings
from gbd.decomp_step import decomp_step_from_decomp_step_id

from cascade_ode.emr.emr import (
    dismod_emr, InsufficientInputs, NoEMRCalculated)
from cascade_ode.argument_parser import cascade_parser
from cascade_ode.patch_io import setup_io_patches
from cascade_ode.emr.data import NoNonZeroValues
from cascade_ode import standard_locations
from cascade_ode.constants import Methods
from cascade_ode.dag import check_error_msg_for_sigkill
from cascade_ode.decomp import get_decomp_method_from_mv_and_flags
from cascade_ode.drill import Cascade, Cascade_loc
from cascade_ode.importer import get_model_version
from cascade_ode.run_all import prepare_directories
from cascade_ode.settings import load as load_settings
from cascade_ode.setup_logger import setup_logger
from cascade_ode.upload import update_run_time

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
    cl = Cascade_loc(1, 0, 2000, cascade, timespan=50, reimport=reimport,
                     feature_flags=feature_flags)
    cl.initialize()
    return cl


def get_SL_betas_and_run_world(cascade, disable_nonstandard_locations=False):
    """
    Compute standard location betas and then run global dismod
    with those SL betas applied as priors.

    Arguments:
        cascade (cascade_ode.drill.Cascade)
        disable_nonstandard_locations (bool): Whether to add betas for
            standard locations.

    Returns:
        None
    """
    log = logging.getLogger(__name__)
    cascade_loc = generate_world(cascade, reimport=True)
    gbd_round_id = cascade.demographics.gbd_round_id

    if not disable_nonstandard_locations:
        log.info("Extracting standard location betas")
        betas = standard_locations.extract_betas(
            cascade_loc=cascade_loc,
            gbd_round_id=gbd_round_id)
        # lets save them for reference
        betas.to_csv(os.path.join(cascade.root_dir, 'sl_betas.csv'),
                     index=False)

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
    '''
    run dismod for a global cascade_loc object

    Since we're running dismod, that means this function reads from and writes
    to file system.

    Args:
        cascade_loc (drill.cascade.Cascade_loc): global Cascade_loc object of
            the model_version_id we're running dismod for

    Returns:
        None
    '''
    log = logging.getLogger(__name__)

    log.info("Starting global dismod")
    cascade_loc.run_dismod()
    log.info("dismod finished")
    log.info("summarizing posterior")
    cascade_loc.summarize_posterior()
    log.info("summarizing posterior finished")
    log.info("begin draw")
    cascade_loc.draw()
    log.info("draw finished")
    log.info("beginning predict")
    cascade_loc.predict()
    log.info("predict finished")


class GlobalCascade(object):
    """
    The GlobalCascade object is responsible for running the initial
    global dismod model.
    """

    def __init__(self, mvid, cv_iter_id, no_upload=False,
                 disable_nonstandard_locations=False):
        """
        Initializing the GlobalCascade involves instantiating drill.Cascade
        for the first time, which means this method writes to the file system
        and potentially promotes data t2 -> t3 .

        Args:
            mvid (int): model version id
            cv_iter_id (int): Integer in range 0-10. 0 if no cross validation.
            no_upload (bool): Turn off EMR upload because it's harder to mock.
            disable_nonstandard_locations (bool): Whether to add
                betas to locations.
        """
        self.mvid = mvid
        self.cv_iter_id = cv_iter_id
        self.no_upload = no_upload
        self.disable_nonstandard_locations = disable_nonstandard_locations

        self.logdir = '{}/{}'.format(settings['log_dir'], self.mvid)
        log = logging.getLogger(__name__)
        log.info(
            "Beginning GlobalCascade with mvid {} and cv_iter_id {}".format(
                mvid, cv_iter_id))

        log.info("Beginning cascade creation")
        self.cascade = Cascade(mvid, reimport=True, cv_iter=cv_iter_id)
        log.info("Done with cascade creation")

        self.meid = (
            self.cascade.model_version_meta.modelable_entity_id.values[0])
        if self.meid in [9422, 7695, 1175, 10352, 9309]:
            self.is_tb = True
        else:
            self.is_tb = False
        self.has_csmr = 'mtspecific' in self.cascade.data.integrand.unique()

    def run_global(self, feature_flags=None):
        """
        The initial global cascade run is actually a series of runs.

        2 runs are always going to happen -- 1 run with non-standard locations
        removed in order to get betas, and then another run involving all
        locations. The 2nd run will use betas calculated from the first as
        priors.

        Optionally, there is a 3rd run that can occur before the other 2. If
        EMR calculation is specified in settings, we'll run an initial global
        dismod model to generate adjusted inputs into the EMR calculation.

        If EMR runs, that means this method uploads EMR results to Epi db.

        Args:
            feature_flags (SimpleNamespace): Flags to send to computation.
        """

        log = logging.getLogger(__name__)
        log.info("Starting GlobalCascade.run_global")

        csmr_cause_id = (
            self.cascade.model_version_meta.add_csmr_cause.values[0])
        if csmr_cause_id is None:
            csmr_cause_id = np.nan
        ccvid = self.cascade.model_version_meta.csmr_cod_output_version_id
        ccvid = ccvid.values[0]
        remdf = self.cascade.model_params.query(
            'parameter_type_id == 1 & measure_id == 7')
        if len(remdf) > 0:
            remdf = remdf[['parameter_type_id', 'measure_id', 'age_start',
                           'age_end', 'lower', 'mean', 'upper']]
        else:
            remdf = None

        should_run_emr = (self.cv_iter_id == 0 and (
            not np.isnan(csmr_cause_id) or self.has_csmr)
            and (not self.is_tb))
        emr_disabled_setting = self.cascade.model_version_meta.get(
                'disable_emr', pd.Series())
        emr_is_disabled = emr_disabled_setting.unique().tolist() == [1]

        if should_run_emr and not emr_is_disabled:

            emr_prior = self.cascade.model_params.query(
                'parameter_type_id == 1 & measure_id == 9')
            if len(emr_prior) == 1:
                zero_EMR_prior = (emr_prior.lower.squeeze() == 0 and
                                  emr_prior.upper.squeeze() == 0 and
                                  emr_prior.age_start.squeeze() == 0 and
                                  emr_prior.age_end.squeeze() >= 100)
                if zero_EMR_prior:
                    raise InvalidSettings("Cannot set a value prior of 0 for "
                                          "EMR for ages 0-100 while also "
                                          "triggering EMR calculation via "
                                          "cause/remission settings")

            if np.isnan(csmr_cause_id):
                csmr_type = "custom"
            else:
                csmr_type = "cod"

            update_run_time(self.mvid)

            # remove EMR data from global cascade_loc before running dismod
            cascade_loc = generate_world(
                self.cascade, reimport=True,
                feature_flags=feature_flags)
            sex_id_is_both = 0
            cascade_loc.gen_data(sex_id_is_both, drop_emr=True)
            run_world(cascade_loc)

            # try to compute/upload EMR
            decomp_step = decomp_step_from_decomp_step_id(
                get_model_version(self.mvid).decomp_step_id.unique()[0])
            try:
                dismod_emr(self.mvid,
                           envr=settings['env_variables']['ENVIRONMENT_NAME'],
                           remission_df=remdf, csmr_type=csmr_type,
                           decomp_step=decomp_step,
                           no_upload=self.no_upload)
            except (NoNonZeroValues, InsufficientInputs, NoEMRCalculated) as e:
                log.info((
                    "EMR calculation started but did not complete due to "
                    "insufficient inputs. Skipping. Error was '{}'".format(e)))
            else:
                log.info("Emr done")

            # after EMR run, we need to regenerate cascade so that new EMR data
            # is present in files
            self.cascade = Cascade(self.mvid,
                                   reimport=True,
                                   cv_iter=self.cv_iter_id,
                                   feature_flags=feature_flags)

            get_SL_betas_and_run_world(self.cascade,
                                       self.disable_nonstandard_locations)
        else:
            # if we shouldn't run EMR we should still run world
            # with SL betas
            get_SL_betas_and_run_world(self.cascade,
                                       self.disable_nonstandard_locations)


def parse_args(args=None):
    parser = cascade_parser("Launch initial global dismod model")
    parser.add_argument('mvid', type=int)
    parser.add_argument('--cv_iter', type=int, default=0)
    return parser.parse_args(args)


def main():
    """
    Parses command line to launch the initial global dismod model
    """
    args = parse_args()
    dirs = prepare_directories(args.mvid, create_directories=False)
    logging_filepath = '%s/%s' % (
        dirs['model_logdir'], f'{args.mvid}_{args.cv_iter}_global.log')
    setup_logger(
        logging_filepath,
        level=args.quiet - args.verbose)
    log = logging.getLogger(__name__)
    log.debug("main started")

    setup_io_patches(args.no_upload)

    try:
        decomp_method = get_decomp_method_from_mv_and_flags(args.mvid, args)
        global_model = GlobalCascade(
            args.mvid, args.cv_iter, args.no_upload,
            decomp_method[Methods.DISABLE_NON_STANDARD_LOCATIONS])
        global_model.run_global(feature_flags=args)
    except Exception as e:
        log.exception("error in main run_global with args {}".format(
            str(args)))
        if args.pdb:
            # This invokes a live pdb debug session when an uncaught
            # exception makes it here.
            import pdb
            import traceback

            traceback.print_exc()
            pdb.post_mortem()
        elif check_error_msg_for_sigkill(str(e)):
            log.error('Found error SIGKILL:9, assuming kernel failed from '
                      'memory overages, returning code 137.')
            sys.exit(137)
        else:
            raise e


if __name__ == '__main__':
    main()
