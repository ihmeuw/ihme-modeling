
import os
import shutil
import warnings
from pathlib import Path
from getpass import getuser

from clinical_info.Clinical_Runs import inpatient, claims, version_updater


if __name__ == '__main__':
    run_id = 25
    clinical_age_group_set_id = 2
    # GBD2020 env
    env_stgpr_id = '151676'

    new_run = False

    if new_run:
        make_dir(run_id)
        copy_files(run_id, env_stgpr_id)

    # # for now we run linearly
    inp = inpatient.Inpatient(
        run_id=run_id, clinical_age_group_set_id=clinical_age_group_set_id,
        remove_live_births=True)

       ###################################################################################
    ### PLEASE REVIEW ALL OF THE ATTRIBUTES OF THE INPATIENT CLASS BEFORE BEGINNING A RUN ###
       ###################################################################################

    inp.que = 'long.q'  # check qfree then assign accordingly. Pipeline needs memory, not threads
    inp.cf_agg_stat = 'median'

    inp.env_stgpr_id = env_stgpr_id  # breaks if the correct envelope version isn't copied into the run
    inp.env_path = FILEPATH.\
                format(r=inp.run_id, env=inp.env_stgpr_id)
    inp.no_draws_env = FILEPATH.\
                    format(r=inp.run_id, env=inp.env_stgpr_id)

    inp.gbd_round_id = 7
    inp.decomp_step = 'iterative'

    inp.central_lookup = {'haqi':
                            {'covariate_id': 1099,
                             'model_version_id': 36996,
                             'decomp_step': 'iterative'},
                          'live_births_by_sex':
                            {'covariate_id': 1106,
                             'model_version_id': 36527,
                             'decomp_step': 'iterative'},
                          'ASFR':
                            {'covariate_id': 13,
                             'model_version_id': 35669,
                             'decomp_step': 'iterative'},
                          'IFD_coverage_prop':
                            {'covariate_id': 51,
                             'model_version_id': 36928,
                             'decomp_step': 'iterative'},
                          'population':
                            {'run_id': 212,
                             'decomp_step': 'iterative'}
                         }

    for key in inp.central_lookup.keys():
        inp.central_lookup[key]['gbd_round_id'] = inp.gbd_round_id

    inp.clinical_decomp_step = 'iterative'


inp.automater()
print(inp)


inp.main(control_begin_with=False)


out = outpatient.Outpatient(run_id=run_id,
                            gbd_round_id=6,
                            decomp_step='step1')

claim = claims.Claims(run_id=run_id)
claim.main()
