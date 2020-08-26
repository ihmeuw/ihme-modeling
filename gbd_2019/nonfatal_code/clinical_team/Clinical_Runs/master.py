"""
This is the clinical pipeline's master script which runs our
three main processes (inpatient, outpatient and claims)


Providing some example code which shows how to manipulate the inp class
Let's say you want to re-run the pipeline starting with the apply_envelope step
and use the existing files of the CFs * env * cause fractions which _must_ be
have been written within the run_id for this to work

inp.begin_with = 'apply_envelope'
inp.run_tmp_unc = False
inp.main(control_begin_with=True)
"""
import shutil
import warnings
import getpass
import sys

user = getpass.getuser()

repo = "FILEPATH".format(user)
for p in ["FILENAME"]:
    sys.path.append(repo + p)

import inpatient
import claims

import version_updater

def make_dir(run):
    """
    creates the new run_id folder tree using the structure from the last run
    excludes data files stored as csv, hdf and dta
    """
    base = "FILEPATH"
    src = "FILEPATH".format(base, run - 1)
    dst = "FILEPATH".format(base, run)
    ign = shutil.ignore_patterns("FILEPATH", '*.pkl', '*.p', '*.log')
    shutil.copytree(src, dst, ignore=ign)

def copy_files(run_id, env_stgpr_id):
    """
    There are some files that will often be unchanged between runs
    copy these over manually
    """
    base = "FILEPATH"
    src = "FILEPATH".format(base, run_id - 1)
    dst = "FILEPATH".format(base, run_id)


    warnings.warn("Ensure that this is updated for decomp 4")

    split_table = "FILEPATH"


    warnings.warn("""\n\n
                     Update the envelope for GBD 2020!! The envelope from
                     April 8th 2019 is being copied over FROM RUN 3. This
                     is meant to break when a new envelope is created""")

    full_env = f"FILEPATH"
    no_draw_env = f"FILEPATH"
    new_full = "FILENAME"
    new_no_draw = "FILENAME"

    copy_dict = {src + split_table: dst + split_table,
                 full_env: dst + new_full,
                 no_draw_env: dst + new_no_draw}

    for key in list(copy_dict.keys()):
        shutil.copy(key, copy_dict[key])
    return



run_id = 7
env_stgpr_id = 53771

new_run = False

if new_run:
    make_dir(run_id)
    copy_files(run_id, env_stgpr_id)


inp = inpatient.Inpatient(run_id=run_id)

inp.env_stgpr_id = env_stgpr_id
inp.env_path = "FILEPATH"\
               "FILEPATH".\
               format(r=inp.run_id, env=inp.env_stgpr_id)
inp.no_draws_env = "FILEPATH"\
                   "FILEPATH".\
                   format(r=inp.run_id, env=inp.env_stgpr_id)
inp.decomp_step = 'step2'

version_updater.update_tables(user=user, run_id=run_id,
                              env_stgpr_id=inp.env_stgpr_id,
                              env_me_id=-1,
                              description='decomp4',
                              cf_version_id=5)

print("switching to the broad squaring method")
inp.weight_squaring_method = 'broad'
print(inp)

inp.cf_model_type = 'mr-brt'


inp.main(control_begin_with=False)


out = outpatient.Outpatient(run_id=run_id,
                            gbd_round_id=6,
                            decomp_step='step1')

claim = claims.Claims(run_id=run_id)
claim.main()



print("pop the champagne")
