"""
Submit the marketscan NR jobs
"""

import os
import shutil
import glob
import getpass
import datetime
import warnings
import pandas as pd
from pathlib import Path
from db_tools.ezfuncs import query
from clinical_info.ClusterHelpers.qsub import QSub
from clinical_info.Functions import hosp_prep
from clinical_info.Mapping import clinical_mapping
from clinical_info.Database.bundle_relationships import relationship_methods as brm


def archiver(files):
    """Takes a list of filepaths, creates a new directory in
    an FILEPATH folder and then moves all files into that folder"""
    parent = Path(files[0]).parent
    now = datetime.datetime.now()
    new_parent = FILEPATH
    os.mkdir(new_parent)
    for f in files:
        fname = os.path.basename(f)
        new_f = f'{new_parent}{fname}'
        shutil.move(f, new_f)


def submit_qsubs(bundles, run_id):
    """Takes a df with model groups to send out"""

    for col in bundles.columns:
        assert bundles[col].dtype.kind == 'i', (f'These columns must '
                                                f'be integers or worker '
                                                f'jobs fail {bundles.info()}')

    user = getpass.getuser()

    errors_output_base_dir = FILEPATH
    code_dir = FILEPATH

    print("Launching jobs ...")
    model_group_list = []
    for idx in bundles.index:
        model_group = "_".join(bundles.loc[idx].astype(str).tolist())
        model_group_list.append(model_group)
        job = QSub(qsub)
        job.launch(debug=False)
    hosp_prep.job_holder('msnr_', sleep_time=30, init_sleep=1)

    return model_group_list


def check_failures(model_group_list, base):
    """See if filepath outputs exist"""

    failure_groups = []
    for mg in model_group_list:
        read_path = (FILEPATH)
        if os.path.exists(read_path):
            pass
        else:
            failure_groups.append(mg)

    if not failure_groups:
        return None

    fail_perc = (len(failure_groups) / len(model_group_list)) * 100
    print(f"{len(failure_groups)} jobs failed or {fail_perc}% "
          "of the model group list you passed in")
    if len(failure_groups) <= 20:
        print(f"These specific groups are {failure_groups}")

    # make group failures into a df
    fdf = pd.DataFrame({'failure_groups': failure_groups})
    fdf = fdf.failure_groups.str.split("_", expand=True).astype(int)
    fdf.columns = ['bundle_id', 'estimate_id', 'sex_id', 'group_loc']

    return fdf


def agg_outputs(base):
    # agg all jobs back together
    failure_groups = []
    df_list = []
    read_paths = glob.glob(f'{base}/FILEPATH')
    for read_path in read_paths:
        try:
            df_list.append(pd.read_csv(read_path))
        except:
            failure_groups.append(read_path)

    # no jobs should fail to read
    if failure_groups:
        raise ValueError((f"We do not expect any jobs to fail reading. "
                          f"These jobs failed {failure_groups}"))

    return pd.concat(df_list, sort=False, ignore_index=True)


def test_active_bundles(bundles, run_id, drops=None):
    # as of GBD2020's new map the active bundles have changed
    active_bundles = clinical_mapping.get_active_bundles(cols=['bundle_id'],
                                                        estimate_id=[17, 21])

    # remove the bundles that will eventually be cloned
    bundle_clone = brm.BundleClone(run_id=run_id)
    bundle_clone.get_tables()
    drops += bundle_clone.bundle_relationship.output_bundle_id.unique().tolist()

    if drops:
        warnings.warn(f"dropping bundle(s) {drops}")
        active_bundles = active_bundles[~active_bundles.bundle_id.isin(drops)]

    missing_actives = set(active_bundles.bundle_id) - set(bundles.bundle_id)
    if missing_actives:
        raise ValueError(("Missing active bundles aren't allowed unless "
                          "purposefully dropped. Review the bundles "
                          f"{missing_actives}"))


def create_bundles(read_path):

    data_bundles = (pd.read_csv(read_path,
                                usecols=['bundle_id', 'estimate_id',
                                         'sex_id']).
                    drop_duplicates())
    data_bundles['group_loc'] = 102


    cols = ['bundle_id', 'estimate_id', 'sex_id', 'group_loc']
    data_bundles = data_bundles[cols]
    data_bundles.sort_values(cols, inplace=True)


    cm = clinical_mapping.create_bundle_restrictions()
    data_bundles.loc[data_bundles.bundle_id.isin(
        cm[cm.male == 0].bundle_id), 'sex_id'] = 2
    data_bundles.loc[data_bundles.bundle_id.isin(
        cm[cm.female == 0].bundle_id), 'sex_id'] = 1
    data_bundles.drop_duplicates(inplace=True)

    return data_bundles


def looper(retries, run_id, base, bundles):


    failed_bundles = None
    while retries > 0:
        # re-send only failed bundles
        if failed_bundles is not None:
            print("There are failed bundles, re-sending model groups ...")
            bundles = failed_bundles.copy()
        # send jobs
        model_group_list = submit_qsubs(bundles, run_id)
        # get failures
        failed_bundles = check_failures(model_group_list, base)
        if failed_bundles is None:
            print("no failed bundles observed, moving on")
            break
        # reduce retries by one
        retries = retries - 1
        print(f"There are {retries} retries remaining")

    return


def main(run_id, drops=[]):
    """Main function used to run the Marketscan noise reduction for a given run
    Params:
        run_id (int): identifies which run to use
        drops (list): manual list of bundle_ids to drop. Any cloned bundles will
                      be appended to this list in 'test_active_bundles()`
    """

    base = FILEPATH
    read_path = FILEPATH


    data_bundles = create_bundles(read_path)

    test_active_bundles(bundles=data_bundles,
                        run_id=run_id,
                        drops=drops)

    files = glob.glob(f'{base}/FILEPATH')
    if files:
        print(f"Archiving {len(files)} files")
        archiver(files)
        agg_file = (f"{base}/FILEPATH")
        if os.path.exists(agg_file):
            archiver([agg_file])
    else:
        pass

    looper(retries=3, run_id=run_id, base=base, bundles=data_bundles)

    # re-aggregate and ensure that there aren't missing active bundles
    df = agg_outputs(base)

    test_active_bundles(bundles=df, run_id=run_id, drops=drops)

    # write the file to get passed onto our final formatter
    df.to_csv(
        f'{base}FILEPATH',
        index=False)
