"""Noise reduction phase."""
import sys

import pandas as pd

from cod_prep.claude.noise_reduction import NoiseReducer
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import (
    get_value_from_nid,
    get_malaria_model_group_from_nid
)

from cod_prep.utils import print_log_message, just_keep_trying, report_duplicates, report_if_merge_fail
from cod_prep.claude.claude_io import write_phase_output, get_claude_data
from cod_prep.claude.run_phase_nrmodel import model_group_is_run_by_cause
from cod_prep.utils.nr_helpers import get_fallback_model_group
pd.options.mode.chained_assignment = None

CONF = Configurator('standard')
# sources containing maternal deaths that are noise reduced
MATERNAL_NR_SOURCES = [
    "Mexico_BIRMM", "Maternal_report", "SUSENAS",
    "China_MMS", "China_Child",
]

NR_DIR = CONF.get_directory('nr_process_data')


def get_model_result_path(nr_dir, model_group, launch_set_id, cause_id=None):
    if cause_id is None:
        filepath = "FILEPATH"
    else:
        filepath = "FILEPATH"
    return filepath


def get_malaria_noise_reduction_model_result(malaria_model_group, launch_set_id):
    """Read in the csv with saved malaria model result."""
    malaria_dfs = []
    for model_group in malaria_model_group:
        if model_group != "NO_NR":
            malaria_filepath = get_model_result_path(
                NR_DIR, model_group, launch_set_id
            )
            df = just_keep_trying(
                pd.read_csv,
                args=[malaria_filepath],
                max_tries=100,
                seconds_between_tries=6,
                verbose=True
            )
            malaria_dfs.append(df)
    malaria_results = pd.concat(malaria_dfs)
    return malaria_results


def get_model_cols(df, model_group):
    if model_group.startswith("VA-") or (model_group == "CHAMPS"):
        return ['cf', 'cf_pred', 'se_pred', 'cf_post', 'var_post']
    elif model_group.startswith("VR-") or model_group.startswith("MATERNAL") or \
        model_group.startswith('POLICE_VIOLENCE'):
        return [
            col for col in df.columns if col.startswith('pred')
            or col.startswith('std_err') or (col == 'average_prior_sample_size')
        ]
    else:
        raise AssertionError("Don't understand this model group")


def stitch_fallback_model_results(df, fallback_model_group,
                                  launch_set_id):
    """
    For any row of data marked as sparse, replace with the result
    from the fallback model (higher location level)
    """
    # if nothing is sparse, no need to bring in the fallback results
    if not (len(df.loc[df.sparsity_flag == 1]) > 0):
        return df

    merge_cols = [
        'nid', 'extract_type_id', 'location_id', 'year_id',
        'age_group_id', 'sex_id', 'cause_id', 'site_id'
    ]
    model_cols = get_model_cols(df, fallback_model_group)
    # Get all of the fallback model results
    if model_group_is_run_by_cause(fallback_model_group):
        fallback_model = pd.concat(
            [
                pd.read_csv(
                    get_model_result_path(
                        NR_DIR, fallback_model_group, launch_set_id, cause_id=cause_id
                    )
                )
                for cause_id in df.query("sparsity_flag == 1").cause_id.unique().tolist()
            ]
        )
    else:
        fallback_model = pd.read_csv(get_model_result_path(
            NR_DIR, fallback_model_group, launch_set_id
        ))
    fallback_model = fallback_model[merge_cols + model_cols]
    df = df.merge(fallback_model, how='left', on=merge_cols, suffixes=('', '_fallback'))
    report_if_merge_fail(df.query("sparsity_flag == 1"), model_cols[0] + '_fallback', merge_cols)

    # where the current model was determined to be sparse, replace with fallback model results
    for col in model_cols:
        df.loc[df.sparsity_flag == 1, col] = df[col + '_fallback']
        df = df.drop(col + '_fallback', axis=1)
    df = df.drop('sparsity_flag', axis=1)
    return df


def get_noise_reduction_model_result(nid, extract_type_id, launch_set_id,
                                     model_group, cause_id=None):
    """Read in the csv with saved model results."""
    filepath = get_model_result_path(
        NR_DIR, model_group, launch_set_id, cause_id=cause_id
    )

    results = just_keep_trying(
        pd.read_csv,
        args=[filepath],
        max_tries=100,
        seconds_between_tries=6,
        verbose=True
    )

    if len(results) == 0 and cause_id is not None:
        return results

    results = results[
        (results['nid'] == nid) &
        (results['extract_type_id'] == extract_type_id)
    ]

    fallback_model_group = get_fallback_model_group(
        model_group, location_set_version_id=CONF.get_id("location_set_version"),
        force_rerun=False, block_rerun=True, cache_results=False)
    if fallback_model_group != 'NO_NR':
        results = stitch_fallback_model_results(
            results, fallback_model_group=fallback_model_group,
            launch_set_id=launch_set_id)

    drop_columns = [
        col for col in [
            'country_id', 'subnat_id', 'year_bin', 'is_loc_agg', 'sparsity_flag',
            'iso3', 'code_system_id'
        ] if col in results.columns
    ]
    results = results.drop(drop_columns, axis=1)

    # make sure there aren't any null values
    if 'VA-' not in model_group and "CHAMPS" not in model_group:
        assert not results.isnull().values.any(), \
            "There are missing values in the {}".format(filepath)

    return results


def add_malaria_model_group_result(df, nid, extract_type_id,
                                   launch_set_id, malaria_model_group):
    keep_cols = df.columns.tolist()
    malaria_results = get_malaria_noise_reduction_model_result(
        malaria_model_group, launch_set_id)
    malaria_results = malaria_results.query(
        f"nid == {nid} & extract_type_id == {extract_type_id}"
    )
    if len(malaria_results) > 0:
        malaria = df['cause_id'] == 345
        df = df.loc[~malaria]
    df = pd.concat([df, malaria_results], ignore_index=True)
    report_duplicates(df, ['nid', 'extract_type_id', 'site_id', 'cause_id',
                           'sex_id', 'age_group_id', 'location_id', 'year_id'])
    df = df[keep_cols]
    return df


def run_phase(nid, extract_type_id, project_id, launch_set_id, data_type_id, source,
              model_group, malaria_model_group):
    run_noise_reduction = True
    run_by_cause = False

    # determine above values using the source and model group
    if model_group == "NO_NR":
        run_noise_reduction = False

    if model_group_is_run_by_cause(model_group):
        run_by_cause = True

    if run_noise_reduction:
        if run_by_cause:

            filepath = "FILEPATH"
            causes = sorted(list(pd.read_csv(filepath, dtype=int)['cause_id'].unique()))
            print_log_message("Reading cause-specific files".format(len(causes)))
        else:
            causes = [None]
            print_log_message("Reducing cause-appended file")

        dfs = []
        for cause_id in causes:
            df = get_noise_reduction_model_result(
                nid, extract_type_id, launch_set_id,
                model_group, cause_id=cause_id
            )
            if 'Unnamed: 0' in df.columns:
                df = df.drop('Unnamed: 0', axis=1)
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)

        if "NO_NR" not in malaria_model_group:
            df = add_malaria_model_group_result(
                df, nid, extract_type_id, launch_set_id,
                malaria_model_group
            )

        print_log_message("Running bayesian noise reduction algorithm using fitted priors")
        noise_reducer = NoiseReducer(data_type_id, source)
        df = noise_reducer.get_computed_dataframe(df)

    else:
        # simply get the aggregated result
        print_log_message(
            "Skipping noise reduction for source {} and model "
            "group {}".format(source, model_group)
        )
        df = get_claude_data(
            "aggregation", nid=nid, extract_type_id=extract_type_id, project_id=project_id
        )

    return df


def main(nid, extract_type_id, project_id, launch_set_id):
    """Run the noise reduction phase."""
    print_log_message("Beginning noise reduction phase")

    data_type_id = get_value_from_nid(
        nid, 'data_type_id', project_id=project_id, extract_type_id=extract_type_id
    )
    source = get_value_from_nid(
        nid, 'source', project_id=project_id, extract_type_id=extract_type_id
    )
    model_group = get_value_from_nid(
        nid, 'model_group', project_id=project_id, extract_type_id=extract_type_id
    )
    malaria_model_group = get_malaria_model_group_from_nid(
        nid, extract_type_id, project_id
    )

    df = run_phase(nid, extract_type_id, project_id, launch_set_id, data_type_id,
                   source, model_group, malaria_model_group)

    print_log_message(
        "Writing {n} rows of output for launch set {ls}, nid {nid}, extract "
        "{e}".format(n=len(df), ls=launch_set_id, nid=nid, e=extract_type_id)
    )
    ids = ['age_group_id', 'cause_id', 'extract_type_id',
           'location_id', 'year_id', 'site_id', 'sex_id', 'nid']
    df[ids] = df[ids].astype(int)
    write_phase_output(df, 'noisereduction', nid,
                       extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    project_id = int(sys.argv[3])
    launch_set_id = int(sys.argv[4])
    main(nid, extract_type_id, project_id, launch_set_id)
