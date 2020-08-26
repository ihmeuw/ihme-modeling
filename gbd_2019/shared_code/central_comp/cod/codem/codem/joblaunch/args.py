
"""Functions to get arguments for a CODEm model from the command line or from kickoff.py.
"""
import argparse


def get_args():
    """
    Get arguments from CODEm model.
    """
    parser = argparse.ArgumentParser(description='Run CODEm via codeV2')
    parser.add_argument('model_version_id', help='the model version id to use', type=int)
    parser.add_argument('db_connection', help='db server to connect to', type=str)
    parser.add_argument('old_covariates_mvid', help='a model version id from which to use the selected covariates',
                        type=int)
    parser.add_argument('-dbg', '--debug_mode', help='whether or not to save preds', type=bool, required=False)
    parser.add_argument('-cores', '--num_cores', help='number of cores', type=int, required=False, default=20)
    parser.add_argument('-gb', '--gigabytes', help='gigabytes of memory', type=int, required=False, default=20)
    parser.add_argument('-min', '--minutes', help='minutes of runtime', type=int, required=False, default=60)
    parser.add_argument('-sr', '--step-resources', help='json file with additional resource parameters',
                        type=str, required=False, default='{}')
    args = parser.parse_args()
    return args


def get_step_args():

    parser = argparse.ArgumentParser(description='Run step tasks')
    parser.add_argument('-model-version-id', help='the model version id to use', type=int, required=True)
    parser.add_argument('-db-connection', help='db server to connect to', type=str, required=True)
    parser.add_argument('-debug-mode', help='in debug mode or not', type=bool, required=True)
    parser.add_argument('-old-covariates-mvid', help='a model version id from which to use the selected covariates',
                        type=int, required=True)
    parser.add_argument('-cores', help='number of threads from h_rt that is being passed to the child job',
                        type=int, required=True)
    parser.add_argument('--outcome', help='the outcome to use for covariate selection', type=str, required=False)
    parser.add_argument('--ko', help='the knockout number for this process', type=int, required=False)

    args = parser.parse_args()
    if args.old_covariates_mvid == 0:
        args.old_covariates_mvid = None

    return args

