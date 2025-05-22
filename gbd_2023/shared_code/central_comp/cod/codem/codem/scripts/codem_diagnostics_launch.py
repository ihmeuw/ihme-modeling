import os

from codem_diagnostics import run_codem_diagnostics

from codem.joblaunch.args import get_diagnostics_args


def main():
    """
    Wrapper to call CODEmDiagnostics package codem_diagnostics.run_codem_diagnostics for
    two model versions.
    """
    args = get_diagnostics_args()

    model_version_id_1 = args.model_version_id_1
    model_version_id_2 = args.model_version_id_2
    cluster_account = args.cluster_account
    overwrite = args.overwrite

    os.environ["CODEM_DIAGNOSTICS_USE_CURRENT_ENV"] = "1"
    run_codem_diagnostics(
        model_version_id_1,
        model_version_id_2,
        cluster_account=cluster_account,
        overwrite=overwrite,
    )
