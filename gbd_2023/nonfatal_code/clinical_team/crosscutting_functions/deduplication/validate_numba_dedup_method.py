import pandas as pd
from crosscutting_functions.mapping import clinical_mapping_db


def compare_gbd_results(bundle_id, estimate_id):
    """Read and compare final files for run 35 against a set of results
    produced using the recursive duration method that were manually archived."""
    base = "FILEPATH"

    numba_df = pd.read_parquet(
        "FILEPATH"
    )
    recursive_df = pd.read_parquet(
        "FILEPATH"
    )

    pd.testing.assert_frame_equal(numba_df, recursive_df)


def get_in_map_active_bundles(map_version):
    """The workflow was run with only active bundles that are present in
    the map (eg not maternal ratios or parent injuries)."""
    bundles = clinical_mapping_db.get_active_bundles(map_version)
    map_bundles = clinical_mapping_db.get_clinical_process_data("icg_bundle", map_version)
    return bundles[bundles.bundle_id.isin(map_bundles.bundle_id)]


def compare_dedup_methods(bundles):
    """Loop over all bundles and estimates used in GBD and compare numba to
    recursive duration final results."""
    failures = []
    successes = []
    for bundle_id in bundles.bundle_id.unique():
        for estimate_id in [17, 21]:
            try:
                compare_gbd_results(bundle_id, estimate_id)
                successes.append((bundle_id, estimate_id))
            except Exception as e:
                failures.append((bundle_id, estimate_id, e))

    return failures, successes


failures, successes = compare_dedup_methods(get_in_map_active_bundles(map_version=30))
