import pandas as pd
from db_tools.ezfuncs import query
from clinical_info.Mapping import clinical_mapping


def make_cc_to_bundle_map(map_version, icd_only):
    """Create a mapping going from cause code to bundle, retain ICG for review"""

    cc = clinical_mapping.get_clinical_process_data(
        "cause_code_icg", map_version=map_version
    )
    if icd_only:
        cc = cc[cc.code_system_id.isin([1, 2])]
    bun = clinical_mapping.get_clinical_process_data(
        "icg_bundle", map_version=map_version
    )

    m = bun.merge(
        cc, how="left", on=["icg_id", "icg_name", "map_version"], validate="m:m"
    )
    assert m.isnull().sum().sum() == 0, "why are there nulls"
    return m


def compare_bundles(left, right):
    """loop over all the bundle_ids on the right side
    identify any mapping differences at the ICD level and save them
    identify any bundles that don't exist on the left side
    """

    matching_bundles = []
    new_bundles = []
    for b in right.bundle_id.unique():
        right_b = right.query(f"bundle_id == {b}")
        left_b = left.query(f"bundle_id == {b}")
        diff = set(right_b.cause_code.unique()).symmetric_difference(
            left_b.cause_code.unique()
        )
        if not diff:
            matching_bundles.append(b)
        if len(left_b) == 0:
            new_bundles.append(b)
    changed_bundles = list(
        set(right.bundle_id.unique()) - set(matching_bundles + new_bundles)
    )
    assert not set(changed_bundles).intersection(set(matching_bundles))
    assert not set(new_bundles).intersection(set(matching_bundles))
    assert not set(new_bundles).intersection(set(changed_bundles))
    return matching_bundles, new_bundles, changed_bundles


def get_swaps(left, right, new_bundles):
    """Loop over the list of new bundles and subset that bundle from right
    Loop over all bundles in left, check if the ICD set has no diffs between
    left and right.

    If there are none then identify the bundle swap and break loop. This may be
    an issue if multiple bundles share identical ICD code mapping
    """

    swaps = {}  # keys are old bundles, values are matching new bundles
    for nb in new_bundles:
        right_b = right.query(f"bundle_id == {nb}")
        for ob in left.bundle_id.unique():
            if not set(right_b.cause_code).symmetric_difference(
                left.loc[left.bundle_id == ob, "cause_code"]
            ):
                swaps[ob] = nb
                break
    return swaps


def replace_swaps(left, swaps):
    """Align left by replace old bundle_ids from left with new bundles from right"""

    for old, new in swaps.items():
        left.loc[left.bundle_id == old, "bundle_id"] = new
    return left


def apply_bundle_swapping(df, map_version_older, map_version_newer, drop_data=False):
    """
    Give this function will swap out old bundle ids for new bundles ids, if
    the mapping between has not changed between the two mapping versions
    provided. This will can drop data. if drop_data is true, it will KEEP

    1) bundles where the mapping remained consisten between the map_version,
        but whose bundle_id did change
    2) bundles where there was no change in the mapping, and the bundle_id
        stayed the same

    That is, bundle_ids whose ICD code mapping changed in some way will be
    dropped

    df: Pandas dataframe with bundle_ids
    map_version_older: (int) older map version id
    map_version_newer: (int) newer map version id
    """

    new = make_cc_to_bundle_map(map_version=map_version_newer, icd_only=True)
    old = make_cc_to_bundle_map(map_version=map_version_older, icd_only=True)

    matching_bundles, new_bundles, changed_bundles = compare_bundles(old, new)

    swaps = get_swaps(old, new, new_bundles)

    df = replace_swaps(df, swaps)

    if drop_data:
        keep_bundles = list(swaps.values()) + matching_bundles
        df = df.loc[df.bundle_id.isin(keep_bundles), :]

    return df
