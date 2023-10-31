from db_tools.ezfuncs import query
import sys
import pandas as pd

from clinical_info.Mapping import clinical_mapping


def write_acause_rei_to_bundle_map(run_id):

    old_file = pd.read_csv(
       FILEPATH

    bun = clinical_mapping.get_clinical_process_data(
        'icg_bundle', prod=True, map_version='current')
    bun = bun[['bundle_id', 'map_version']].drop_duplicates()

    bun_map = query(QUERY.format(
        tuple(bun.bundle_id.unique())),)

    acause_df = query(QUERY.format(tuple(
        bun_map[bun_map.cause_id.notnull()].cause_id.unique())),)[['cause_id', 'acause']]
    rei_df = query(QUERY.format(tuple(
        bun_map[bun_map.rei_id.notnull()].rei_id.unique())), )[['rei_id', 'rei']]

    new_map = bun_map[['bundle_id', 'cause_id', 'rei_id']].copy()
    new_map = new_map.merge(acause_df, how='outer', on='cause_id')
    new_map = new_map.merge(rei_df, how='outer', on='rei_id')

    new_map['bundle_acause_rei'] = "Missing"

    mask = "new_map['cause_id'].notnull()"
    new_map.loc[eval(mask), 'bundle_acause_rei'] = new_map.loc[eval(
        mask), 'acause']
    mask = "new_map['rei_id'].notnull()"
    new_map.loc[eval(
        mask), 'bundle_acause_rei'] = new_map.loc[eval(mask), 'rei']

    print("beginning a few tests")
    for acause in acause_df.acause.unique():
        cid = new_map.query("acause == @acause").cause_id.drop_duplicates()
        assert cid.size == 1
        cid = cid.iloc[0]
        assert acause == new_map.query("cause_id == @cid").acause.iloc[0]
    for rei in rei_df.rei.unique():
        rid = new_map.query("rei == @rei").rei_id.drop_duplicates()
        assert rid.size == 1
        rid = rid.iloc[0]
        assert rei == new_map.query("rei_id == @rid").rei.iloc[0]

    new_map.drop(['cause_id', 'rei_id', 'acause', 'rei'], axis=1, inplace=True)

    assert new_map.shape[0] == new_map.bundle_id.unique(
    ).size, "duplicated bundles"
    assert (new_map['bundle_acause_rei'] == "Missing").sum() == 0

    print("tests passed, writing to run {}".format(run_id))
    write_path = QUERY.format(
        run_id)
    new_map.to_csv(write_path, index=False)
    return
