from datetime import datetime

import numpy as np

from cod_prep.utils import report_if_merge_fail


def assert_cols_present_in_df(df, cols, cols_name="columns", df_name="df"):
    df_cols = list(df.columns)
    missing_cols = list(set(cols) - set(df_cols))
    if len(missing_cols) > 0:
        raise AssertionError(
            "Could not find {cn} {mc} in {dn} "
            "cols {dc}".format(cn=cols_name, mc=missing_cols, dn=df_name, dc=df_cols)
        )


def relative_rate_split(
    split_df,
    pop_df,
    dist_df,
    detail_map_dict,
    split_cols,
    split_inform_cols,
    pop_id_cols,
    value_cols,
    val_to_dist_map_dict=None,
    pop_val_name="population",
    dist_val_name="weight",
    verbose=False,
):
    """Split an aggregate column using relative rate splitting"""

    orig_val_sums = {vcol: split_df[vcol].sum() for vcol in value_cols}
    if verbose:
        print("[{}] Checking column validity in " "all inputs".format(str(datetime.now())))
    check_cols_dfs_names = [
        (split_df, split_cols, "split_cols", "split_df"),
        (split_df, split_inform_cols, "split_inform_cols", "split_df"),
        (split_df, pop_id_cols, "pop_id_cols", "split_df"),
        (split_df, value_cols, "value_cols", "split_df"),
        (pop_df, pop_id_cols, "pop_id_cols", "pop_df"),
        (dist_df, split_cols, "split_cols", "dist_df"),
        (dist_df, split_inform_cols, "split_inform_cols", "dist_df"),
    ]
    errors = []
    for check_cols_dfs_set in check_cols_dfs_names:
        try:
            assert_cols_present_in_df(*check_cols_dfs_set)
        except AssertionError as e:
            errors.append(e)
    if len(errors) > 0:
        raise AssertionError("\n".join(["{}".format(error) for error in errors]))

    meta_cols = list(
        set(split_df.columns) - set(value_cols) - set(split_cols) - set(split_inform_cols)
    )
    meta_cols.remove("orig_location_id")

    if verbose:
        print(
            "[{}] Expanding split columns to all detailed "
            "values possible".format(str(datetime.now()))
        )
    split_col_renames = {split_col: "agg_{}".format(split_col) for split_col in split_cols}
    for new_col in list(split_col_renames.values()):
        assert new_col not in split_df.columns, "{} already in split_df columns".format(new_col)
    split_df = split_df.rename(
        columns=lambda x: split_col_renames[x] if x in list(split_col_renames.keys()) else x
    )
    for split_col in split_cols:
        detail_map = detail_map_dict[split_col]
        split_col_rename = split_col_renames[split_col]
        split_df = split_df.merge(detail_map, on=split_col_rename, how="left")
        report_if_merge_fail(split_df, split_col, [split_col_rename])

    if verbose:
        print("[{}] Merging population".format(str(datetime.now())))
    split_df = split_df.merge(pop_df, on=pop_id_cols, how="left")
    split_df.drop("location_id", axis=1, inplace=True)
    split_df["location_id"] = split_df["orig_location_id"]
    split_df.drop("orig_location_id", axis=1, inplace=True)
    report_if_merge_fail(split_df, pop_val_name, pop_id_cols)


    dist_cols = split_cols + split_inform_cols
    dist_names = {dist_col: "dist_{}".format(dist_col) for dist_col in dist_cols}
    if val_to_dist_map_dict is not None:
        for dist_col in dist_cols:
            if dist_col in list(val_to_dist_map_dict.keys()):
                if verbose:
                    print(
                        "[{}] Mapping {} to values used in "
                        "distributions".format(str(datetime.now()), dist_col)
                    )
                dist_val_map = val_to_dist_map_dict[dist_col]
                dist_name = dist_names[dist_col]
                split_df = split_df.merge(dist_val_map, on=dist_col, how="left")
                report_if_merge_fail(split_df, dist_name, [dist_col])
    for dist_col in dist_cols:
        dist_name = dist_names[dist_col]
        if dist_name not in split_df.columns:
            split_df[dist_name] = split_df[dist_col]

    dist_df = dist_df.rename(columns=lambda x: dist_names[x] if x in list(dist_names.keys()) else x)
    if verbose:
        print("[{}] Merging split distributions".format(str(datetime.now())))
    merge_cols = list(dist_names.values())
    split_df = split_df.merge(dist_df, on=merge_cols, how="left")
    report_if_merge_fail(split_df, dist_val_name, merge_cols)


    agg_val_names = {vcol: "agg_{}".format(vcol) for vcol in value_cols}
    for vcol in value_cols:
        agg_val_name = agg_val_names[vcol]
        assert agg_val_name not in split_df.columns, "Unexpected: {} already in columns".format(
            agg_val_name
        )
    assert "exp_val" not in split_df.columns, "Unexpected: {} already in columns".format("exp_val")
    assert "sum_exp_val" not in split_df.columns, "Unexpected: {} already in columns".format(
        "sum_exp_val"
    )

    split_df = split_df.rename(
        columns=lambda x: agg_val_names[x] if x in list(agg_val_names.keys()) else x
    )

    if verbose:
        print("[{}] Calculating expected values".format(str(datetime.now())))
    split_df["exp_val"] = split_df[dist_val_name] * split_df[pop_val_name]

    if verbose:
        print("[{}] Calculating K denominators".format(str(datetime.now())))
    group_cols = split_inform_cols + meta_cols + list(split_col_renames.values())
    split_df["sum_exp_val"] = split_df.groupby(group_cols)["exp_val"].transform(sum)

    if verbose:
        print("[{}] Fixing 0 K-denominators".format(str(datetime.now())))
    split_df.loc[split_df["sum_exp_val"] == 0, "exp_val"] = 1
    split_df.loc[split_df["sum_exp_val"] == 0, "sum_exp_val"] = split_df.groupby(group_cols)[
        "exp_val"
    ].transform(sum)

    if verbose:
        print("[{}] Calculating split values".format(str(datetime.now())))
    for vcol in value_cols:
        agg_vcol = agg_val_names[vcol]
        split_df[vcol] = split_df["exp_val"] * (split_df[agg_vcol] / split_df["sum_exp_val"])

    if verbose:
        print("[{}] Validating result".format(str(datetime.now())))
    new_val_sums = {vcol: split_df[vcol].sum() for vcol in value_cols}
    error_list = []
    error_text = "New sum for {vcol} [{nval}] does not equal old sum [{oval}]"
    for vcol in value_cols:
        oval = orig_val_sums[vcol]
        nval = new_val_sums[vcol]
        if not np.allclose(oval, nval):
            error_list.append(error_text.format(vcol=vcol, nval=nval, oval=oval))
    if len(error_list) > 0:
        raise AssertionError("\n".join(error_list))

    return split_df
