import numpy as np
from cod_prep.utils import report_if_merge_fail
from datetime import datetime


def assert_cols_present_in_df(df, cols, cols_name="columns", df_name="df"):
    df_cols = list(df.columns)
    missing_cols = list(set(cols) - set(df_cols))
    if len(missing_cols) > 0:
        raise AssertionError(
            "Could not find {cn} {mc} in {dn} "
            "cols {dc}".format(cn=cols_name, mc=missing_cols,
                               dn=df_name, dc=df_cols)
        )


def relative_rate_split(split_df, pop_df, dist_df, detail_map_dict,
                        split_cols, split_inform_cols, pop_id_cols, value_cols,
                        val_to_dist_map_dict=None,
                        pop_val_name='population', dist_val_name='weight',
                        verbose=False):
    """Split an aggregate column using relative rate splitting"""

    # save original sums
    orig_val_sums = {vcol: split_df[vcol].sum() for vcol in value_cols}
    if verbose:
        print("[{}] Checking column validity in "
              "all inputs".format(str(datetime.now())))
    # Check that all the columns are present in all the relevant dataframes
    # develop a list of arguments to assert_cols_present_in_df
    check_cols_dfs_names = [
        (split_df, split_cols, "split_cols", "split_df"),
        (split_df, split_inform_cols, "split_inform_cols", "split_df"),
        (split_df, pop_id_cols, "pop_id_cols", "split_df"),
        (split_df, value_cols, "value_cols", "split_df"),
        (pop_df, pop_id_cols, "pop_id_cols", "pop_df"),
        (dist_df, split_cols, "split_cols", "dist_df"),
        (dist_df, split_inform_cols, "split_inform_cols", "dist_df"),
    ]
    # collect all the AssertionErrors and raise them together if multiple found
    errors = []
    for check_cols_dfs_set in check_cols_dfs_names:
        try:
            assert_cols_present_in_df(*check_cols_dfs_set)
        except AssertionError, e:
            errors.append(e)
    if len(errors) > 0:
        raise AssertionError(
            "\n".join(["{}".format(error) for error in errors])
        )

    # determine the non-value, non-split, non-split-informing columns
    # 'nid' probably in this list, for example
    meta_cols = list(set(split_df.columns) - set(value_cols) -
                     set(split_cols) - set(split_inform_cols))
    meta_cols.remove('orig_location_id')

    if verbose:
        print("[{}] Expanding split columns to all detailed "
              "values possible".format(str(datetime.now())))
    # rename split cols to aggregate names
    # e.g. [age_group_id, sex_id] to [agg_age_group_id, agg_sex_id]
    # rename
    split_col_renames = {split_col: 'agg_{}'.format(split_col) for
                         split_col in split_cols}
    # make sure new names like "agg_age_group_id" aren't going to conflict
    for new_col in split_col_renames.values():
        assert new_col not in split_df.columns, \
            "{} already in split_df columns".format(new_col)
    # now do renames
    split_df = split_df.rename(
        columns=lambda x: split_col_renames[x]
        if x in split_col_renames.keys() else x
    )
    # now, for each column with aggregates to be split, expand the split_df to
    # have one row per detail value in the aggregate (e.g. expand one row of
    # 40-49 to two rows with 40-44 and 45-49)
    for split_col in split_cols:
        # get the mapping from aggregate values to detail values
        detail_map = detail_map_dict[split_col]
        # get the aggregate name e.g. 'agg_age_group_id'
        split_col_rename = split_col_renames[split_col]
        # merge and expand
        split_df = split_df.merge(detail_map, on=split_col_rename, how='left')
        # make sure all aggregate values had a detail mapping
        report_if_merge_fail(split_df, split_col, [split_col_rename])

    if verbose:
        print("[{}] Merging population".format(str(datetime.now())))
    # now add population to the dataframe
    split_df = split_df.merge(pop_df, on=pop_id_cols, how='left')
    split_df.drop('location_id', axis=1, inplace=True)
    split_df['location_id'] = split_df['orig_location_id']
    split_df.drop('orig_location_id', axis=1, inplace=True)
    report_if_merge_fail(split_df, pop_val_name, pop_id_cols)

    # now, if different values of each of the dist cols
    # (split_cols + split_info_cols)
    # will be used than the most detailed values, map to those now

    # clarification:
    # the "dist_cols" are the ones that merge onto the distributions that
    # inform splits. In the case of CoD age/sex splitting, the distributions
    # are by cause/age/sex. The section of code below this comment enables
    # the user to have those distributions be at a more aggregate level than
    # the values themselves. In the CoD case, it means we can have pedestrian
    # road injuries split based on the distribution for all road injuries,
    # for example. Then the `cause_id` and the `dist_cause_id` will diverge,
    # and the code handles that using the `val_to_dist_map_dict`, which would
    # have a `val_to_dist_map` for `cause_id`, which itself would have an
    # entry from pedestrian road injuries to all road injuries.
    dist_cols = split_cols + split_inform_cols
    dist_names = {
        dist_col: "dist_{}".format(dist_col) for dist_col in dist_cols
    }
    if val_to_dist_map_dict is not None:
        for dist_col in dist_cols:
            if dist_col in val_to_dist_map_dict.keys():
                if verbose:
                    print("[{}] Mapping {} to values used in "
                          "distributions".format(
                              str(datetime.now()), dist_col)
                          )
                dist_val_map = val_to_dist_map_dict[dist_col]
                dist_name = dist_names[dist_col]
                split_df = split_df.merge(
                    dist_val_map, on=dist_col, how='left')
                # if a mapping exists from distribution column values to values
                # for merging on distributions, then all values of the
                # distribution columns should be there
                report_if_merge_fail(split_df, dist_name, [dist_col])
    for dist_col in dist_cols:
        dist_name = dist_names[dist_col]
        if dist_name not in split_df.columns:
            split_df[dist_name] = split_df[dist_col]

    # now merge on the distributions for splitting
    dist_df = dist_df.rename(
        columns=lambda x: dist_names[x] if x in dist_names.keys() else x
    )
    # merge on the distribution col renames
    if verbose:
        print("[{}] Merging split distributions".format(str(datetime.now())))
    merge_cols = dist_names.values()
    split_df = split_df.merge(dist_df, on=merge_cols, how='left')
    report_if_merge_fail(split_df, dist_val_name, merge_cols)

    # some sort of uniqueness assertion now?

    # now do the splitting

    # make sure created columns aren't already there
    agg_val_names = {vcol: 'agg_{}'.format(vcol) for vcol in value_cols}
    for vcol in value_cols:
        agg_val_name = agg_val_names[vcol]
        assert agg_val_name not in split_df.columns, \
            "Unexpected: {} already in columns".format(agg_val_name)
    assert 'exp_val' not in split_df.columns, \
        "Unexpected: {} already in columns".format('exp_val')
    assert 'sum_exp_val' not in split_df.columns, \
        "Unexpected: {} already in columns".format('sum_exp_val')

    # now do renames for value columns
    split_df = split_df.rename(
        columns=lambda x: agg_val_names[x]
        if x in agg_val_names.keys() else x
    )

    if verbose:
        print("[{}] Calculating expected values".format(str(datetime.now())))
    split_df['exp_val'] = split_df[dist_val_name] * split_df[pop_val_name]

    if verbose:
        print("[{}] Calculating K denominators".format(str(datetime.now())))
    # get sum of expected value across aggregates
    group_cols = split_inform_cols + meta_cols + split_col_renames.values()
    split_df['sum_exp_val'] = split_df.groupby(
        group_cols)['exp_val'].transform(sum)

    if verbose:
        print("[{}] Fixing 0 K-denominators".format(str(datetime.now())))
    # possible for expectation to sum to zero within an aggregate;
    # this might happen where age or sex restrictions make weights equal to 0,
    # maybe for male maternal deaths; then unknown-age male-maternal deaths
    # would be lost in the calculateion. So split with uninformed distribution
    split_df.loc[split_df['sum_exp_val'] == 0, 'exp_val'] = 1
    # then recalculate the sum of expected val
    split_df.loc[
        split_df['sum_exp_val'] == 0,
        'sum_exp_val'
    ] = split_df.groupby(group_cols)['exp_val'].transform(sum)

    if verbose:
        print("[{}] Calculating split values".format(str(datetime.now())))
    # now do actual splitting of each value (user is responsible for making
    # sure the value columns here are meant to be split)
    for vcol in value_cols:
        agg_vcol = agg_val_names[vcol]
        split_df[vcol] = split_df['exp_val'] * \
            (split_df[agg_vcol] / split_df['sum_exp_val'])

    if verbose:
        print("[{}] Validating result".format(str(datetime.now())))
    # make sure sum is same
    new_val_sums = {vcol: split_df[vcol].sum() for vcol in value_cols}
    error_list = []
    error_text = "New sum for {vcol} [{nval}] does not equal old sum [{oval}]"
    for vcol in value_cols:
        oval = orig_val_sums[vcol]
        nval = new_val_sums[vcol]
        if not np.allclose(oval, nval):
            error_list.append(error_text.format(
                vcol=vcol, nval=nval, oval=oval))
    if len(error_list) > 0:
        raise AssertionError(
            "\n".join(error_list)
        )

    # return uncollapsed dataframe
    return split_df
