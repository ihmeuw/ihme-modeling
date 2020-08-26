"""Redistribute garbage codes by split group."""

import pandas as pd
import numpy as np
import json
import os
from cod_process import CodProcess
from configurator import Configurator
from cod_prep.utils import print_log_message, report_if_merge_fail


def get_cause_restrictions(cause_map):
    """Prepare cause map."""
    restrictions = cause_map.copy(deep=True)
    return restrictions.set_index('cause').to_dict()


def eval_condition(x):
    """Evaluate whether a condition is true for a row.a

    Many assumptions are made about variable-naming and which variables are
    referenced in the condition. So that's not ideal. BUT: it will fail
    if it doesn't find the right variables set based on the condition, so it
    is still safe.
    """

    # your linter will think these variables are not used, but they are
    age = x['age']
    sex = x['sex']
    super_region = x['super_region']
    year_id = x['year_id']

    # believe these are unused but set anyway
    country = x['country']
    region = x['region']
    nid = x['nid']

    condition = x['restrictions']
    # in case it isn't clear, this is taking the variables set in this method,
    # and comparing them to the passed condition. So condition might contain
    # '(age >= 0.0)' and if age is set to 1.0 above, then this will return
    # True
    return eval(condition)


def evaluate_cause_restrictions(cause_map, proportion_metadata):
    """Determine proportion_ids within which each cause can be targeted

    Returns:
        cm_eval, pandas.DataFrame: cause, proportion_id, eval
            'eval' is True or False; if False, nothing will be redistributed
            to this cause in this proportion_id
    """

    # make copies to avoid unexpected memory behavior
    prop_metadata = proportion_metadata.copy()
    cmap = cause_map.copy()

    # get all unique restrictions mapped to a restriction_id
    restrictions = cause_map[['restrictions']].drop_duplicates()
    restrictions = restrictions.reset_index(drop=True)
    restrictions['restriction_id'] = restrictions.index

    # get cartesian product of proportion_ids and restrictions
    restrictions['join_key'] = 1
    prop_metadata['join_key'] = 1
    prop_restrictions = prop_metadata.merge(
        restrictions,
        on='join_key'
    ).drop('join_key', axis=1)

    # check if each proportion_id passes each restriction
    prop_restrictions['eval'] = prop_restrictions.apply(
        lambda x: eval_condition(x), axis=1
    )

    # make mapping from restriction to restriction_id
    restrict_dict = restrictions.set_index(
        'restrictions', verify_integrity=True
    ).to_dict()['restriction_id']

    # get cartesian product of all causes and all proportion ids
    cmap['restriction_id'] = cmap['restrictions'].map(restrict_dict)
    cmap = cmap[['cause', 'restriction_id']]
    prop_ids = prop_metadata[['proportion_id']]
    cmap['join_key'] = 1
    prop_ids['join_key'] = 1
    cm_eval = cmap.merge(prop_ids, on='join_key').drop('join_key', axis=1)

    # now find out if each proportion_id passes the cause's restriction
    cm_eval = cm_eval.merge(
        prop_restrictions, on=['proportion_id', 'restriction_id']
    )
    assert cm_eval['eval'].notnull().all()
    cm_eval = cm_eval[['cause', 'proportion_id', 'eval']]

    return cm_eval


def evaluate_cause_restrictions_old(nid, extract_type_id, cause_map,
                                    proportion_metadata, proportion_ids):
    """Merge cause restrictions to the cause map."""
    for t in proportion_ids:
        if t != 'region':
            cause_map['restrictions'] = cause_map['restrictions'].map(
                lambda x: x.replace(t, 'full_cause_map.loc[i,"' + t + '"]')
            )
    dict_cause_map = get_cause_restrictions(cause_map)
    full_cause_map = []
    for pid in proportion_metadata['proportion_id']:
        temp = cause_map.ix[:, ['cause', 'restrictions']].copy(deep=True)
        temp['proportion_id'] = pid
        full_cause_map.append(temp)
    full_cause_map = pd.concat(full_cause_map).reset_index(drop=True)
    full_cause_map = pd.merge(
        full_cause_map,
        proportion_metadata,
        on='proportion_id'
    ).reset_index(drop=True)
    full_cause_map['id'] = full_cause_map.index
    maps = {'id': [], 'eval': []}
    # this is what takes so long
    for i in full_cause_map.index:
        maps['id'].append(i)
        maps['eval'].append(
            eval(dict_cause_map['restrictions'][full_cause_map.ix[i, 'cause']])
        )
    cm_eval = pd.merge(
        full_cause_map,
        pd.DataFrame(maps),
        on='id'
    ).loc[:, ['proportion_id', 'cause', 'eval']].reset_index(drop=True)

    return cm_eval


def read_json(file_path):
    """Prepare packages."""
    # Read in JSON
    with open(file_path) as json_data:
        data = json.load(json_data)
    return data


def extract_package(package, cause_map, package_id):
    """Extract useful package data."""
    fmt_package = {}
    fmt_package['package_id'] = package_id
    add_cols = [
        'package_name', 'package_version_id', 'shared_package_version_id',
        'package_description'
    ]
    for add_col in add_cols:
        fmt_package[add_col] = package[add_col]
    if 'create_targets' in package:
        fmt_package['create_targets'] = package['create_targets']
    else:
        package['create_targets'] = 0
    # do something
    fmt_package['garbage_codes'] = package['garbage_codes']
    fmt_package['target_groups'] = {}
    c = 0
    for tg in package['target_groups']:
        target_codes = []
        except_codes = []
        for tc in tg['target_codes']:
            target_codes.append(tc)
        if 'except_codes' in tg:
            for ec in tg['except_codes']:
                except_codes.append(ec)
        fmt_package['target_groups'][str(c)] = {
            'target_codes': list(
                (set(target_codes) - set(except_codes)) -
                set(fmt_package['garbage_codes'])
            ),
            'weights': tg['weights']
        }
        c += 1
    fmt_package['weight_groups'] = {}
    c = 0
    for wg in package['weight_groups']:
        param = wg['weight_group_parameter']
        param = param.replace(
            "country == 'Russian Federation'", "country == 'Russia'"
        )
        fmt_package['weight_groups'][str(c)] = param
        c += 1
    return fmt_package


def get_packages(package_folder, cause_map):
    """Import packages."""
    package_list = read_json(package_folder + '/_package_list.json')
    packages = []
    for rdp in package_list:
        print("Importing package: " + rdp)
        package_id = rdp.replace("package_", "")
        packages.append(
            extract_package(
                read_json(package_folder + '/' + rdp + '.json'),
                cause_map,
                package_id
            )
        )
    return packages


def pull_metadata(data, unique_ids):
    """Create group identifiers.

    Make a 'unique_group_id' column based on
    unique combinations of a given set of variables.
    """
    data = data[unique_ids].drop_duplicates().reset_index(drop=True)
    data['unique_group_id'] = data.index
    return data


def prep_data(data, signature_ids, proportion_ids, residual_cause='cc_code'):
    """Prepare data.

    Input:
    pandas dataframe (df)
    variable lists
    residual cause name
    Output:
    df with cause, frequencies (key columns = proportion_id, signature_id)
    df with deaths metadata (key column = signature_id)
    df with proportion metadata (key column = proportion_id)
    """
    signature_metadata = pull_metadata(data, signature_ids)
    signature_metadata = signature_metadata.rename(
        columns={'unique_group_id': 'signature_id'}
    )
    proportion_metadata = pull_metadata(data, proportion_ids)
    proportion_metadata = proportion_metadata.rename(
        columns={'unique_group_id': 'proportion_id'}
    )
    data = pd.merge(data, signature_metadata, on=signature_ids)
    data = pd.merge(data, proportion_metadata, on=proportion_ids)
    data = pd.concat([
        data,
        data.loc[
            :,
            ['proportion_id', 'signature_id']
        ].drop_duplicates().set_value(
            data.ix[:,
                    ['proportion_id', 'signature_id']].drop_duplicates().index,
            'cause',
            residual_cause
        ).set_value(
            data.ix[:,
                    ['proportion_id', 'signature_id']].drop_duplicates().index,
            'freq',
            0
        )
    ])
    data = data[['proportion_id', 'signature_id', 'cause', 'freq']
                ].groupby(
        ['proportion_id', 'signature_id', 'cause']
    ).sum().reset_index()
    return data[['proportion_id', 'signature_id', 'cause', 'freq']], \
        signature_metadata, proportion_metadata


def filter_out_obviously_false_weight_groups(package, proportion_metadata):
    """Get rid of weight groups that are explicitly not for this country.

    Returns the weight groups to search for without obviously-false ones.
    """

    # it takes a super long time to find weight groups in regression
    # packages that contain every country in the world when you have to
    # evaluate the weight groups for every country. Since there is only one
    # country in a split group, then restrict the search weight groups to
    # that one to make it faster (only do this if the weight group is
    # for a specific country)
    cntries = set(proportion_metadata.country)
    assert len(cntries) == 1
    cntry = cntries.pop()
    cntry_wgt_grps = pd.Series(package['weight_groups'])
    if cntry == 'Russia':
        for i in range(0, len(cntry_wgt_grps)):
            cntry_wgt_grps[i] = cntry_wgt_grps[i].replace("Russian Federation", "Russia")
    # restrict to either weight groups that could have multiple countries
    # or weight groups that include a restriction on this country
    equals_this_cntry = "\(country == '{}'\)".format(cntry)
    # be wary of logical-nots like ~ and ! because it could be
    # "~(country == 'Armenia')"
    cntry_wgt_grps = cntry_wgt_grps.loc[
        (~cntry_wgt_grps.str.contains('country ==')) |
        ((~cntry_wgt_grps.str.contains("[~!]")) & (
            cntry_wgt_grps.str.contains(equals_this_cntry)))
    ].to_dict()

    return cntry_wgt_grps

def remap_for_decomp_step_one(meta):
    """Country specific regressions cannot run for new GBD2019 locations.
    Using weights of neighboring regional countries for the time being. Regressions
    will be updated in step two, at which point this function should be removed.
    """
    meta.loc[meta.country == 'Saint Kitts and Nevis', 'country'] = 'Dominican Republic'
    meta.loc[meta.country.isin(['San Marino', 'Monaco']), 'country'] = 'Italy'
    meta.loc[meta.country.isin(['Cook Islands', 'Palau']), 'country'] = 'Papua New Guinea'

    return meta


def find_weight_groups(package, proportion_metadata,
                       verify_integrity=False, filter_impossible=False):
    """Fetch a mapping from each unique proportion_id to weight group id

    Parameters
        verify_integrity: bool, Ensure that each proportion
            matches one and only one weight group (turn on with
            caution, when time allows finding and resolving the
            many violations of this condition)
    """
    meta = proportion_metadata.copy(deep=True)
    meta = remap_for_decomp_step_one(meta)
    # REG-28 package in ICD 10 has all weight groups limited to year_id <= 2017 - remap
    # Iceland 2018 to Iceland 2017
    if package['package_version_id'] == 3536:
        meta.loc[(meta.country == 'Iceland') & (meta.year_id == 2018), 'year_id'] = 2017
    weights = []

    # filter out obviously false ones
    if filter_impossible:
        possible_wgt_groups = filter_out_obviously_false_weight_groups(package,
                                                                       meta)
    else:
        possible_wgt_groups = package['weight_groups']

    for wg in possible_wgt_groups:
        parameters = package['weight_groups'][wg]
        if 'Russian Federation' in parameters:
            parameters = parameters.replace("Russian Federation", 'Russia')
        meta['eval'] = meta.eval(parameters)
        meta['weight_group'] = str(wg)
        weights.append(meta.ix[meta['eval'],
                               ['proportion_id', 'weight_group']])

    weight_groups = pd.concat(weights)

    if verify_integrity:
        # only one weight group per proportion
        if not len(set(weight_groups.proportion_id)) == len(weight_groups):
            dups = weight_groups[weight_groups['proportion_id'].duplicated()]
            wgs = dups['weight_group'].unique()
            err_str = "Package {} has weight groups that match " \
                      "multiple: \n".format(package['package_name'])
            for wg in wgs:
                err_str = err_str + package['weight_groups'][wg] + "\n"
            raise AssertionError(err_str)
        # every proportion id has a weight group
        missing_proportions = set(proportion_metadata.proportion_id) - \
            set(weight_groups.proportion_id)
        if not missing_proportions == set():
            missing_metadata = proportion_metadata.loc[
                proportion_metadata['proportion_id'].isin(missing_proportions)
            ]
            raise AssertionError(
                "Package {} does not have weight groups for "
                "these rows: \n{}".format(
                    package['package_name'],
                    missing_metadata)
            )
    return weight_groups


def get_proportions(data, proportion_metadata, package, cause_map_evaluated,
                    residual_cause='cc_code'):
    """
    Generate proportions based on frequencies.

    Can be broken down step by step:
        1. Get list of targets for each target group
        2. Generate base proportions (if the package is
            supposed to create causes)
        3. Pull frequencies of each cause in the data
        4. Determine and pull weight group for proportion group
        5. Pull weights from weight group for each target group
        6. Calculate fraction to each cause
    """
    # someday, turn verify_integrity on to overhaul package weight groups
    weight_groups = find_weight_groups(
        package, proportion_metadata, filter_impossible=True,
        verify_integrity=False
    )

    print_log_message("                -Identifying targets")
    targets = []
    for tg in package['target_groups']:
        temp = pd.DataFrame(
            {'cause': package['target_groups'][tg]['target_codes']}
        )
        temp['target_group'] = tg
        targets.append(temp)
    targets = pd.concat(targets).reset_index(drop=True)

    print_log_message("                -Pulling data counts - 1")
    proportions = []
    for pid in weight_groups['proportion_id']:
        temp = targets.copy(deep=True)
        if package['create_targets'] == 1:
            temp['freq'] = 0.001
        else:
            temp['freq'] = 0
        temp['proportion_id'] = pid
        proportions.append(temp)
    print_log_message("                -Pulling data counts - 2")
    tg_dict = {}
    for tg in package['target_groups']:
        tg_dict[tg] = package['target_groups'][tg]['target_codes']
    tg_df = pd.DataFrame.from_dict(tg_dict, orient='index').stack().reset_index()
    tg_df.columns = ['target_group', 'index', 'cause']
    tg_df = tg_df[['target_group', 'cause']]
    tg_df = tg_df.merge(
        data[['proportion_id', 'cause', 'freq']],
        on='cause',
        how='left'
    )
    proportions.append(tg_df)
    print_log_message("                -Pulling data counts - 3")
    proportions = pd.concat(proportions)
    print_log_message("                -Pulling data counts - 4")
    proportions = proportions.sort_values(
        ['proportion_id', 'target_group', 'cause'],
    ).reset_index(drop=True)
    print_log_message("                -Pulling data counts - 5")
    # If the proportion_id, target group has no data,
    # create targets even if create_targets==0
    proportions = proportions.set_index(
        ['proportion_id', 'target_group']
    ).join(
        proportions.groupby(
            ['proportion_id', 'target_group']
        ).sum().rename(columns={'freq': 'total'})
    )
    print_log_message("                -Pulling data counts - 6")
    proportions.ix[proportions['total'] == 0, 'freq'] = 0.001
    proportions = proportions.drop('total', axis=1).reset_index()

    print_log_message("                -Pulling data counts - 7")
    proportions = pd.merge(
        proportions,
        proportion_metadata,
        on='proportion_id'
    )
    print_log_message("                -Pulling data counts - 8")
    print_log_message("                -Merging on cause restrictions")
    # Merge on cause restrictions
    proportions = pd.merge(
        proportions,
        cause_map_evaluated,
        on=['proportion_id', 'cause'],
        how='left'
    )
    report_if_merge_fail(proportions, 'eval', ['proportion_id', 'cause'])
    # Zero out if the cause is restricted
    proportions.ix[~proportions['eval'], 'freq'] = 0
    # Calculate totals for each cause
    print_log_message("                -Calculating totals for each cause")
    proportions = proportions.ix[
        :,
        ['proportion_id', 'target_group', 'cause', 'freq']
    ].groupby(['proportion_id', 'target_group', 'cause']).sum().reset_index()
    # Calculate totals for each target group & merge back on
    print_log_message("                -Calculating totals for each target group")
    proportions = proportions.set_index(
        ['proportion_id', 'target_group']
    ).join(
        proportions.groupby(
            ['proportion_id', 'target_group']
        ).sum().rename(columns={'freq': 'total'})
    )
    proportions = pd.merge(
        proportions.reset_index(),
        weight_groups,
        on='proportion_id'
    )
    # Merge on weights
    print_log_message("                -Merging on weights")
    weights = []
    for tg in package['target_groups']:
        wg = 0
        for wgt in package['target_groups'][tg]['weights']:
            weights.append(
                {'target_group': tg, 'weight_group': str(wg), 'weight': wgt}
            )
            wg += 1
    weights = pd.DataFrame(weights)
    proportions = pd.merge(
        proportions,
        weights,
        on=['target_group', 'weight_group']
    )
    # Calculate final proportions to apply
    print_log_message("                -Reformatting data type")
    for c in ['freq', 'weight', 'total']:
        proportions[c] = proportions[c].astype('float64')
    print_log_message("                -Calculating proportions")
    proportions['proportion'] = (proportions.freq / proportions.total) * \
        proportions.weight
    # If the total proportion for a given proportion id
    #  is 0, move to the residual code
    print_log_message("                -Adding residual causes where needed")
    proportions = pd.concat([
        proportions.ix[
            proportions.total == 0,
            ['proportion_id', 'target_group', 'weight', 'total']
        ].drop_duplicates().set_index('total').set_value(
            0,
            'cause',
            residual_cause
        ).rename(
            columns={'weight': 'proportion'}
        ).reset_index().ix[
            :,
            ['proportion_id', 'proportion', 'cause']
        ].groupby(
            ['proportion_id', 'cause']
        ).sum().reset_index(),
        proportions.ix[
            proportions.total != 0,
            ['proportion_id', 'cause', 'proportion']
        ].groupby(
            ['proportion_id', 'cause']
        ).sum().reset_index()
    ]).reset_index(drop=True)
    # Again make sure everything sums to 1
    print_log_message("                -Make sure everything sums to 1")
    proportions = proportions.set_index(['proportion_id']).join(
        proportions.groupby(
            ['proportion_id']
        ).sum().rename(columns={'proportion': 'total'}))
    proportions['proportion'] = (proportions.proportion / proportions.total)
    proportions = proportions.reset_index()[
        ['proportion_id', 'cause', 'proportion']]
    return proportions


def redistribute_garbage(data, proportions, package):
    """Prepare garbage codes for redistribution."""
    diagnostics = []
    # Make sure the package contains all the codes in the proportions set
    print_log_message("                -Expanding proportions to signature id")
    temp = data[
        ['proportion_id', 'signature_id']
    ].drop_duplicates().reset_index(drop=True).copy(deep=True)
    proportions = pd.merge(
        temp,
        proportions,
        on='proportion_id'
    )
    # Tag garbage
    print_log_message("                -Tagging garbage")
    causes = data[['cause']].drop_duplicates()
    causes['garbage'] = 0
    causes.loc[causes['cause'].isin(package['garbage_codes']), 'garbage'] = 1
    cause_garbage_map = causes.set_index('cause').to_dict()['garbage']
    data['garbage'] = data['cause'].map(cause_garbage_map)
    diagnostics.append(data.loc[data['garbage'] == 1])
    # Get total number of garbage codes for each signature_id
    print_log_message("                -Summing garbage per signature id")
    temp = data.loc[
        data['garbage'] == 1,
        ['proportion_id', 'signature_id', 'freq']
    ].groupby(['proportion_id', 'signature_id']).sum().reset_index()
    temp = temp.rename(columns={'freq': 'garbage'})
    print_log_message("                -Splitting garbage onto targets: merge")
    # Redistribute garbage onto targets
    additions = pd.merge(
        proportions,
        temp,
        on=['proportion_id', 'signature_id'],
        how='outer'
    )
    print_log_message("                -Splitting garbage onto targets: multiply")
    for c in ['proportion', 'garbage']:
        additions[c] = additions[c].fillna(0)
    additions['freq'] = additions['proportion'] * additions['garbage']
    additions = additions.loc[additions['freq'] > 0,
                              ['signature_id', 'proportion_id', 'cause', 'freq']
                              ]
    diagnostics.append(additions)
    print_log_message("                -Appending split garbage onto non-garbage")
    # Zero out garbage codes
    data.loc[data['garbage'] == 1, 'freq'] = 0
    # Tack on redistributed data
    data = pd.concat([data, additions])
    data = data.loc[:, ['proportion_id', 'signature_id', 'cause', 'freq']]
    data = data.reset_index(drop=True)
    # Create diagnostics
    print_log_message("                -Making diagnostic dataframe")
    diagnostics = pd.concat(diagnostics)
    diagnostics['garbage'] = diagnostics['garbage'].fillna(0)
    # Collapse to proportion id
    diagnostics = diagnostics.groupby(
        ['proportion_id', 'garbage', 'cause']
    )['freq'].sum().reset_index()
    # Return outputs
    return data, diagnostics


def data_has_any_package_garbage(data, package):
    garbage_codes = set(package['garbage_codes'])
    causes = set(data['cause'].unique())
    return not garbage_codes.isdisjoint(causes)


def run_redistribution(input_data, signature_ids, proportion_ids, cause_map,
                       package_folder, residual_cause='cc_code',
                       diagnostic_output=False, first_and_last_only=False,
                       rerun_cause_map=True):
    """Most granular method of whole redistribution process."""
    data, signature_metadata, proportion_metadata = prep_data(
        input_data,
        signature_ids,
        proportion_ids,
        residual_cause=residual_cause
    )
    print_log_message("Importing packages")
    packages = get_packages(package_folder, cause_map)
    print_log_message("Evaluating cause map restrictions")

    cause_map_evaluated = evaluate_cause_restrictions(
        cause_map,
        proportion_metadata
    )
    print_log_message("Run redistribution!")
    diagnostics_all = []
    seq = 0
    if first_and_last_only:
        first = packages[0]
        last = packages[-1]
        packages = [first, last]
    for package in packages:

        if not data_has_any_package_garbage(data, package):
            # let's just pretend this package doesn't exist
            continue

        print_log_message(
            "    package: {}".format(package['package_name'])
        )
        print_log_message(
            "    package_description: {}".format(
                package['package_description'])
        )
        print_log_message("        Deaths before = " + str(data.freq.sum()))
        print_log_message("        Rows before = " + str(len(data)))
        print_log_message("            ... calculating proportions")
        proportions = get_proportions(
            data,
            proportion_metadata,
            package,
            cause_map_evaluated,
            residual_cause=residual_cause
        )
        print_log_message("            ... redistributing data")
        data, diagnostics = redistribute_garbage(
            data,
            proportions,
            package
        )
        data = data.loc[(data['freq'] > 0) | (data['cause'] == residual_cause)]
        data = data.groupby(['proportion_id', 'signature_id', 'cause']
                            ).sum().reset_index()
        if diagnostic_output:
            diagnostics['seq'] = seq
            add_cols = ['shared_package_version_id',
                        'package_version_id', 'package_name', 'package_id']
            for add_col in add_cols:
                diagnostics[add_col] = package[add_col]
            seq += 1
            diagnostics_all.append(diagnostics)
        print_log_message("        Deaths after = " + str(data.freq.sum()))
        print_log_message("        Rows after = " + str(len(data)))
    print_log_message("Done!")
    data = pd.merge(data, signature_metadata, on='signature_id')
    if diagnostic_output:
        diagnostics = pd.concat(diagnostics_all).reset_index(drop=True)
    return data.ix[data.freq > 0], diagnostics, \
        signature_metadata, proportion_metadata


class GarbageRedistributor(CodProcess):
    """Redistribute garbage."""

    conf = Configurator('standard')
    rd_inputs_dir = conf.get_directory('rd_package_dir')
    package_dir = rd_inputs_dir + "/{csid}"

    signature_ids = [
        'global', 'dev_status', 'super_region',
        'region', 'country', 'subnational_level1',
        'subnational_level2', 'location_id', 'site_id',
        'year_id', 'sex', 'age', 'age_group_id',
        'nid', 'extract_type_id', 'split_group', 'sex_id'
    ]
    # level at which proportions are calculated for proportional redistribution
    proportion_ids = [
        'global', 'dev_status', 'super_region',
        'region', 'country', 'subnational_level1',
        'site_id', 'year_id', 'sex', 'age',
        'nid', 'extract_type_id', 'split_group'
    ]
    # columns needed for final output
    output_cols = [
        'location_id', 'site_id', 'year_id',
        'nid', 'extract_type_id', 'split_group', 'cause',
        'freq', 'sex_id', 'age_group_id',
    ]
    # the cause that any age-sex-violating redistribution targets should revert
    # to, which will then be redistributed in all-garbage package
    residual_cause = 'ZZZ'

    # whether outputs have been generated
    redistribution_complete = False

    def __init__(self, code_system_id, first_and_last_only=False):
        self.code_system_id = code_system_id
        self.first_and_last_only = first_and_last_only

    def get_computed_dataframe(self, df, cause_map):
        """Distribute garbage coded deaths onto non-garbage."""
        package_folder = self.package_dir.format(csid=self.code_system_id)
        start_freq = df['freq'].sum()
        output_data, diagnostics, signature_metadata, proportion_metadata = \
            run_redistribution(df,
                               self.signature_ids,
                               self.proportion_ids,
                               cause_map,
                               package_folder,
                               residual_cause=self.residual_cause,
                               diagnostic_output=True,
                               first_and_last_only=self.first_and_last_only)
        end_freq = output_data['freq'].sum()
        self.redistribution_complete = True
        output_data = output_data[self.output_cols]
        self.signature_metadata = signature_metadata
        self.diagnostics = diagnostics
        self.proportion_metadata = proportion_metadata
        return output_data

    def get_diagnostic_dataframe(self):
        """Return evaluation of redistribution.
        """
        if self.redistribution_complete:
            return self.diagnostics
        else:
            raise AssertionError(
                "No outputs to pull from - call get_computed_dataframe first"
            )

    def get_signature_metadata(self):
        """Get the signature metadata"""
        if self.redistribution_complete:
            return self.signature_metadata
        else:
            raise AssertionError(
                "No outputs to pull from - call get_computed_dataframe first"
            )

    def get_proportion_metadata(self):
        """Get the proportion metadata"""
        if self.redistribution_complete:
            return self.proportion_metadata
        else:
            raise AssertionError(
                "No outputs to pull from - call get_computed_dataframe first"
            )
