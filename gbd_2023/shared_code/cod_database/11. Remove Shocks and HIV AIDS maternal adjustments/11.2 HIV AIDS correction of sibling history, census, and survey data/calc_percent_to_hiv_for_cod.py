from __future__ import print_function

import pandas as pd
import numpy as np
import sys
import os
import glob

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, "FILEPATH"))
sys.path.append(repo_dir)
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.claude_io import makedirs_safely
from cod_prep.downloaders import get_current_location_hierarchy, add_location_metadata
from cod_prep.utils.misc import get_norway_subnational_mapping
CONF = Configurator('standard')
GBD_2019_CONF = Configurator(refresh_id=36)

ARCH_SUB_DIR = "FILEPATH"
ARCH_DIR = "FILEPATH"

def get_most_recent_date(paf_dir):
    paf_dates = sorted([x for x in os.listdir(paf_dir)])
    most_recent_paf = paf_dates[-1]
    return most_recent_paf


def get_subnationals_to_aggregate(locations, agg_list):
    locations['agg_id'] = locations.path_to_top_parent.apply(
        lambda x: int(x.split(',')[3]) if len(x.split(',')) >= 4 else np.NaN
    )
    agg_ids = locations.loc[
        locations.ihme_loc_id.isin(agg_list)
    ].location_id.unique().tolist()
    sub_locations = locations.loc[
        (locations.agg_id.isin(agg_ids)) &
        (locations.is_estimate == 1)
    ]
    return sub_locations


def collapse_subnationals(pafs, subnationals):
    m = pd.merge(pafs, subnationals, on='location_id', how='inner')
    m.loc[:, 'location_id'] = m.loc[:, 'agg_id']
    draw_cols = [col for col in m.columns if 'draw' in col]
    aggregated = m.groupby(['age_group_id', 'location_id', 'year'])[
        draw_cols].mean().reset_index()
    return aggregated


def make_india_state_aggregates(pafs, locations):
    detail_india = locations.loc[(locations.ihme_loc_id.str.startswith('IND')) &
                                 (locations.level == 5)
                                 ]['location_id'].unique().tolist()
    pafs = pafs.loc[pafs.location_id.isin(detail_india)]

    pafs = add_location_metadata(pafs, 'parent_id',
                                 location_meta_df=locations)
    pafs['location_id'] = pafs['parent_id']
    pafs.drop('parent_id', axis=1, inplace=True)

    draw_cols = [col for col in pafs.columns if 'draw' in col]
    state_aggs = pafs.groupby(['age_group_id', 'location_id', 'year'])[
        draw_cols].mean().reset_index()
    return state_aggs

def make_england_admin1_aggregates(pafs, locations):
    utlas = locations.loc[
        locations.parent_id.isin([4618,4619,4620,4621,4622,4623,4624,4625,4626])
    ].location_id.unique().tolist()
    pafs = pafs.loc[pafs.location_id.isin(utlas)]

    pafs = add_location_metadata(pafs, 'parent_id', location_meta_df=locations)
    pafs['location_id'] = pafs['parent_id']
    pafs = pafs.drop('parent_id', axis=1)

    draw_cols = [col for col in pafs.columns if 'draw' in col]
    aggs = pafs.groupby(
        ['age_group_id', 'location_id', 'year']
    )[draw_cols].mean().reset_index()
    return aggs

def aggregate_to_new_norway_subnationals(pafs):
    df = get_norway_subnational_mapping()
    nor_map = df.set_index('location_id_old')['location_id_new'].to_dict()

    old_nor_rows = pafs.location_id.isin(nor_map.keys())
    old_nor = pafs.loc[old_nor_rows].copy()
    pafs = pafs.loc[~old_nor_rows]

    old_nor['location_id'] = old_nor['location_id'].map(nor_map)
    draw_cols = [col for col in old_nor.columns if 'draw' in col]
    new_nor = old_nor.groupby(['age_group_id', 'location_id', 'year'])[
        draw_cols].mean().reset_index()
    pafs = pafs.append(new_nor)
    return pafs


def read_paf_data(year):
    paf_dir = "FILEPATH"
    paf_fname = get_most_recent_date(paf_dir)

    pafs = pd.read_hdf(
        '{pd}/{pf}/pafs_{y}.h5'.format(
            pd=paf_dir, pf=paf_fname, y=year
        ), 'pafs')
    return pafs


def clean_paf_data(pafs):
    locations = get_current_location_hierarchy(
        location_set_version_id=CONF.get_id("location_set_version")
    )
    old_england_locations = get_current_location_hierarchy(
        location_set_id=35, location_set_version_id=858,
        force_rerun=False, block_rerun=True
    )

    country_list = CONF.get_id("subnational_modeled_iso3s")
    subnationals = get_subnationals_to_aggregate(
        locations, country_list)[['location_id', 'agg_id']]
    aggregates = collapse_subnationals(pafs, subnationals)
    india_states = make_india_state_aggregates(pafs, locations)
    england_admin1 = make_england_admin1_aggregates(pafs, old_england_locations)
    pafs = pd.concat([pafs, aggregates, india_states, england_admin1])

    pafs = pafs.set_index(['age_group_id', 'location_id', 'year'])
    pafs = pafs[[col for col in pafs.columns if 'draw_' in col]]
    return pafs


def calc_props(pafs, prop_maternal):
    to_hiv = pafs * (1 - prop_maternal)
    to_hiv['pct_hiv'] = to_hiv.mean(axis=1)
    to_hiv = to_hiv['pct_hiv'].reset_index()
    to_mat_hiv = pafs * (prop_maternal)
    to_mat_hiv['pct_maternal_hiv'] = to_mat_hiv.mean(axis=1)
    to_mat_hiv = to_mat_hiv['pct_maternal_hiv'].reset_index()
    to_mat_hiv_vr = pafs * prop_maternal / (1 - pafs * prop_maternal)
    to_mat_hiv_vr['pct_maternal_hiv_vr'] = to_mat_hiv_vr.mean(axis=1)
    to_mat_hiv_vr = to_mat_hiv_vr['pct_maternal_hiv_vr'].reset_index()

    props = pd.merge(to_hiv, to_mat_hiv,
                     on=['age_group_id', 'location_id', 'year'],
                     how='inner')
    props = pd.merge(props, to_mat_hiv_vr,
                     on=['age_group_id', 'location_id', 'year'],
                     how='inner')
    props = props.fillna(0)
    return props


def write_maternal_hiv_props(year):
    modelable_entity_id = 9015
    out_dir = "FILEPATH"
    files_to_remove = glob.glob("FILEPATH")
    for f in files_to_remove:
        try:
            os.unlink(f)
        except Exception:
            pass

    prop_maternal = .13 / 1.13

    pafs = read_paf_data(year)

    pafs = clean_paf_data(pafs)

    props = (prop_maternal * pafs).reset_index()

    draw_cols = [col for col in props.columns if 'draw' in col]

    detailed_locations = set(
        get_current_location_hierarchy(
            location_set_version_id=CONF.get_id("location_set_version"))
        .query("most_detailed == 1").location_id
    )
    locations = props.location_id.unique()
    missing_locs = set(detailed_locations) - set(locations)
    assert missing_locs == set(), "Missing: \n{}".format(missing_locs)
    i = 0
    num_locs = len(locations)
    a_tenth = num_locs // 10
    for loc in detailed_locations:
        ld = props.query('location_id==@loc')
        ld = ld[['age_group_id'] + draw_cols]
        ld.to_csv("FILEPATH")
        if i % a_tenth == 0:
            sys.stdout.write('\r')
            sys.stdout.write('[%-10s] %d%%' %
                             ("=" * (i // a_tenth), 10 * (i // a_tenth)))
            sys.stdout.flush()
        i = i + 1

    outfile = "FILEPATH"
    arch_file = "FILEPATH"
    if os.path.exists(outfile):
        os.unlink(outfile)
        
    prop_maternal = .13 / 1.13

    pafs = read_paf_data(year)

    pafs = clean_paf_data(pafs)

    props = calc_props(pafs, prop_maternal)

    props.to_csv(outfile, index=False)
    props.to_csv(arch_file, index=False)

if __name__ == "__main__":
    year = int(sys.argv[1])
    process = sys.argv[2]

    if process == "cod_props":
        main(year)
    elif process == "maternal_hiv":
        write_maternal_hiv_props(year)
    else:
        raise ValueError("ERROR")
