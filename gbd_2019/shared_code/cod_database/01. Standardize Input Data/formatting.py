import os
import numpy as np
import pandas as pd
import warnings
import datetime

from cod_prep.downloaders import (
    add_code_metadata,
    get_nid_metadata,
    get_nidlocyear_map,
    add_location_metadata,
    add_age_metadata,
    get_cod_ages,
    get_sites
)
from cod_prep.utils import (
    report_if_merge_fail, cod_timestamp, add_tbl_metadata, report_duplicates
)
from claude_io import (
    write_phase_output, write_to_claude_nid_table
)
from db_tools import (
    ezfuncs, query_tools
)
from configurator import Configurator

CONF = Configurator('standard')

ALL_CAUSE_EXTRACT_ID = 167


def adjust_representative_id(df):
    """
    Change representative_id to 1 for non-VR.
    """
    if 'ihme_loc_id' not in df.columns:
        df = add_location_metadata(
            df, 'ihme_loc_id',
            location_set_version_id=CONF.get_id('location_set_version')
        )
    df['iso3'] = df['ihme_loc_id'].str[0:3]
    override_locs = ['PAK', 'NGA', 'PHL']
    vr_data_types = [9, 10]
    # TO DO
    # this function should be flexible enough to:
    # 1) check if there are VR data for this specific location/year
    # 2) change represenative id to 0 for non-VR if there is VR
    # 3) change representative id to 1 ONLY if there is no VR present
    # 4) adding a print statement/documentation of loc/years changed would be nice
    # today = datetime.date.today()
    # decomp_step_2_end = datetime.date(2019, 5, 20)
    # assert decomp_step_2_end > today, "Fix this method before we add new data!"
    df.loc[
        (df['iso3'].isin(override_locs)) &
        (df['ihme_loc_id'].str.len()) > 4 &
        (~df['data_type_id'].isin(vr_data_types)), 'representative_id'
    ] = 1

    return df


def group_six_minor_territories(df, sum_cols):
    """Replace the location_id to be six minor territories if it is one.

    Directly modifies the given dataframe so that if the location id is one
    of the locations for India's Six Minor Territories, Urban',
    it gets that location id, and same with 'Six Minor Territories, Rural'.

    Args:
        df (DataFrame):

    Returns:
        DataFrame
    """
    df = df.copy()
    urban_terr = [43871, 43876, 43878, 43879, 43889, 43897]
    rural_terr = [43907, 43912, 43914, 43915, 43925, 43933]
    df.loc[df['location_id'].isin(urban_terr), 'location_id'] = 44540
    df.loc[df['location_id'].isin(rural_terr), 'location_id'] = 44539
    assert not (df['location_id'].isin(urban_terr) |
                df['location_id'].isin(rural_terr)).values.any()
    group_cols = list(set(df.columns) - set(sum_cols))
    df = df.groupby(group_cols, as_index=False)[sum_cols].sum()
    return df


def induce_extraction_type(df, conn_def='prodcod'):
    """Induce extract type from the loc-type of the most detailed location."""
    # pull locations from shared.location directly because
    # location hierarchy versions will be inherently finnicky
    # for situations where extraction type falls out of the norm
    min_data_location_id = df.location_id.min()
    max_data_location_id = df.location_id.max()
    # do not think there will be lots of parallel formatting jobs, so this
    # feels like an alright situation to query the DB directly
    location_types = ezfuncs.query("""
        SELECT
            location_id, location_type_id,
            location_level AS level
        FROM shared.location
        WHERE location_id BETWEEN {} AND {}
    """.format(min_data_location_id, max_data_location_id),
        conn_def=conn_def)

    level_types = add_tbl_metadata(
        location_types,
        df, ['level', 'location_type_id'],
        'location_id',
        force_rerun=False, cache_dir='standard',
        location_set_version_id=CONF.get_id('location_set_version')
    )
    report_if_merge_fail(level_types, 'location_type_id', 'location_id')
    level_types = level_types[['level', 'location_type_id']].drop_duplicates()

    max_level = level_types.level.max()
    level_types = level_types.loc[level_types['level'] == max_level]
    level_types.sort_values(by='location_type_id')

    # extra verification that there is just one loc type
    level_types = level_types.drop_duplicates('location_type_id', keep='first')
    location_type_id = level_types['location_type_id'].iloc[0]

    loc_type = ezfuncs.query("""
        SELECT location_type
        FROM shared.location_type
        WHERE location_type_id = {}
    """.format(location_type_id), conn_def=conn_def)
    return str(loc_type.iloc[0, 0])


def assert_is_valid_source(source):
    """Verify that source(s) actually exists in the datadir.

    Arguments:
        source, str or list of str: source or sources to check
    """
    if type(source) == str:
        source = [source]
    ddir = CONF.get_directory('jdatasets')
    valid_sources = os.listdir(ddir)
    valid_sources.remove("_README.txt")
    # don't upload sources with underscores in the front
    valid_sources = [
        vs.strip("_") for vs in valid_sources if "Incoming_tmp" not in vs
    ]
    bad_ones = set(source) - set(valid_sources)
    if len(bad_ones) > 0:
        raise AssertionError(
            "These sources are not in 03_datases on the J drive "
            "(remember to remove underscores from source): \n"
            "{}".format(bad_ones)
        )


def insert_names(name_table, name_df, df_name_col=None, conn_def='ADDRESS'):
    """Insert the name in the df to cod.name_table table."""
    # make sure the column name in the df matches that in the db
    name_tables_to_col_name = {
        'site': 'site_name',
        'source': 'source_name'
    }
    assert name_table in name_tables_to_col_name.keys(), \
        "Invalid name table: {}".format(name_table)
    name_col = name_tables_to_col_name[name_table]

    assert set(name_df.columns) == set([name_col]), \
        "Pass a df with one column: '{}'. You gave a df with these " \
        "columns: {}".format(name_col, name_df.columns)

    # verify that sources are ok to upload
    if name_table == "source":
        assert_is_valid_source(name_df[name_col].unique())

    # restrict data to just that
    name_df = name_df[[name_col]].drop_duplicates().dropna()
    unique_names = name_df[name_col].unique()
    names_clause = "','".join(unique_names)
    names_query = """
        SELECT *
        FROM cod.{name_table}
        WHERE {name_col} IN ('{names_clause}')
    """.format(
        name_table=name_table, name_col=name_col, names_clause=names_clause
    )
    overlap = ezfuncs.query(names_query, conn_def=conn_def)
    if len(overlap) > 0:
        raise AssertionError(
            "Conflicting {name_col}s already present: \n{overlap}".format(
                name_col=name_col, overlap=overlap)
        )
    engine = ezfuncs.get_engine(conn_def)
    conn = engine.connect()
    name_df.to_sql(name_table, conn, if_exists='append', index=False)
    conn.close()
    print("Uploaded new {name_col}s: \n{name_df}".format(
        name_col=name_col, name_df=name_df))


def map_site_id(df, site_col='site', conn_def='ADDRESS', upload=True):
    """Map site_id to the data given site_col.

    Will upload sites to cod.site if necessary.
    """
    # set cache options
    force_cache_options = {
        'force_rerun': True,
        'block_rerun': False,
        'cache_dir': "standard",
        'cache_results': True,
        'verbose': True
    }

    # named site_name in db
    df = df.rename(columns={site_col: 'site_name'})
    df['site_name'] = df['site_name'].fillna("")

    # get site names in db
    unique_sites = df[['site_name']].drop_duplicates()
    db_sites = get_sites(**force_cache_options)
    # make site_name both lower for merge, since mysql site name is
    # case-insensitive
    # and this is how outliers are stored
    unique_sites['site_name_orig'] = unique_sites['site_name']
    unique_sites['site_name'] = unique_sites['site_name'].str.strip().str.lower()
    db_sites['site_name'] = db_sites['site_name'].str.strip().str.lower()

    # merge onto ones in df
    unique_sites = unique_sites.merge(db_sites, how='left')
    unique_sites['site_name'] = unique_sites['site_name_orig']
    unique_sites = unique_sites.drop('site_name_orig', axis=1)

    # find missings
    upload_sites = unique_sites[unique_sites['site_id'].isnull()]
    upload_sites = upload_sites[['site_name']].drop_duplicates()
    if len(upload_sites) > 0:
        print(
            "No site_id for sites {}".format(upload_sites.site_name.unique())
        )
        if upload:
            print("Uploading new sites...")
            # if any, upload them
            insert_names('site', upload_sites, conn_def=conn_def)

            # refresh db_sites
            db_sites = get_sites(**force_cache_options)
            unique_sites = unique_sites.drop('site_id', axis=1)
            unique_sites = unique_sites.merge(db_sites, how='left')
            report_if_merge_fail(unique_sites, 'site_id', 'site_name')
        else:
            print("Not uploading new sites, allowing merge to fail...")

    df = df.merge(unique_sites, on='site_name', how='left')
    if upload:
        report_if_merge_fail(unique_sites, 'site_id', 'site_name')

    return df


def map_extract_type_id(df, source, extract_type, conn_def='ADDRESS'):
    nid_extract_types = {}
    start_extract_type = extract_type
    for nid in df.nid.unique():
        if source == "China_DSP_prov_ICD10":
            extract_type = "admin1: DSP sites only"
        elif start_extract_type is None:
            extract_type = None
            extract_type = induce_extraction_type(
                df.loc[df['nid'] == nid],
                conn_def=conn_def)
        else:
            extract_type = extract_type

        extract_type = str(extract_type).strip()
        # Determine (or create) the extract_type_id
        extract_type_id = pull_or_create_extract_type(
            extract_type, conn_def=conn_def
        )
        assert extract_type_id is not None, "Weird fail"
        nid_extract_types[nid] = extract_type_id

    df['extract_type_id'] = df['nid'].map(nid_extract_types)
    report_if_merge_fail(df, 'extract_type_id', 'nid')

    split_iso_nids = list(pd.read_csv(CONF.get_resource('split_iso_nids'))['nid'].unique())
    if (df['nid'].isin(split_iso_nids).any()):
        nid_loc_df = df.loc[
            df['nid'].isin(split_iso_nids),
            ['nid', 'location_id']
        ].drop_duplicates()
        nid_loc_df = add_location_metadata(
            nid_loc_df, 'ihme_loc_id',
            location_set_version_id=CONF.get_id('location_set_version')
        )
        nid_loc_df['iso3'] = nid_loc_df['ihme_loc_id'].str.slice(0, 3)

        nid_loc_df['extract_type'] = nid_loc_df.apply(
            lambda x: "{nid}: {iso3} data".format(
                nid=x['nid'], iso3=x['iso3']),
            axis=1
        )
        extract_types = nid_loc_df[['extract_type']].drop_duplicates()
        extract_types['extract_type_id_new'] = \
            extract_types['extract_type'].apply(
                lambda x: pull_or_create_extract_type(x, conn_def=conn_def))
        nid_loc_df = nid_loc_df.merge(
            extract_types, on='extract_type', how='left')
        report_if_merge_fail(nid_loc_df, 'extract_type_id_new', 'extract_type')
        df = df.merge(nid_loc_df, on=['nid', 'location_id'], how='left')
        report_if_merge_fail(
            df.loc[df['nid'].isin(split_iso_nids)],
            'extract_type_id_new', ['nid', 'location_id'])
        df.loc[
            df['nid'].isin(split_iso_nids),
            'extract_type_id'] = df['extract_type_id_new']
        df = df.drop(
            ['extract_type_id_new', 'iso3', 'ihme_loc_id', 'extract_type'],
            axis=1
        )

    if source == "VA_lit_GBD_2010_2013":
        # nids need extract types by code system
        # these have already been inserted to extract type table
        code_system_to_extract_type_id = {
            # DRC nationwide 2004
            244: 350,
            # DRC EastWest 2002
            273: 347,
            # Halder (30)
            294: 353,
            # Moran (26)
            296: 356,
        }
        df.loc[
            df['nid'].isin([93570, 93664]), 'extract_type_id'
        ] = df['code_system_id'].map(code_system_to_extract_type_id)

    # police data fixes
    elif source == "Various_RTI":
        code_system_to_extract_type_id = {
            # Economic Commission for Europe
            237: 719,
            # Turkey Traffic Accidents
            241: 722,
        }
        df.loc[
            df['nid'].isin([93599]), 'extract_type_id'
        ] = df['code_system_id'].map(code_system_to_extract_type_id)

    # distinguish between Matlab sources since all are NID 935
    # these etids have already been inserted into the claude_extract_type
    elif source == 'Matlab_1963_1981':
        df.loc[df['nid'] == 935, 'extract_type_id'] = 380
    elif source == 'Matlab_1982_1986':
        df.loc[df['nid'] == 935, 'extract_type_id'] = 383
    elif source == 'Matlab_1987_2002':
        df.loc[df['nid'] == 935, 'extract_type_id'] = 371
    elif source == 'Matlab_2003_2006':
        df.loc[df['nid'] == 935, 'extract_type_id'] = 374
    elif source == 'Matlab_2007_2012':
        df.loc[df['nid'] == 935, 'extract_type_id'] = 377

    # fixes for "maternal" data
    elif source == "Combined_Census":
        df.loc[df['nid'] == 7942, 'extract_type_id'] = 479
    elif source == "Maternal_Census":
        df.loc[df['nid'] == 7942, 'extract_type_id'] = 482
        df.loc[df['nid'] == 10319, 'extract_type_id'] = 485
    elif source == "Other_Maternal":
        df.loc[df['nid'] == 10319, 'extract_type_id'] = 488
    # these two Pakistan_DHS sources have the same NID
    elif source == "Pakistan_maternal_DHS_2006":
        df["extract_type_id"] = 539
    elif source == "Pakistan_child_DHS_2006":
        df["extract_type_id"] = 536

    # age-specific maps in nid 413394 - need different extracts
    # for different code systems
    elif source == 'VA_lit_GBD_2019':
        code_system_to_extract_type_id = {711: 1745, 712: 1748}
        df.loc[df['nid'] == 413394, 'extract_type_id'] =\
            df['code_system_id'].map(code_system_to_extract_type_id)
        assert df.extract_type_id.notnull().all()

    if (df['data_type_id'].isin([5, 6, 7]).any()):
        # these have already been inserted to extract type table
        csetid = {49: 440, 50: 443, 52: 386, 162: 446, 163: 449, 155: 455,
                  148: 458, 149: 461, 150: 464, 156: 467, 577: 470}
        df.loc[
            df['nid'].isin([24134, 108611, 125702, 132803, 154012]),
            'extract_type_id'] = df['code_system_id'].map(csetid)
        data_type_dict = {5: 473, 7: 476}
        df.loc[
            df['code_system_id'] == 154, 'extract_type_id'
        ] = df['data_type_id'].map(data_type_dict)
    return df


def pull_or_create_extract_type(extract_type, conn_def='ADDRESS'):
    extract_type_id = pull_extract_type_id(extract_type, conn_def=conn_def)
    if extract_type_id is None:
        insert_extract_type(extract_type, conn_def=conn_def)
        extract_type_id = pull_extract_type_id(extract_type, conn_def=conn_def)
    return extract_type_id


def insert_extract_type(extract_type, conn_def='ADDRESS'):
    """Insert a new extract type."""
    assert pull_extract_type_id(extract_type, conn_def=conn_def) is None, \
        "Already exists"
    engine = ezfuncs.get_engine(conn_def)
    conn = engine.connect()
    conn.execute("""
        INSERT INTO cod.claude_extract_type
            (extract_type)
            VALUES
            ("{}")
    """.format(extract_type))
    conn.close()


def pull_extract_type_id(extract_type, conn_def='ADDRESS'):
    """Query cod database for extract_type_id."""
    extract_type_id = ezfuncs.query("""
        SELECT extract_type_id
        FROM cod.claude_extract_type
        WHERE extract_type = "{}"
    """.format(extract_type), conn_def=conn_def)
    if len(extract_type_id) == 1:
        return extract_type_id.iloc[0, 0]
    elif len(extract_type_id) == 0:
        return None
    else:
        raise AssertionError("Got multiple rows: {}".format(extract_type_id))


def insert_source_id(source, conn_def='ADDRESS'):
    """Insert a new source_id."""
    if pull_source_id(source, conn_def=conn_def) is None:
        engine = ezfuncs.get_engine(conn_def)
        conn = engine.connect()
        print('\nInserting new source to cod.source table')
        conn.execute("""
            INSERT INTO cod.source
                (source_name)
                VALUES
                ("{}")
        """.format(source))
        conn.close()
    else:
        print("Source already exists")


def pull_source_id(source, conn_def='ADDRESS'):
    """Query cod database for source_id."""
    source_id = ezfuncs.query("""
        SELECT DISTINCT source_id
        FROM cod.source
        WHERE source_name = "{}"
    """.format(source), conn_def=conn_def)
    if len(source_id) == 1:
        return source_id.iloc[0, 0]
    elif len(source_id) == 0:
        return None
    else:
        raise AssertionError("Got multiple rows: {}".format(source_id))


def check_subnational_locations(df):
    """Check that there are no national observations where we model subnationally.

    Use the list of subnationally modeled countries to ensure that there are no
    national observations. This will raise an assertion error, and the user
    should either re extract subnational information from the original data
    or accept that it will remain inactive.
    """
    subnational_iso3s = CONF.get_id('subnational_modeled_iso3s')
    # exceptions for Ukraine which is location split in aggregation
    # remove Ukraine-- this is a special case where we're splitting
    # the national Ukraine data to Ukraine without crimea/sevastopol
    # later the in the prep process (see count_adjustments.py)
    if "UKR" in subnational_iso3s:
        subnational_iso3s.remove('UKR')
    df = add_location_metadata(
        df, 'ihme_loc_id',
        location_set_version_id=CONF.get_id('location_set_version')
    )
    nid_etid_pairs = list(
        df[
            ['nid', 'extract_type_id']
        ].drop_duplicates().to_records(index=False))
    nid_etid_pairs = [tuple(pair) for pair in nid_etid_pairs]

    for x in nid_etid_pairs:
        nid = x[0]
        etid = x[1]
        check_loc = list(df.loc[
            (df['nid'] == nid) & (df['extract_type_id'] == etid), 'ihme_loc_id'
        ].unique())[0]
        if check_loc in subnational_iso3s:
            print(
                "Found {} where we model subnationally "
                "for NID {} extract_type_id {}. Setting is_active"
                " to False".format(check_loc, nid, etid)
            )
            df.loc[
                (df['nid'] == nid) &
                (df['extract_type_id'] == etid), 'is_active'
            ] = 0
    return df


def pull_nid_metadata(conn_def='ADDRESS'):
    """Query cod database for all entries in cod.claude_nid_metadata table"""
    is_active_status = ezfuncs.query("""
        SELECT nid, parent_nid, extract_type_id, code_system_id,
               data_type_id, source, is_active, is_mort_active
        FROM cod.claude_nid_metadata
    """, conn_def=conn_def)
    return is_active_status


def report_gaps_and_overlaps(nid_etid_tuple, nid_etid_df):
    '''
    Raise an assertion error if there are gaps or overlaps between age groups in nid_etid_df.

    Note that gaps between age groups are allowed for VA, survey/census, and CHAMPS data
    (data_type_ids 7, 8, and 12).

    NEED TO REVIEW: Should we continue to allow overlapping age groups in any data type?

    Arguments:
        nid_etid_tuple, tuple: specifies NID and extract_type_id of nid_etid_df
        nid_etid_df, pandas.DataFrame: A df of unique age groups from the data for the NID and
        extract_type specified by nid_etid_tuple. The columns of nid_etid_df are age_group_id,
        age_group_years_start, age_group_years_end, and data_type_id, and each row is a unique age
        group. The age groups are assumed to be sorted in ascending order, first by
        age_group_years_start, then by age_group_years_end.
    '''

    data_type_id = nid_etid_df.data_type_id.unique()
    assert len(data_type_id) == 1, "NID, extract_type {} has more than 1 data_type_id".format(
        nid_etid_tuple)

    gaps = []
    overlaps = []
    for i in range(len(nid_etid_df) - 1):
        group_end = nid_etid_df.age_group_years_end.iloc[i]
        next_group_start = nid_etid_df.age_group_years_start.iloc[i + 1]
        if next_group_start == 999:
            break
        elif group_end < next_group_start:
            pair_with_gap = nid_etid_df.iloc[[i, i + 1]].copy()
            gaps.append(pair_with_gap)
        elif group_end > next_group_start:
            pair_with_overlap = nid_etid_df.iloc[[i, i + 1]].copy()
            overlaps.append(pair_with_overlap)

    if (gaps != []) & (data_type_id[0] not in [7, 8, 12]):
        gaps_df = pd.concat(gaps, ignore_index=True)
        raise AssertionError(
            "The following pairs of age groups in the data have gaps between them for "
            "NID, extract_type {}: \n {}".format(nid_etid_tuple, gaps_df)
        )
    if (overlaps != []) & (data_type_id[0] not in [6, 7, 8]):
        overlaps_df = pd.concat(overlaps, ignore_index=True)
        if 22 in overlaps_df.age_group_id.unique():
            warnings.warn(
                'The "All Ages" age group (age_group_id = 22) is in the data and overlaps '
                'with at least one other age group. If this age group contains UNIQUE deaths whose '
                'ages are unknown, then these deaths should be reassigned to the "Unknown" age group '
                '(age_group_id = 283).'
            )
        raise AssertionError(
            "The following pairs of age groups in the data overlap for NID, extract_type "
            "{}: \n {}".format(nid_etid_tuple, overlaps_df)
        )


def report_too_much_age_detail(nid_etid_tuple, nid_etid_df):
    '''
    Raise an assertion error if there is too much age detail in the age groups in nid_etid_df.

    For the age groups listed in nid_etid_df, report if there are age groups that can be
    aggregated to more closely match the ideal cod age groups. Note that VA, survey/
    census, and CHAMPS data are NOT checked for too much age detail when passed to this function.

    Arguments:
        nid_etid_tuple, tuple: specifies NID and extract_type_id of nid_etid_df
        nid_etid_df, pandas.DataFrame: A df of unique age groups from the data for the NID and
        extract_type specified by nid_etid_tuple. The columns of nid_etid_df are age_group_id,
        age_group_years_start, age_group_years_end, and data_type_id, and each row is a unique age
        group. The age groups are assumed to be sorted in ascending order, first by
        age_group_years_start, then by age_group_years_end. The age groups are also assumed
        to have no gaps or overlaps between them.
    '''

    data_type_id = nid_etid_df.data_type_id.unique()
    assert len(data_type_id) == 1, "NID, extract_type {} has more than 1 data_type_id".format(
        nid_etid_tuple)

    if data_type_id[0] not in [7, 8, 12]:
        cod_ages = get_cod_ages()
        cod_ages = cod_ages[['age_group_id', 'age_group_years_start', 'age_group_years_end']]
        cod_ages.rename(columns=lambda x: "cod_" + x, inplace=True)

        # For each cod age group, find age groups in the df that are contained within it
        cod_ages['df_age_group_ids'] = cod_ages.apply(
            lambda x: nid_etid_df.loc[
                ((nid_etid_df.age_group_years_start >= x['cod_age_group_years_start']) &
                 (nid_etid_df.age_group_years_end <= x['cod_age_group_years_end'])), 'age_group_id'
            ].tolist(), axis='columns'
        )

        # If a cod age group contains more than 1 df age group, then the df is too detailed
        cod_ages['is_too_detailed'] = cod_ages.df_age_group_ids.apply(lambda x: len(x) > 1)

        if cod_ages.is_too_detailed.any():
            raise AssertionError("Age groups in the data can be aggregated to match standard "
                                 "cod age groups for NID, extract_type {}: \n {}".format(
                                     nid_etid_tuple, cod_ages.loc[cod_ages.is_too_detailed]))


def report_no_terminal_ages(nid_etid_tuple, nid_etid_df):
    """Raise an assertion error if there is no terminal age group for VR."""
    vr = (nid_etid_df['data_type_id'].isin([9, 10]))
    has_terminal_age = (nid_etid_df['age_group_years_end'] == 125)
    # non-vr or maternal data exempt from needing terminal age_groups
    # Other_maternal (93739) has vr within, making an exception
    if (len(nid_etid_df[vr]) == 0) | (93739 in nid_etid_tuple):
        pass
    elif len(nid_etid_df[vr & has_terminal_age]) == 0:
        raise AssertionError(
            "The following NID, extract_type pair lacks a terminal age group: {}.".format(
                nid_etid_tuple)
        )


def check_age_groups(df):
    '''Raise a series of assertion errors if standard checks for age groups are violated.

        Input: pandas DataFrame
        Returns: None, raises assertion errors

        Checks for:
        - age_group_ids not in shared.ages
        - overlapping age groups
        - lack of terminal age group (e.g. 95+) in VR
    '''
    for nid_etid, nid_etid_df in df.groupby(['nid', 'extract_type_id']):
        nid_etid_df = add_age_metadata(
            nid_etid_df, ['age_group_years_start', 'age_group_years_end']
        )
        report_if_merge_fail(df, 'age_group_id', 'age_group_years_start')
        report_if_merge_fail(df, 'age_group_id', 'age_group_years_end')
        nid_etid_df = nid_etid_df[
            ['age_group_id', 'age_group_years_start', 'age_group_years_end', 'data_type_id']
        ].drop_duplicates().sort_values(by=['age_group_years_start', 'age_group_years_end'])

        # NOTE: report_gaps_and_overlaps MUST come before report_too_much_age_detail
        # report_too_much_age_detail cannot handle age groups with gaps or overlaps
        report_gaps_and_overlaps(nid_etid, nid_etid_df)
        report_no_terminal_ages(nid_etid, nid_etid_df)
        report_too_much_age_detail(nid_etid, nid_etid_df)


def check_vr_raw_causes(df):
    """Check for common mistakes in cause formatting for VR data."""
    if len(df.loc[df['data_type_id'].isin([9, 10])]) > 0:
        # only have checks for ICD10 and ICD9 detail data at the moment
        # to add more checks, remove this this if block
        if len(df.loc[df['code_system_id'].isin([1, 6])]) > 0:
            message = ""
            code_system_ids = df['code_system_id'].unique()
            for code_system_id in code_system_ids:
                cs_df = df.query("code_system_id == {}".format(code_system_id))
                cs_df = add_code_metadata(cs_df, 'value', code_system_id=code_system_id)
                if code_system_id == 6:
                    # check for N codes
                    ncode_df = cs_df.loc[cs_df['value'].str.contains('^[89]')]
                    if len(ncode_df) > 0:
                        message = "!!CONFIRM OR CHANGE TO E CODES!! \nNature of injury"\
                            " codes will be mapped to garbage \n{}".format(ncode_df.head())
                if code_system_id == 1:
                    ucode_df = cs_df.loc[cs_df['value'].str.startswith('U0')]
                    if len(ucode_df) > 0:
                        message = "These codes should only be in US data"\
                            " \n{}".format(ucode_df)
                    stcode_df = cs_df.loc[cs_df['value'].str.contains('^[ST]')]
                    if len(stcode_df) > 0:
                        message = "Data contain S/T codes that will"\
                            " mostly be mapped to garbage \n{}".format(stcode_df)
                if message != "":
                    warnings.warn(message)


def refresh_claude_nid_cache_files():
    """Automatically refresh cached files from claude tables."""
    print("\nRefreshing claude nid metadata cache files")
    force_cache_options = {
        'force_rerun': True,
        'block_rerun': False,
        'cache_dir': "standard",
        'cache_results': True,
        'verbose': True
    }
    get_nid_metadata(**force_cache_options)
    get_nidlocyear_map(**force_cache_options)


def finalize_formatting(df, source, write=False, code_system_id=None, extract_type=None,
                        conn_def='ADDRESS', is_active=False, refresh_cache=True, check_ages=True):
    """Finalize the formatting of the source and optionally write it out.

    Decides whether to map code_id based on whether code_id is already a
        column in the dataset.

    Needs the following information from either the df values or from the
        nid_meta_vals dict:

            data_type_id
            representative_id

        All of the above must have only one value per nid in df.

    Maps site_id to the data based on incoming 'site' column. Will upload
        any sites that are not in the cod.site table already.

    Arguments:
        df, pandas.DataFrame: The dataframe with near-formatted data
        source, str: The source this df is (should be the whole source and
            nothing but the source). Will break if there is no source in
            FILEPATH with this name, and you should pass the
            source without a leading underscore even if it is that way
            in J
        write, bool: whether to write the outputs
        extract_type, str: The manner in which the nid was extracted. If
            left as None, will be induced by the location_type_id of
            the location_id with the maximum level in the dataset. This should
            be over-ridden in cases like China DSP, where the same locations
            are used in two extraction types - "DSP + VR" and "DSP"; China DSP
            then gets two extraction types: "admin1" and
            "admin1: DSP sites only" (in the particular instance of DSP,
            extract type is built into this code. Feel free to add other
            source-extract type mappings here to force consistency.)
        check_ages, bool: Whether or not to enforce age group checks such as
            ensuring no overlaps or gaps. This can be turned off because sometimes
            raw data reports overlapping age groups (e.g. Palestine data has Gaza Strip and West
            Bank data with different age groupings).

    Returns:
        Every local value to the function
        Why? There are multiple df outputs, and formatting is a very
        engaged process so its helpful to just see everything sometimes
    """
    # set column groups, and verify that we have everything we need
    NID_META_COLS = [
        'nid', 'parent_nid', 'extract_type_id', 'source', 'data_type_id',
        'code_system_id', 'is_active', 'is_mort_active'
    ]
    NID_LOCATION_YEAR_COLS = [
        'nid', 'extract_type_id', 'location_id', 'year_id', 'representative_id'
    ]
    FORMATTED_ID_COLS = [
        'nid', 'extract_type_id', 'code_id', 'sex_id', 'site_id', 'year_id',
        'age_group_id', 'location_id'
    ]
    if 'code_id' in df.columns:
        code_col = 'code_id'
        map_code_id = False
    elif 'cause' in df.columns:
        code_col = 'cause'
        map_code_id = True
    else:
        raise AssertionError("Need either 'code_id' or 'cause' in columns")
    INCOMING_EXPECTED_ID_COLS = [
        'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id', code_col,
        'site', 'data_type_id', 'representative_id', 'code_system_id'
    ]
    VALUE_COLS = ['deaths']
    FINAL_FORMATED_COLS = FORMATTED_ID_COLS + VALUE_COLS

    missing_cols = set(INCOMING_EXPECTED_ID_COLS) - set(df.columns)
    assert len(missing_cols) == 0, \
        "Required formatting columns not found in df: \n{}".format(missing_cols)

    # SET FORMATTING TIMESTAMP
    format_timestamp = cod_timestamp()
    print("Finalizing formatting with timestamp {}".format(format_timestamp))

    # ADD SOURCE
    df['source'] = source

    # MAP OR CHECK CODE ID
    code_system_ids = df.code_system_id.unique()
    if map_code_id:
        cs_dfs = []
        for code_system_id in code_system_ids:
            cs_df = df.loc[df['code_system_id'] == code_system_id].copy()
            # map code_id to the data
            cs_df['value'] = cs_df['cause']
            cs_df = add_code_metadata(cs_df, ['code_id'], code_system_id=code_system_id,
                                      merge_col='value', force_rerun=True, cache_dir='standard')
            report_if_merge_fail(cs_df, ['code_id'], ['value'])
            cs_df = cs_df.drop('value', axis=1)
            cs_dfs.append(cs_df)
        df = pd.concat(cs_dfs, ignore_index=True)
    else:
        # CHECK THAT EVERY CODE_ID IS IN THE ENGINE ROOM AND IN THE CODE SYSTEM
        all_codes_q = """
            SELECT code_id
            FROM engine_room.maps_code
            WHERE code_system_id IN ({})
        """.format(",".join([str(c) for c in code_system_ids]))
        all_codes = ezfuncs.query(all_codes_q, conn_def='ADDRESS')
        bad_codes = set(df.code_id) - set(all_codes.code_id)
        assert len(bad_codes) == 0, "Found code ids in data that can't exist in code "\
                                    "systems {}: {}".format(code_system_ids, bad_codes)
    check_vr_raw_causes(df)

    # MAP SITE ID
    df = map_site_id(df, conn_def=conn_def)
    # MAP EXTRACT TYPE ID
    df = map_extract_type_id(df, source, extract_type, conn_def=conn_def)
    # CHANGE SIX MINOR TERRITORIES TO AGGREGATE UNION LOCATIONS
    df = group_six_minor_territories(df, sum_cols=VALUE_COLS)

    # sorry for putting this here
    # drop these loc/years b/c env < deaths creating negative cc_code
    # maybe re run w/ another envelope?
    df = df.loc[~((df['nid'] == 279644) & (df['year_id'] == 2011))]
    df = df.loc[~(df['nid'].isin([24143, 107307]))]

    # ENSURE NO NEGATIVES
    for val_col in VALUE_COLS:
        assert (df[val_col] >= 0).all(), \
            "there are negative values in {}".format(val_col)

    ################################################
    # keep all 0s now, messing up for NR in non-VR
    # df['val_sum_tmp'] = df[VALUE_COLS].sum(axis=1)
    # all-cause extractions want to keep zeroes
    # keep_zeroes = df['extract_type_id'] == ALL_CAUSE_EXTRACT_ID
    # otherwise, drop them
    # greater_than_zero = df['val_sum_tmp'] > 0
    # df = df[greater_than_zero | keep_zeroes]
    # df = df.drop('val_sum_tmp', axis=1)
    ################################################

    # CHECKS FOR FORMATTED PHASE OUTPUT
    input_df = df[FINAL_FORMATED_COLS].copy()
    assert not input_df.isnull().values.any(), "null values in df"
    dupped = input_df[input_df.duplicated()]
    if len(dupped) > 0:
        raise AssertionError("duplicate values in df: \n{}".format(dupped))

    # GROUP IF NECESSARY
    if input_df[FORMATTED_ID_COLS].duplicated().any():
        input_df = input_df.groupby(
            FORMATTED_ID_COLS, as_index=False)[VALUE_COLS].sum()

    # TESTS F0R CHECKING AGE GROUP IDS
    if check_ages:
        check_age_groups(df)

    # MORE TESTS FOR DEATHS - MAYBE THAT THEY AREN'T MORE THAN 1.25 THE
    # VALUE IN THE ENVELOPE BY LOCATION AGE YEAR SEX?

    # AND THEN WRITE A TABLE OF COMPARISONS OF DEATHS / ENVELOPE BY LOCATION
    # AGE YEAR SEX FOR REVIEW

    # MAKE NID METADATA TABLE
    if 'parent_nid' not in df.columns:
        df['parent_nid'] = np.nan

    if is_active is True:
        warnings.warn("""is_active is deprecated: use the update_nid_metadata_status
                         function to change the status of finalized datasets""")

    # Use existing is_active and is_mort_active values, otherwise default to 0
    nid_map = pull_nid_metadata()
    df = df.merge(nid_map,
                  on=['nid', 'parent_nid', 'extract_type_id',
                      'source', 'data_type_id', 'code_system_id'],
                  how='left')

    df_na = df[pd.isnull(df['is_active'])]
    df_na = df_na[['nid', 'extract_type_id']].drop_duplicates()

    if df_na.shape[0] > 0:
        print("""New rows for the following NID/extract_type_id will be added
                 with is_active and is_mort_active = 0:\n {}""".format(df_na))

    df['is_active'] = df['is_active'].fillna(0)
    df['is_mort_active'] = df['is_mort_active'].fillna(0)

    # CHECK SUBNATIONAL LOCATIONS
    df = check_subnational_locations(df)

    # OVERRIDE REPRESENTATIVE ID FOR NON-VR
    df = adjust_representative_id(df)

    nid_meta_df = df[NID_META_COLS].drop_duplicates()
    nid_meta_df['last_formatted_timestamp'] = format_timestamp

    # MAKE NID LOCATION YEAR TABLE
    nid_locyears = df[NID_LOCATION_YEAR_COLS].drop_duplicates()
    nid_locyears['last_formatted_timestamp'] = format_timestamp
    # check one iso3 per nid
    nid_locyears = add_location_metadata(nid_locyears, 'ihme_loc_id')
    nid_locyears['iso3'] = nid_locyears['ihme_loc_id'].str.slice(0, 3)
    report_duplicates(
        nid_locyears[['nid', 'extract_type_id', 'iso3']].drop_duplicates(),
        ['nid', 'extract_type_id']
    )
    nid_locyears = nid_locyears.drop(['ihme_loc_id', 'iso3'], axis=1)

    if write:
        # write nid metadata
        write_to_claude_nid_table(
            nid_meta_df, 'claude_nid_metadata', replace=True, conn_def=conn_def
        )

        # write nid location-year map
        write_to_claude_nid_table(
            nid_locyears, 'claude_nid_location_year', replace=True,
            conn_def=conn_def
        )

        # write to cod.source for new sources
        insert_source_id(source)

        nid_extracts = input_df[
            ['nid', 'extract_type_id']
        ].drop_duplicates().to_records(index=False)
        for nid, extract_type_id in nid_extracts:
            nid = int(nid)
            extract_type_id = int(extract_type_id)
            print("Writing nid {}, extract_type_id {}".format(
                nid, extract_type_id))
            idf = input_df.loc[
                (input_df['nid'] == nid) &
                (input_df['extract_type_id'] == extract_type_id)
            ].copy()
            phase = 'formatted'
            launch_set_id = format_timestamp
            print("\nTotal deaths: {}".format(idf.deaths.sum()))
            write_phase_output(idf, phase, nid, extract_type_id, launch_set_id)

        # now refresh cache files for nid
        if refresh_cache:
            refresh_claude_nid_cache_files()

    return locals()


def update_nid_metadata_status(nid, extract_type_id,
                               is_active=None, is_mort_active=None, conn_def='ADDRESS'):
    """Update the status of an extraction (uniquely identified by NID and extract_type_id)

    Updates the values of is_active and/or is_mort_active in the cod.claude_nid_metadata
    table based on the the NID and extract_type_id


    Arguments:
        nid, int: NID to modify
        extract_type_id, int: extract_type_id to modify
        is_active, bool: (optional) new value for is_active to be updated in the database.
                        Default: None (does not take any action)
        is_mort_active, bool: (optional) new value for is_mort_active to be updated in the database
                        Default: None (does not take any action)
    """
    is_active_status = ezfuncs.query("""
        SELECT is_active, is_mort_active, data_type_id
        FROM cod.claude_nid_metadata
        WHERE nid = {}
        AND extract_type_id = {}
        """.format(nid, extract_type_id), conn_def=conn_def)

    if is_active_status.shape[0] == 0:
        raise AssertionError("""nid {}, extract_type_id {} does not exist in
                                cod.claude_nid_metadata""".format(nid, extract_type_id))

    if is_active_status.shape[0] > 1:
        raise AssertionError("""nid {}, extract_type_id {} has multiple entries
                                in cod.claude_nid_metadata""".format(nid, extract_type_id))

    # If not specified, inherit is_active/is_mort_active from the current status
    # If specified as a bool, convert to 1/0
    if is_active is None:
        is_active = is_active_status['is_active'][0]
    else:
        is_active = 1 * is_active

    if is_mort_active is None:
        is_mort_active = is_active_status['is_mort_active'][0]
    else:
        is_mort_active = 1 * is_mort_active

    # rows with all-cause extract type should NEVER be cod active
    if extract_type_id == ALL_CAUSE_EXTRACT_ID:
        print("Marking data formatted for all cause mortality as is_active = 0")
        is_active = 0

    # is_mort_active will NEVER be 1 for non-VR (i.e. data type ids other than 9 or 10)
    if is_active_status['data_type_id'][0] not in [9, 10]:
        print("Marking non-VR data as is_mort_active = 0")
        is_mort_active = 0

    myconn = ezfuncs.get_connection('ADDRESS')

    query_tools.exec_query("""
        UPDATE cod.claude_nid_metadata
        SET is_active = {},
            is_mort_active = {}
        WHERE nid = {}
        AND extract_type_id = {}
        """.format(is_active, is_mort_active, nid, extract_type_id), cxn=myconn, close=True)
