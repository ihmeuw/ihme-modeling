import os
import numpy as np
import pandas as pd

from cod_prep.downloaders import (
    add_code_metadata,
    get_nid_metadata,
    get_nidlocyear_map,
    add_location_metadata
)
from cod_prep.utils import (
    report_if_merge_fail, cod_timestamp, add_tbl_metadata, report_duplicates
)
from claude_io import (
    write_phase_output, write_to_claude_nid_table
)
from db_tools import ezfuncs
from configurator import Configurator

CONF = Configurator('standard')

VA_2010_2013 = [93693, 106964, 107291, 106964, 106964]
VA_2015 = [156884, 156912]
MATERNAL = [208315, 229822, 243597, 245181, 106347]
SPLIT_ISO_NIDS = \
    [284465, 108858, 311356] + VA_2010_2013 + VA_2015 + MATERNAL + [155336, 93752, 134110, 104272, 91739]

ALL_CAUSE_EXTRACT_ID = 167


def group_six_minor_territories(df, sum_cols):

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


def induce_extraction_type(df, conn_def='ADDRESS'):

    min_data_location_id = df.location_id.min()
    max_data_location_id = df.location_id.max()

    location_types = ezfuncs.query("""
        SELECT
            location_id, location_type_id,
            location_level AS level
        FROM ADDRESS
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
        FROM ADDRESS
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
    ddir = "FILEPATH"
    valid_sources = os.listdir(ddir)
    valid_sources.remove("FILEPATH")
    # don't upload sources with underscores in the front
    valid_sources = [
        vs.strip("_") for vs in valid_sources if "Incoming_tmp" not in vs
    ]
    bad_ones = set(source) - set(valid_sources)
    if len(bad_ones) > 0:
        raise AssertionError(
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
        FROM ADDRESS
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


def map_site_id(df, site_col='site', conn_def='ADDRESS'):

    df = df.rename(columns={site_col: 'site_name'})
    df['site_name'] = df['site_name'].fillna("")

    # get site names in db
    unique_sites = df[['site_name']].drop_duplicates()
    db_sites_q = """SELECT site_name, site_id FROM ADDRESS"""
    db_sites = ezfuncs.query(
        db_sites_q,
        conn_def=conn_def
    )
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
            "No site_id for sites {}, uploading."
            "..".format(upload_sites.site_name.unique())
        )
        # if any, upload them
        insert_names('site', upload_sites, conn_def=conn_def)

        # refresh db_sites
        db_sites = ezfuncs.query(
            "SELECT site_name, site_id FROM ADDRESS",
            conn_def=conn_def
        )
        unique_sites = unique_sites.drop('site_id', axis=1)
        unique_sites = unique_sites.merge(db_sites, how='left')
        report_if_merge_fail(unique_sites, 'site_id', 'site_name')

    df = df.merge(unique_sites, on='site_name', how='left')
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

    if (df['nid'].isin(SPLIT_ISO_NIDS).any()):

        nid_loc_df = df.loc[
            df['nid'].isin(SPLIT_ISO_NIDS),
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
            df.loc[df['nid'].isin(SPLIT_ISO_NIDS)],
            'extract_type_id_new', ['nid', 'location_id'])
        df.loc[
            df['nid'].isin(SPLIT_ISO_NIDS),
            'extract_type_id'] = df['extract_type_id_new']
        df = df.drop(
            ['extract_type_id_new', 'iso3', 'ihme_loc_id', 'extract_type'],
            axis=1
        )

    if source == "VA_lit_GBD_2010_2013":

        code_system_to_extract_type_id = {
            244: 350,
            273: 347,
            294: 353,
            296: 356,
        }
        df.loc[
            df['nid'].isin([93570, 93664]), 'extract_type_id'
        ] = df['code_system_id'].map(code_system_to_extract_type_id)

    # police data fixes
    elif source == "Various_RTI":
        code_system_to_extract_type_id = {
            237: 719,
            241: 722,
        }
        df.loc[
            df['nid'].isin([93599]), 'extract_type_id'
        ] = df['code_system_id'].map(code_system_to_extract_type_id)

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

    elif source == "Combined_Census":
        df.loc[df['nid'] == 7942, 'extract_type_id'] = 479
    elif source == "Maternal_Census":
        df.loc[df['nid'] == 7942, 'extract_type_id'] = 482
        df.loc[df['nid'] == 10319, 'extract_type_id'] = 485
    elif source == "Other_Maternal":
        df.loc[df['nid'] == 10319, 'extract_type_id'] = 488
    elif source == "Pakistan_maternal_DHS_2006":
        df["extract_type_id"] = 539
    elif source == "Pakistan_child_DHS_2006":
        df["extract_type_id"] = 536

    if (df['data_type_id'].isin([5, 6, 7]).any()):
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
        INSERT INTO ADDRESS
            (extract_type)
            VALUES
            ("{}")
    """.format(extract_type))
    conn.close()


def pull_extract_type_id(extract_type, conn_def='ADDRESS'):
    """Query cod database for extract_type_id."""
    extract_type_id = ezfuncs.query("""
        SELECT extract_type_id
        FROM ADDRESS
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
            INSERT INTO ADDRESS
                (source_name)
                VALUES
                ("{}")
        """.format(source))
        conn.close()
    else:
        print("Source already exists")


def pull_source_id(source, conn_def='ADDRESS'):
    source_id = ezfuncs.query("""
        SELECT DISTINCT source_id
        FROM ADDRESS
        WHERE source_name = "{}"
    """.format(source), conn_def=conn_def)
    if len(source_id) == 1:
        return source_id.iloc[0, 0]
    elif len(source_id) == 0:
        return None
    else:
        raise AssertionError("Got multiple rows: {}".format(source_id))


def check_subnational_locations(df):

    subnational_iso3s = CONF.get_id('subnational_modeled_iso3s')

    if 'UKR' in subnational_iso3s:
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
        try:
            assert check_loc not in subnational_iso3s
        except:
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


def check_age_groups(df):
    """Check for some things we know to be true for age groups.

    Things to look for:
        - age group years start and end do not overlap
        - for VR, the final age group should be 80+, 85+, etc.
    """
    raise NotImplementedError


def finalize_formatting(df, source, write=False, code_system_id=None,
                        extract_type=None, conn_def='ADDRESS', is_active=True):

    NID_META_COLS = [
        'nid', 'parent_nid', 'extract_type_id', 'source', 'data_type_id',
        'code_system_id', 'is_active'
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
    if len(missing_cols) > 0:
        raise AssertionError(
            """These columns are needed for formatting but not found in df:

            {}
            """.format(missing_cols)
        )

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
            cs_df = add_code_metadata(
                cs_df, ['code_id'],
                code_system_id=code_system_id, merge_col='value',
                force_rerun=True, cache_dir='standard'
            )
            print(cs_df.loc[cs_df['code_id'].isnull()].value.unique())
            report_if_merge_fail(cs_df, ['code_id'], ['value'])
            cs_df = cs_df.drop('value', axis=1)
            cs_dfs.append(cs_df)
        df = pd.concat(cs_dfs, ignore_index=True)
    else:
        # ADD TEST TO CHECK THAT EVERY CODE_ID IS IN THE ENGINE ROOM AND IN THE
        # CODE SYSTEM
        all_codes_q = """
            SELECT code_id
            FROM ADDRESS
            WHERE code_system_id IN ({})
        """.format(",".join([str(c) for c in code_system_ids]))
        all_codes = ezfuncs.query(all_codes_q, conn_def='engine')
        bad_codes = set(df.code_id) - set(all_codes.code_id)
        if len(bad_codes) > 0:
            print(
                "Found these code ids in data that can't exist in code "
                "systems {}: {}".format(code_system_ids, bad_codes)
            )

    # MAP SITE ID
    df = map_site_id(df, conn_def=conn_def)
    # MAP EXTRACT TYPE ID
    df = map_extract_type_id(df, source, extract_type, conn_def=conn_def)

    # CHANGE SIX MINOR TERRITORIES TO AGGREGATE UNION LOCATIONS
    df = group_six_minor_territories(
        df, sum_cols=VALUE_COLS
    )

    df = df.loc[~((df['nid'] == 279644) & (df['year_id'] == 2011))]
    df = df.loc[~(df['nid'].isin([24143, 107307]))]

    # ENSURE NO NEGATIVES
    for val_col in VALUE_COLS:
        assert (df[val_col] >= 0).all(), \
            "there are negative values in {}".format(val_col)

    input_df = df[FINAL_FORMATED_COLS].copy()
    assert not input_df.isnull().values.any(), "null values in df"
    dupped = input_df[input_df.duplicated()]
    if len(dupped) > 0:
        raise AssertionError("duplicate values in df: \n{}".format(dupped))

    # GROUP IF NECESSARY
    if input_df[FORMATTED_ID_COLS].duplicated().any():
        input_df = input_df.groupby(
            FORMATTED_ID_COLS, as_index=False)[VALUE_COLS].sum()


    # MAKE NID METADATA TABLE
    if 'parent_nid' not in df.columns:
        df['parent_nid'] = np.nan

    df['is_active'] = 1 * is_active

    # CHECK SUBNATIONAL LOCATIONS
    # alters is_active if needed
    df = check_subnational_locations(df)

    nid_meta_df = df[NID_META_COLS].drop_duplicates()
    nid_meta_df['last_updated_timestamp'] = format_timestamp

    # MAKE NID LOCATION YEAR TABLE
    nid_locyears = df[NID_LOCATION_YEAR_COLS].drop_duplicates()
    nid_locyears['last_updated_timestamp'] = format_timestamp
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

    return locals()
