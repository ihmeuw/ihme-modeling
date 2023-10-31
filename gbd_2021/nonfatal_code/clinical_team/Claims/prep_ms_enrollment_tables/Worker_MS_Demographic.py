import pandas as pd
import numpy as np
import mysql.connector
import time
import tables
from datetime import datetime
import os.path
import warnings
import sys
import re

proc_year = sys.argv[1]

dcon = pd.read_csv(FILEPATH)

cnx = mysql.connector.connect(CREDENTIALS)
cursor = cnx.cursor()


def write(df, full_year, year):
    '''
    Write data to file

    Args:
        df_list: (list) a list of data frames
        full_year: (bool) a flag used to determine which dir to write to
        year: (str) corresponding year of the df_list

    '''

    base = FILEPATH

    if full_year:
        extension = FILEPATH.format(year)
    else:
        extension = FILEPATH.format(year)

    if not full_year:  # do a groupby on the sample size info
        df = df.groupby(['age_start', 'age_end', 'sex', 'egeoloc', 'year']).agg(
            {'sample_size': 'sum'}).reset_index()

    file = base + extension
    if os.path.exists(file):
        today = re.sub("\W", "_", str(datetime.now()))[0:19]
        # create archive dir
        adir = os.path.dirname(file) + FILEPATH.format(today)
        if not os.path.exists(adir):
            os.makedirs(adir)
        new_f = adir + "/" + os.path.basename(file)
        os.rename(file, new_f)

    if full_year:
        df = df.drop_duplicates()
        assert df.enrolid.unique().size == df.shape[0],\
            ("The number of unique enrollees should match the number of rows. "
             "look into this")

        df.to_hdf(file, key='df', format='fixed', mode='w')
        print(f"Saved to {file}.")
    else:
        if df[['sex', 'age_start', 'age_end',
               'egeoloc', 'year']].drop_duplicates().shape[0] != df.shape[0]:
            warnings.warn("Why is this assertion bad, look into it")
        df.to_stata(file, write_index=False)
        print(f"Saved to {file}.")
    return


def queryIds(table, sval, endval):
    '''
    retrieve the data we'll use to create sample size and unique enrolids
    This is a big table of grouped enrolids, min_age, sex and egeoloc
    we're processing by year so that's the last demographic variable

    Args:
        table: (str) corresponding MS table to be queried

    Returns:
        A MySQL connector cursor object
    '''
    # get the inpatient table to search for deaths
    stype = 'fact'
    if 'mdcr' in table:
        stype = 'fact_mdcr'
    yr = table[-4:]
    inp_table = "{}_inpatient_admission_{}".format(stype, yr)

    if '2013' in table:
        cursor = cnx.cursor()
        id_query = ('''QUERY
            '''
            % (table, sval, endval, table, sval, endval, table, sval,
               endval, table, inp_table, sval, endval)
        )

    else:
        cursor = cnx.cursor()
        id_query = (
            '''QUERY
            '''
            % (table, sval, endval, table, sval, endval, table, sval,
               endval, table, inp_table, sval, endval)
        )
    cursor.execute(id_query)
    return cursor


def create_pd_df(rows):
    """
    Turn the cursor object into a pandas DataFrame
    """
    df = pd.DataFrame(rows, columns=['enrolid', 'age', 'sex', 'egeoloc'])
    return df


def create_enrolid_table(df, year):
    """
    Take a chunk of records from the cursor object and turn them into the
    enrollment table
    """
    df = df[['enrolid']].drop_duplicates()
    df['enrolid'] = df['enrolid'].astype(int)
    df['year'] = int(year)
    return df


def create_sample_size(df, year):
    """
    Take a chunk of records from the cursor object and turn them into the
    sample size table
    """
    df.drop_duplicates('enrolid', keep='first', inplace=True)

    df['year'] = int(year)
    df['sample_size'] = 1
    df = df.groupby(['age', 'sex', 'egeoloc', 'year']).agg(
        {'sample_size': 'sum'}).reset_index()

    # set dtypes
    df['sex'] = df['sex'].astype(str)
    df[['age', 'egeoloc']] = df[['age', 'egeoloc']].astype(int)
    df['age_start'] = df['age']
    df['age_end'] = df['age']
    df.drop('age', axis=1, inplace=True)

    return df


def make_between_vals(quantity):
    """
    performing a groupby on a table with over 500 million rows is very RAM
    intensive in MySQL so to get around this limit we'll split our enrollees
    up into `quantity` groups and perform the groupby using between statements
    """

    enrolid_breaks = pd.read_csv(
        FILEPATH)
    enrolid_breaks = enrolid_breaks.sample(quantity)
    enrolid_breaks = enrolid_breaks.sort_values('start')
    enrolid_breaks.reset_index(drop=True, inplace=True)

    enrolid_breaks.loc[enrolid_breaks['start'] ==
                       enrolid_breaks['start'].min(), 'start'] = 0
    enrolid_breaks.drop('end', axis=1, inplace=True)

    # create the end column which aligns with the start column
    enrolid_breaks['end'] = enrolid_breaks['start'].shift(-1) - 1

    # manually set the largest enrolid value
    enrolid_breaks.loc[enrolid_breaks.shape[0] - 1, 'end'] = 99033001974399

    # test a little
    assert (enrolid_breaks.start.shift(-1) > enrolid_breaks.end).sum() ==\
        quantity - 1, "The enrolid break table was not made properly"
    return enrolid_breaks


# Create MS tables
tables = [TABLES]

chunk = 1000000

all_start = time.time()

quantity = 50
enrolid_breaks = make_between_vals(quantity)

list_demo_dfs = []
list_id_dfs = []

df_list = []
for table in tables:
    print("\n\nBeginning to process {}...".format(table))
    table_year = table.split('_')[-1]

    start = time.time()

    counter = 0
    for i in np.arange(0, enrolid_breaks.shape[0], 1):
        cid_start = time.time()
        cursor_id = queryIds(table,
                             sval=enrolid_breaks.loc[i, 'start'],
                             endval=enrolid_breaks.loc[i, 'end'])
        print("Finished running phase {} of {} of the queryIds func in {} min"
              .format(i + 1, quantity, round((time.time() - cid_start) / 60, 1)
                      ))

        while True:
            rows = cursor_id.fetchmany(size=chunk)
            if not rows:
                break
            # convert the records into pandas dataframe
            df = create_pd_df(rows)
            print("This chunk contains a df with {} rows".format(df.shape[0]))
            df_list.append(df)

    print("\n\nQuerying the dB and processing data finished...")
    total_time = (time.time() - start) / 60
    print("Table: {} \tTotal Time: {}".format(table, total_time))


df = pd.concat(df_list, ignore_index=True)
file = FILEPATH.format(proc_year)
df.to_hdf(file, key='df', format='fixed')
print(f"Saved to {file}")


et = create_enrolid_table(df.copy(), table_year)
samp = create_sample_size(df.copy(), table_year)

assert et.shape[0] == samp.sample_size.sum(),\
    "what happened? sample size is {} and unique enrolid size is {}"\
    .format(samp.sample_size.sum(), et.shape[0])

write(et, full_year=True, year=table_year)
write(samp, full_year=False, year=table_year)
print("\n\nEverything finished in {} min".format((time.time() - all_start)
                                                 / 60))

cursor.close()
cnx.close()
