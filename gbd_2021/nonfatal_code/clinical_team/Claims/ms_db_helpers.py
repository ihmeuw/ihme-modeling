
import pandas as pd
import numpy as np
import time
import glob
import re
import os
import datetime
import multiprocessing
import warnings


def clean_dir():
    # get a list of the old helper files
    files = glob.glob(
        FILEPATH
    if not files:
        warnings.warn(
            "There are no files to move, look into this if it's unexpected")
        return
    # todays date for archiving
    today = re.sub("\W", "_", str(datetime.datetime.now()))[0:10]
    # create archive dir
    adir = FILEPATH.format(
        today)
    if not os.path.exists(adir):
        os.makedirs(adir)
    for f in files:
        new_f = adir + "/" + os.path.basename(f)
        os.rename(f, new_f)
    return


def get_good_enrollees():
    files = glob.glob(
        FILEPATH)

    df = pd.concat([pd.read_hdf(f) for f in files])

    if 'index' in df.columns:
        df.drop('index', axis=1, inplace=True)

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='raise')

    df = df.sort_values('enrolid')

    return df


def get_master_vals(df, n):
    master_vals = pd.qcut(x=df.enrolid, q=n, precision=0).unique()
    return master_vals


def make_bins(master_vals, maxid):
    bins = pd.read_csv(
        FILEPATH)
    bins = bins['start'].tolist()
    bins[0] = 100
    len(bins)
    bins = bins + [maxid + 1]
    return bins


def remove_db_enrolid_error(df):
    df = df[df['enrolid'] != 0]
    return df


def create_groups(df, n, bins):
    df = remove_db_enrolid_error(df)

    df = df.assign(cuts=pd.cut(df.enrolid, bins, labels=np.arange(0, n, 1)))

    assert df.cuts.isnull().sum() == 0
    return df


def create_between_edges(n, master_vals):

    # make a dataframe
    left_list = []
    right_list = []
    for i in np.arange(0, n, 1):
        # add start and env values
        right_list.append(master_vals[i].right)
        left_list.append(master_vals[i].left)

    dat = pd.DataFrame({'start': left_list, 'end': right_list})

    dat.loc[0, 'start'] = 100

    dat.loc[dat['start'] > dat['start'].min(), 'start'] = dat.loc[dat['start']
                                                                  > dat['start'].min(), 'start'] + 1
    dat = dat[['start', 'end']]
    compare = dat['start'] - dat['end']

    assert not (compare > 0).any(), "There are starts larger than end"
    assert not set(dat['start']).intersection(set(dat['end'])),\
        "Some values overlap, this will not perform properly with MySQL"

    dat.to_csv(
       FILEPATH, index=False)
    return


def update_between_query(df):
    """
    realign the query statements with the group files
    """
    t = df.groupby('cuts').agg({'enrolid': 'min'}).reset_index()
    t.columns = ['cuts', 'start']
    t['end'] = df.groupby('cuts').agg({'enrolid': 'max'})

    for c in t.cuts.unique():

        assert df[df.cuts == c].enrolid.min() == t[t.cuts ==
                                                   c].start.min(), "start is off"
        assert df[df.cuts == c].enrolid.max() == t[t.cuts ==
                                                   c].end.min(), "end is off"
    t.drop('cuts', axis=1).to_csv(
        FILEPATH, index=False)
    return


def make_cut_list(df, n):
    start = time.time()
    df_list = [df[df.cuts == i] for i in np.arange(0, n, 1)]
    end = (time.time() - start) / 60
    print("list made in {} minutes".format(end))
    return df_list


def csv_writer(df):
    assert type(df) == pd.DataFrame
    print(df.shape[0])
    i = int(df.cuts.unique().astype(int))
    df.drop('cuts', axis=1, inplace=True)
    wpath = FILEPATH.format(
        i)
    df.to_csv(wpath, index=False)
    print("done with group {}".format(i))
    return


if __name__ == '__main__':
    n_pools = 4

    clean_dir()

    n = 350

    df = get_good_enrollees()

    master_vals = get_master_vals(df, n)

    create_between_edges(n, master_vals)

    bins = make_bins(master_vals, df.enrolid.max())
    df = create_groups(df, n, bins)

    update_between_query(df)

    df_list = make_cut_list(df, n)

    p = multiprocessing.Pool(n_pools)
    p.map(csv_writer, df_list)
