
import pandas as pd
import sys
import time
import datetime
import re
import warnings
from random import randint
import mysql.connector as mariadb

from clinical_info.Claims.query_ms_db import marketscan_estimate_indv


def connect_and_query(year, group_num, chunksize=400000):
    """
    Params
        year: (int)
            year of the tables to query across. We use four tables for each
            year
        group_num: (int)
            Identifies which group of unique enrolids to use
        chunksize: (int)
            number of records to return at once.

    """
    assert type(
        year) != float, "year variable cannot be a float, the query will fail"

    between_path = FILEPATH
    betweens = pd.read_csv(between_path)
    # this freezes if the group_num is not valid instead of erroring out as expected
    if group_num not in betweens.index:
        assert False, "The group number {} is invalid. Please check the betweens index at {}".format(
            group_num, between_path)
    start = betweens.loc[group_num, 'start']
    end = betweens.loc[group_num, 'end']

    func_start = time.time()
    # create a connection to the db
    mariadb_connection = mariadb.connect(CREDENTIALS)

    inp_cols = "admdate, enrolid, age, sex, egeoloc, year, dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10, dx11, dx12, dx13, dx14, dx15"
    otp_cols = "svcdate, stdplac, enrolid, age, sex, egeoloc, year, dx1, dx2, dx3, dx4"

    # 2015/16 has ICD 10 data identified by the dxver col below
    if year in [2015, 2016, 2017]:
        inp_cols = inp_cols + ", dxver"
        otp_cols = otp_cols + ", dxver"

    df_list = []
    tables = [TABLES]
    failures = []
    failures2 = []
    for table in tables:
        print("starting on table {} {}".format(table, year))
        if 'inpatient' in table:
            cols = inp_cols
            otp = 0
            date_col = 'admdate'
        else:
            cols = otp_cols
            otp = 1
            date_col = 'svcdate'

        # prep the query statement
        sql_query = "QUERY".format(
            cols, table, year, start, end)
        # query the db and return a dataframe
        for tmp in pd.read_sql(sql_query, con=mariadb_connection, chunksize=chunksize):
            tmp['is_otp'] = otp
            tmp.rename(columns={"{}".format(date_col): 'date'}, inplace=True)

            if tmp.shape[0] > 0:
                if 'dxver' not in tmp.columns and year in [2000] + list(range(2010, 2015, 1)):
                    tmp['dxver'] = 9
                else:
                    assert year in [
                        2015, 2016, 2017], "The year {} isn't acceptable".format(year)
                for col in ['egeoloc', 'sex', 'date', 'age']:
                    if tmp[col].unique().size < 2:
                        msg = "Only {s} unique {c} values found on table {t}_{y}\n{v}".\
                            format(c=col, s=tmp[col].unique().size,
                                   t=table, y=year, v=" ".join(tmp[col].unique().astype(str).tolist()))
                        failures2.append(msg)
                    if col in ['egeoloc', 'sex'] and tmp[col].min() == 0:
                        failures.append("might modify to not remove this in the future, "
                                        "but zero isn't an acceptable value for sex or "
                                        "location and it probably means the upload has an issue")

            df_list.append(tmp)

    mariadb_connection.close()

    if not df_list:
        return "move on"

    if failures2:
        warnings.warn("\n".join(failures2))
    if failures:
        return "\n".join(failures)

    df = pd.concat(df_list, ignore_index=True, sort=False)

    for col in ['year', 'egeoloc', 'stdplac']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])

    df.loc[df['dxver'].isin([0, "0"]), 'dxver'] = 2
    df.loc[df['dxver'].isin([9, "9"]), 'dxver'] = 1

    df = df[df['dxver'].isin([1, 2])]

    df.rename(columns={'dxver': 'code_system_id'}, inplace=True)

    df['loop_year'] = year
    print("Year {} ran in {} min and is shape {}".format(
        year, (time.time() - func_start) / 60, df.shape))
    return df


def join_good_enrolids(df, group_num):
    """
    Params:
        df: (pd.DataFrame)
            A df of MS claims data pulled from the database
        group_num: (int)
            Identifies which group of unique enrolids to use
    """

    goods = pd.read_csv(
        FILEPATH.format(group_num))
    for col in goods:
        goods[col] = pd.to_numeric(goods[col], errors='raise')

    # inner join goods and the df
    pre = df.shape[0]
    df = df.merge(goods, how='inner', on=['year', 'enrolid'])
    assert pre >= df.shape[0], "The merge changed the df shape from {} to {} perhaps enrolids are duped".format(
        pre, df.shape[0])

    return df


def set_min_age(df):
    """
    people can get 1 year older within the same calendar year. Modify all ages to min
    age within that year
    """
    # create the min_age df
    min_age = df.groupby(['enrolid', 'year']).agg(
        {'age': 'min'}).reset_index().rename(columns={"age": "min_age"})

    # merge on min age
    pre = df.shape[0]
    df = df.merge(min_age, how='left', on=['enrolid', 'year'])
    assert pre == df.shape[0], "The merge changed the df shape from {} to {} perhaps enrolids are duped".format(
        pre, df.shape[0])

    df['age_diff'] = df['min_age'] - df['age']

    pre = df.shape[0]
    df = df[df['age_diff'] <= 1]
    print(
        "{} rows were dropped b/c age diff was too large".format(pre - df.shape[0]))

    if df.shape[0] == 0:
        return

    if abs(df.age_diff.max()) > 1:
        print("review age diffs {}".format(df.age_diff.describe()))

    # prep cols and return
    df.drop(['age', 'age_diff'], axis=1, inplace=True)
    df.rename(columns={'min_age': 'age'}, inplace=True)
    return df


def query_by_year(attempts, group_num, years):

    final_list = []
    # loop over years in df
    for year in years:
        year = int(year)

        break_counter = 0
        if attempts > 0:
            while break_counter < attempts + 1:
                break_counter += 1
                try:
                    tmp = connect_and_query(year, group_num)
                except Exception as e:
                    print(e)
                    time.sleep(randint(30, 1000))
                if 'tmp' in locals():
                    break
                if not 'tmp' in locals():
                    print(
                        "The db query failed {} times. Killing this job".format(attempts))
                    today = re.sub("\W", "_", str(
                        datetime.datetime.now()))[0:10]
                    e = str(e)
                    fail = pd.DataFrame(
                        {'fail_date': today, 'group_number': group_num, 'error': e}, index=[0])
                    fail.to_hdf(FILEPATH,
                                key='df', mode='a', format='table', append=True)
                    warnings.warn(
                        "Job {} failed with error {}".format(group_num, e))
                    assert False, "The dB query failed"
            print(
                "finished with {}, moving on to next year outside of while loop".format(year))
        if attempts == 0:
            tmp = connect_and_query(year, group_num)

        if type(tmp) == str:
            if tmp == "move on":
                print("skipping year {} and moving on".format(year))
                del tmp
                continue
            else:
                assert False, tmp
        pre_join = tmp.shape[0]
        tmp = join_good_enrolids(tmp, group_num)
        tmp = set_min_age(tmp)

        if not isinstance(tmp, pd.DataFrame):
            continue
        print("{} rows were dropped after inner joining good enrolids. appending to final_list...".
              format(pre_join - tmp.shape[0]))
        final_list.append(tmp)
        del tmp
        time.sleep(1)

    if len(final_list) == 0:
        df = pd.DataFrame(columns=['date', 'enrolid', 'age', 'sex', 'egeoloc', 'year', 'dx1', 'dx2', 'dx3', 'dx4', 'dx5', 'dx6',
                                   'dx7', 'dx8', 'dx9', 'dx10', 'dx11', 'dx12', 'dx13', 'dx14', 'dx15', 'code_system_id', 'is_otp', 'stdplac', 'loop_year'])
        return df

    df = pd.concat(final_list, ignore_index=True, sort=False)
    return df


def get_good_enrolids(fpath):

    df = pd.read_hdf(fpath)
    return df


def finished_querying(run_id, group_num):
    out_dir =FILEPATH.format(
        run_id)
    out_dir = out_dir.replace("\r", "")
    dat = pd.DataFrame({"status": "All done getting dB data!"}, index=[0])

    dat.to_csv(FILEPATH.format(out_dir, group_num), index=False)

    return


def check_enrolids_by_year(run_id, group_num, years):

    read_path = FILEPATH.format(
        group_num)
    tmp = pd.read_csv(read_path)
    tmp = tmp[tmp['year'].isin(years)]
    rows = len(tmp)
    if rows == 0:
        # if there aren't any rows for these years then write dummy files
        write_path = FILEPATH.format(
            r=run_id)
        pd.DataFrame().to_hdf("{wp}bundle_group_{g}.H5".format(
            wp=write_path, g=group_num), key='df')
        pd.DataFrame().to_hdf("{wp}icg_group_{g}.H5".format(
            wp=write_path, g=group_num), key='df')
        # write the query helper file so the submit script can keep going
        finished_querying(run_id=run_id, group_num=group_num)
        # end the program
        sys.exit(0)
    else:
        pass
    return


if __name__ == '__main__':

    main_start = time.time()

    group_num = sys.argv[1]
    run_id = sys.argv[2]

    group_num = int(re.sub("[^0-9]", "", group_num))
    run_id = run_id.replace("\r", "")

    years = [2000, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017]

    check_enrolids_by_year(run_id=run_id, group_num=group_num, years=years)

    df = query_by_year(attempts=3, group_num=group_num, years=years)

    df['code_system_id'] = pd.to_numeric(df['code_system_id'])

    assert (df['year'] == df['loop_year']).all(), "years are wrong"
    df.drop('loop_year', axis=1, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    print("{} rows now have missing dates, these will be dropped".format(
        df['date'].isnull().sum()))
    df = df[df['date'].notnull()]

    print("Querying MS dB finished in {} minutes. Moving on to processing marketscan".format(
        round((time.time() - main_start) / 60, 2)))

    finished_querying(run_id=run_id, group_num=group_num)

    marketscan_estimate_indv.process_marketscan(df=df.copy(), group=str(
        group_num), clinical_age_group_set_id=1,
        prod=True, run_id=run_id, cause_type='bundle', break_if_not_contig=False)

    print("All finished! This group took {} minutes".format(
        round((time.time() - main_start) / 60, 2)))
