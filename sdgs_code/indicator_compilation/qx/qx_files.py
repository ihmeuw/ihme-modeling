import pandas as pd
import sys
import os

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry


def process_qx_file(filepath):
    """Process a qx file"""
    assert 'qx_' in filepath, 'this wasnt a qx filepath'
    location_id = int(filepath.replace('qx_', '').replace('.csv', ''))
    df = pd.read_csv(os.path.join(dw.QX_DIR, filepath))
    df = df.query('sex_id==3 & year_id>=1990')
    assert 1 in set(df.age_group_id), \
        'need 5q0'
    assert 42 in set(df.age_group_id), \
        'need neonatal qx'
    df = df.ix[df.age_group_id.isin([1, 42])]
    # percentage
    df['metric_id'] = 2
    # probability of death with hiv and shocks included
    df['measure_id'] = 27
    # location
    df['location_id'] = location_id
    df = df[['location_id', 'year_id',
             'age_group_id', 'sex_id',
             'metric_id', 'measure_id',
             'draw', 'qx']]
    df = df.set_index(['location_id', 'year_id',
                       'age_group_id', 'sex_id',
                       'metric_id', 'measure_id', 'draw']).unstack()
    df.columns = df.columns.droplevel()
    df = df.reset_index()

    def replace_cols(x):
        if isinstance(x, int):
            return 'draw_' + str(x)
        else:
            return x

    df.columns = [replace_cols(col) for col in df.columns]
    return df


def write_output(df):
    """Write output of processing qx files to 5q0 and NN file"""
    five_q_z = df.query('age_group_id==1')
    nn_qx = df.query('age_group_id==42')
    out_dir = '{dd}/pod/{v}/'.format(dd=dw.INPUT_DATA_DIR,
                                     v=dw.QX_VERS)
    try:
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    except OSError:
        pass
    five_q_z.to_hdf(out_dir + '1.h5', format="table", key="data",
                    data_columns=['location_id', 'year_id'])
    nn_qx.to_hdf(out_dir + '42.h5', format="table", key="data",
                 data_columns=['location_id', 'year_id'])


def process_all_qx_files(test=True):
    """Sequentially write results from all qx files"""
    qx_files = os.listdir(dw.QX_DIR)
    qx_files = [q for q in qx_files if 'qx_' in q]
    if test:
        qx_files = qx_files[0:40]
    dfs = []
    print('assembling qx files')
    n = len(qx_files)
    i = 0
    for qx_file in qx_files:
        df = process_qx_file(qx_file)
        dfs.append(df)
        i = i + 1
        if i % (n / 10) == 0:
            print('{i}/{n} qx_files complete'.format(i=i, n=n))
    df = pd.concat(dfs, ignore_index=True)
    locsdf = qry.get_sdg_reporting_locations()
    df = df[df['location_id'].isin(locsdf['location_id'].values)]
    id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
               'measure_id', 'metric_id']
    df = df[id_cols + dw.DRAW_COLS]
    assert set(df.age_group_id) == {1, 42}, 'unexpected age group ids'
    assert set(df.year_id) == set(range(1990, 2017)), 'unexpected year ids'
    return df

if __name__ == "__main__":
    df = process_all_qx_files(test=False)
    write_output(df)
