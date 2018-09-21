import pandas as pd
import numpy as np
import sys
import os

def pandas_read_infer_filetype(filepath, limits =None):
    if filepath[-4:] == '.csv':
        df = pd.read_csv(filepath)
        if limits is not None:
            for limit in limits:
                df = df.query(limit)

    elif filepath[-3:] == '.h5' or filepath[-4:] == '.hdf' or filepath[-5:] == '.hdf5':

        df = pd.read_hdf(filepath, key='data', where=limits)

    return df

def zero_to_one_ax_func(df_row, deathrate_col):

    if df_row['age_group_years_start'] == 0:

        if df_row[deathrate_col] < 0.107:
            if df_row['sex_id'] == 1:
                return 0.045 + 2.684*df_row[deathrate_col]
            elif df_row['sex_id'] == 2:
                return 0.053 + 2.800*df_row[deathrate_col]
            elif df_row['sex_id'] == 3:
                return 0.049 + 2.742*df_row[deathrate_col]
        else:
            if df_row['sex_id'] == 1:
                return 0.330
            elif df_row['sex_id'] == 2:
                return 0.350
            elif df_row['sex_id'] == 3:
                return 0.340

    elif df_row['age_group_years_start'] == 1:

        if df_row[deathrate_col] < 0.107:
            if df_row['sex_id'] == 1:
                return 1.651 - 2.816*df_row['prev_age_deathrate']
            elif df_row['sex_id'] == 2:
                return 1.522 - 1.518*df_row['prev_age_deathrate']
            elif df_row['sex_id'] == 3:
                return 1.5865 - 2.167*df_row['prev_age_deathrate']
        else:
            if df_row['sex_id'] == 1:
                return 1.352
            elif df_row['sex_id'] == 2:
                return 1.361
            elif df_row['sex_id'] == 3:
                return 1.3565
    else:
        return df_row['ax']

def process_pop(pop_path, pop_prefix, external_cols, truncate=False,
                index_cols = ['location_id', 'year_id','age_group_id','sex_id']):
    """ Read in population given a path, a prefix to identify the population data, and a list of columns that this pop data will be used to match on

    :param pop_path: path to population data, csv or hdf
    :param pop_prefix: string prefix that identifies which column in the dataset are actually population
    :param external_cols: List of strings with the names of columns we're going to use pop with. Helps return the right number of columns with proper names
    :param truncate: If True and there are more pop cols than external cols, only the first population columns will be returned.
    :param index_cols: Columns with demographic info
    """


    # Get the data
    pop = pandas_read_infer_filetype(pop_path)

    # Identify columns with pop data
    pop_cols = [col for col in pop.columns if pop_prefix in col]

    # We don't want to actually duplicate data we dont have
    unique_pop_names = pop_cols

    # A few situations:
    # 1. We have 1 pop column to match with N external columns,
    #    so we return a list with the same column repeated N times (just the name, not the data)
    # 2. We have N pop columns to match with N external columns, we do nothing spexical
    # 3. We have P pop columns to match with N external columns:
    #    a. If P > N
    #       i. if truncate is true, return the first N pop columns
    #       ii. if truncate is false, raise an error
    #    b. If P < N, raise an error


    # Double check the pop common prefix
    pop_prefix = os.path.commonprefix(pop_cols)
    external_prefix = os.path.commonprefix(external_cols)

    if pop_prefix == external_prefix:

        # If they match, rename the pop columns
        rename_dict = {pop_col:'_'+pop_col for pop_col in pop_cols}
        pop = pop.rename(columns=rename_dict)
        pop_cols = ['_'+pop_col for pop_col in pop_cols]
        unique_pop_names = pop_cols


    pop = pop[index_cols+unique_pop_names]

    return pop, pop_cols

def calculate_ex(input_frame, deathrate_columns='mx', pop_columns = None, debug_location=0, debug_sex=0,
                 pop_path =None, pop_prefix=None,
                 external_ax=None, ax_prefix='', drop_deathrate=True, deathrate_prefix='nothing',
                 save_path=None):
    """ Calculate life expectancy from mortality rate. Can accept either a single column or a list of columns (useful
    for draws). If the dataframe has a column called 'pop', it will be used, so make sure the values are accurate.

    :param input_frame: Pandas dataframe. Must have either 'age_group_years_start' column corresponding to GBD ages: 0,0.01,0.1,1,5,...80
                      or an 'age_group_id' column. Must have either iso3 or location_id column. Can handle any number
                      of years
    :param deathrate_columns: A list strings or a single string containing columns with mortality rate.
    :param pop_columns: A list of strings identifying the population columns in the input frame
    :param external_ax: Optional, a sting containing a filepath with ax values
    :param ax_prefix: a prefix present in each ax column in the external_ax data
    :param deathrate_prefix: set this to the prefix that identifies deathrate columns if you want it removed in the ex and ax columns
    :param save_path: if provided, life expectancy will be saved at this path as a csv(csv for easy aggregation in bash)
    :return: A pandas dataframe containing a column with life expectancy values for every input deathrate_column.
             Life expectancy columns are named ex_
    """

    if not isinstance(deathrate_columns, list):
        deathrate_columns = [deathrate_columns]


    print external_ax
    if external_ax is not None:
        print 'here'
        ax_data = pandas_read_infer_filetype(external_ax)
        ax_columns = [col for col in ax_data.columns if ax_prefix in col]
        print ax_columns
    else:
        ax_columns = False

    if pop_columns is None:
        pop, pop_columns = process_pop(pop_path, pop_prefix, deathrate_columns)
        input_frame = input_frame.merge(pop)

    if not isinstance(pop_columns, list):
        pop_columns = [pop_columns]

    for col_num, (deathrate_column, pop_column) in enumerate(zip(deathrate_columns,pop_columns)):

        dataframe = input_frame.copy()
        if col_num % 10 == 0:
            print col_num
            print deathrate_column
            print pop_column
        if col_num == 0:
            print dataframe[pop_column]
            print dataframe[deathrate_column]

        # Get death counts
        try:
            dataframe.loc[:, 'deaths'] = dataframe[pop_column] * dataframe[deathrate_column]
        except:
            print dataframe[pop_column]
            print dataframe[deathrate_column]
            print dataframe
            raise

        # Collapse ages < 1
        less_than_1 = dataframe.query('age_group_years_start < 1 and age_group_years_end <= 1')
        dataframe = dataframe.query('age_group_years_start >= 1')

        less_than_1 = less_than_1.groupby(['location_id', 'year_id', 'sex_id']).sum()

        less_than_1[[deathrate_column]] = less_than_1['deaths'] / less_than_1[pop_column]


        less_than_1 = less_than_1.reset_index()
        
        less_than_1.loc[:,'age_group_years_start'] = 0
        less_than_1.loc[:,'age_group_years_end'] = 1

        less_than_1.loc[:, 'age_group_id'] = np.NaN

        # Dataframe now has a single age below 1
        dataframe = dataframe.append(less_than_1, ignore_index=True)

        if external_ax is not None:
            dataframe = dataframe.merge(ax_data, on=['location_id', 'year_id','age_group_years_start','sex_id'])

        ages = [0, 1]+[5*x for x in range(1, 19+1)]

        # ax = mean age of death
 ax_map = pd.DataFrame({'age_group_years_start': ages, 'ax': [0] + [0] +[2.5]*19})

        # n = person years in age group
        n_map = pd.DataFrame({'age_group_years_start': ages, 'n': [1.000000]+[4.0]+[5.0]*19})

        dataframe = dataframe.merge(ax_map)
        dataframe = dataframe.merge(n_map)


        dataframe = dataframe.sort(['location_id', 'year_id','sex_id','age_group_years_start'])
        dataframe['prev_age_deathrate'] = dataframe[deathrate_column].shift(1)

        if not ax_columns:
            dataframe['ax'] = dataframe.apply(lambda x: zero_to_one_ax_func(x, deathrate_column), axis=1)
        else:
            dataframe['ax'] = dataframe[ax_columns[col_num]]

        # Add an index to reference ages
        dataframe = dataframe.set_index(['location_id', 'year_id', 'sex_id', 'age_group_years_start']).sortlevel()

        # Iterate over ax to compute
        tolerance = 0.0001
        current_delta = 1.0
        iteration = 0

        if not ax_columns:
            while current_delta >= tolerance:
                if iteration % 10 == 0:
                    print "iter {i:d}: {cd:f}".format(i=iteration, cd=current_delta)

                iteration += 1
                dataframe.loc[:, 'qx'] = dataframe['n']*dataframe[deathrate_column]/(1+(dataframe['n']-dataframe['ax'])
                                                                                        *dataframe[deathrate_column])
                # px = survival probability
                dataframe.loc[:, 'px'] = 1-dataframe['qx']

                # lx = number alive, initializing entire column here
                dataframe.loc[:, 'lx'] = 0

                # Initialize first age to arbitrary starting pop
                idx = pd.IndexSlice
                dataframe.loc[idx[:, :,  :, 0], idx['lx']] = 100000

                # Update all ages after the first one
                for a, age in enumerate(ages[1:]):

                    prev_age = ages[a]
                    # Need to add .values because the result will have a different age index than where it is being stored
                    dataframe.loc[idx[:, :,  :, age], idx['lx']] = dataframe.loc[idx[:, :,  :, prev_age], idx['lx']].values * \
                                                                dataframe.loc[idx[:, :,  :, prev_age], idx['px']].values

                # dx = crude death rate
                dataframe['dx'] = dataframe['lx']*dataframe['qx']

                # Get dx for ages before and after
                dataframe['next_dx'] = dataframe['dx'].shift(-1).fillna(0)
                dataframe['prev_dx'] = dataframe['dx'].shift(1).fillna(0)

                # Calculate new ax from that one fancy equation in the mortality book
                dataframe['new_ax'] = (-dataframe['n']/24*dataframe['prev_dx'] +
                        dataframe['n']/2*dataframe['dx'] +
                        dataframe['n']/24*dataframe['next_dx']) / dataframe['dx']

                # These ages dont change
                dataframe.loc[idx[:, :,  :, [0,1,5,ages[-2],ages[-1]]], 'new_ax'] = dataframe.loc[idx[:, :,  :, [0,1,5,ages[-2],ages[-1]]], 'ax']

                current_delta = np.max(np.abs(dataframe['new_ax'] - dataframe['ax']))

                dataframe['ax'] = dataframe['new_ax']


        dataframe['ax'] = dataframe.apply(lambda x: 1.1 if x['ax'] < 0 else x['ax'], axis=1)

        # Make lifetable
        # qx = probability of death
        dataframe.loc[:, 'qx'] = dataframe['n']*dataframe[deathrate_column]/(1+(dataframe['n']-dataframe['ax'])
                                                                                  *dataframe[deathrate_column])
        # px = survival probability
        dataframe.loc[:, 'px'] = 1-dataframe['qx']

        # lx = number alive, initializing entire column here
        dataframe.loc[:, 'lx'] = 0

        # Initialize first age to arbitrary starting pop
        idx = pd.IndexSlice
        dataframe.loc[idx[:, :,  :, 0], idx['lx']] = 100000

        # Update all ages after the first one
        for a, age in enumerate(ages[1:]):
            
            prev_age = ages[a]
            # Need to add .values because the result will have a different age index than where it is being stored
            dataframe.loc[idx[:, :,  :, age], idx['lx']] = dataframe.loc[idx[:, :,  :, prev_age], idx['lx']].values * \
                                                          dataframe.loc[idx[:, :,  :, prev_age], idx['px']].values

        # dx = crude death rate
        dataframe['dx'] = dataframe['lx']*dataframe['qx']

        # nLx
        dataframe
        dataframe['nLx'] = 0.0

        # Update all ages except the last one
        for a, age in enumerate(ages[:-1]):
            next_age = ages[a+1]
            dataframe.loc[idx[:, :,  :, age], idx['nLx']] = (dataframe.loc[idx[:, :,  :, age], idx['n']].values *
                                                                dataframe.loc[idx[:, :,  :, next_age], idx['lx']].values) +\
                                                               (dataframe.loc[idx[:, :,  :, age], idx['ax']].values *
                                                                dataframe.loc[idx[:, :,  :, age], idx['dx']].values)


        dataframe.loc[idx[:, :,  :, ages[-1]], idx['nLx']] = dataframe.loc[idx[:, :,  :, ages[-1]], idx['lx']] /\
                                                                dataframe.loc[idx[:, :,  :, ages[-1]], idx[deathrate_column]]

        # Initial Tx = sum nLx
        # Reset index to aggregate over ages
        dataframe = dataframe.reset_index()
        aggregated = dataframe.groupby(['location_id', 'year_id', 'sex_id']).sum()[['nLx']]

        aggregated = aggregated.rename(columns={'nLx':'Tx'})
        aggregated = aggregated.reset_index()

        dataframe = dataframe.merge(aggregated)
        dataframe = dataframe.set_index(['location_id', 'year_id', 'sex_id', 'age_group_years_start'])
        dataframe = dataframe.sortlevel()

        # Now Tx for the rest of the ages
        for a, age in enumerate(ages[1:]):
            prev_age = ages[a]
            dataframe.loc[idx[:, :,  :, age], idx['Tx']] = dataframe.loc[idx[:, :,  :, prev_age], idx['Tx']].values -\
                                                            dataframe.loc[idx[:, :,  :, prev_age], idx['nLx']].values

        if col_num == 0:
            result = dataframe.copy()

        result['ex_'+deathrate_column.replace(deathrate_prefix,'')] = dataframe['Tx']/dataframe['lx']
        result['lx_'+deathrate_column.replace(deathrate_prefix, '')] = dataframe['lx']

        if col_num==99:
            print deathrate_column

        # Save ax for yll calcs
        if not ax_columns:
            result['ax_'+deathrate_column.replace(deathrate_prefix, '')] = dataframe['ax']

    if drop_deathrate:
        result = result.drop(deathrate_columns, axis=1)

    # Save as a csv so we can aggregate in bash
    if save_path is not None:
        result.to_csv(save_path)


    print deathrate_columns

    return result

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--in_path", help="filepath to pandas dataframe containing columns with deathrate data",
                        type=str)

    parser.add_argument("--o", help="filepath of where to save result csv",
                        type=str)
    parser.add_argument("--pop_path", help="path to pop data",
                        type=str)
    parser.add_argument("--pop_prefix", help="prefix for pop cols",
                        type=str)
    parser.add_argument("-sex", help="optional sex_id to calculate life expectancy for.",
                        type=int)

    parser.add_argument("-exp", dest="exp", help="If present, deathrate columns will be exponentiated first",
                        action="store_true")

    parser.set_dUSERts(exp=False)

    parser.add_argument("-mx_pre", help="string prefix that identifies which columns have deathrate data in the input dataset",
                        type=str)
    parser.set_dUSERts(mx_pre='mx')

    parser.add_argument("-ax_path", help="optional path to external ax dataset",
                        type=str)
    parser.add_argument("-ax_pre", help="string prefix that identifies the ax columns in the optional ax dataset",
                        type=str)
    parser.set_dUSERts(ax_pre='')

    args = parser.parse_args()

    args.in_path = args.in_path.strip()
    args.o = args.o.strip()

    args.mx_pre = args.mx_pre.strip()

    if args.ax_path is not None:
        args.ax_path = args.ax_path.strip()

    if args.ax_pre is not None:
        args.ax_pre = args.ax_pre.strip()

    if args.pop_path is not None:
        args.pop_path = args.pop_path.strip()

    if args.pop_prefix is not None:
        args.pop_prefix = args.pop_prefix.strip()


    # Read the input data
    limits = []

    if args.sex is not None:
        limits += ["sex_id=={s:d}".format(s=args.sex)]
    print limits
    deathrate_df = pandas_read_infer_filetype(args.in_path, limits=limits)

    deathrate_cols = [col for col in deathrate_df if args.mx_pre in col]


    if args.exp:
        for col in deathrate_cols:
            deathrate_df.loc[:,col] = np.exp(deathrate_df[col])

    calculate_ex(deathrate_df, deathrate_columns=deathrate_cols, external_ax=args.ax_path, ax_prefix=args.ax_pre,
                 pop_prefix = args.pop_prefix, pop_path = args.pop_path,
                 drop_deathrate=True, deathrate_prefix='nothing', save_path=args.o)
