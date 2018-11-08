from get_draws.api import get_draws
from db_queries.get_age_metadata import get_age_weights as gaw
from db_queries import get_population as gp
from db_queries import get_location_metadata as glm
import pandas as pd
import numpy as np
import feather
from numpy import inf
import scipy
locmeta = glm(35)
country_list = list(locmeta[locmeta.level == 3]['location_id'])
draw_cols = ['draw_{}'.format(i) for i in range(0,1000)]
index_cols = ['indicator_id','location_id', 'year_id', 'sex_id', 'age_group_id', 'scenario']

def intercept_shift(future, past, indicator_id, log = "log", full = False, scenarios = True):
    """Intercept Shift future data using past/future data year 2017
    Will perform function in either log or logit space.
    Filters to SDG reporting locations and SDG reporting years.

    Parameters
    ----------
    past : pandas DataFrame
        expected to contain years 1990 to 2017

    future : pandas DataFrame
        expected to contain years 2017 to 2030
        and same age group, location, and sex variables as past

    indicator_id: integer
        indicator_id to assign newly constructed future data

    log: string, "log" or "logit"
        Enter to choose if intercept shift should be done in log space or logit space.  *Proportions should be done in logit, Default is log

    full: Boolean
        Enter to obtain full time series or only the intercept shifted forecasted data.  Default is future only, False.

    scenarios: Boolean
        Enter to obtain intercept shift of indicators with scenarios or without (goal keepers have scenarios)
    Returns
    -------
    out : pandas DataFrame
        Contains year .
    """
    if scenarios:
        assert "scenario" in future.columns, "scenario column must be present in future data if scenarios is True"
        assert "scenario" in past.columns, "scenario column must be present in past data if scenarios is True"
        assert set(past.scenario) == set([0]), "make sure past data contains all scenarios and that the scenario is not in better, reference worse and in -1, 0 ,1"
        assert set(future.scenario) == set([-1,0,1]), "make sure future data contains all scenarios and that the scenario is not in better, reference worse and in -1, 0 ,1"
    else:
        if "scenario" in past.columns:
            past = past[past.scenario == 0]
            assert len(past) > 0, "Make sure that if scenario columns exist in past data, the scenarios contain integer 0, as reference scenario"
        if "scenario" in future.columns:
            future = future[past.scenario == 0]
            assert len(past) > 0, "Make sure that if scenario columns exist in future data, the scenarios contain integer 0, as reference scenario"
    assert log == "log" or log == "logit", "Must enter log or logit"
    assert set(past.age_group_id) == set(future.age_group_id), "Make sure you have identical age groups"
    assert set(past.sex_id) == set(future.sex_id), "Make sure you have identical sexes"
    assert set(past.location_id) == set(future.location_id), "Make sure you have identical locations"
    future = future[(future.year_id > 2016) & (future.year_id < 2031)]
    past = past[(past.year_id > 1989) & (past.year_id < 2018)]
    assert len(future) != 0, "Empty data frame.  Future data should have years 2017 to 2030"
    assert len(future[(future.year_id ==2017)]) != 0, "Future data does not contain year 2017"
    past = past[(past.location_id.isin(country_list)) & (past.year_id.isin(range(1990,2018)))]
    assert len(past) != 0, "Empty data frame.  Past data should have years 1990 to 2017"
    assert len(past[(past.year_id ==2017)]) != 0, "Past data does not contain year 2017"
    past.loc[:, draw_cols] = past.loc[:, draw_cols].clip(lower = 1e-12)
    future.loc[:, draw_cols] = future.loc[:, draw_cols].clip(lower = 1e-12)
    if log == 'log':
        if scenarios:
            future_dfs = []
            for i in [-1,0,1]:
                future_l_space = future[(future.year_id ==2017) & (future.scenario == i)]
                future_l_space = future_l_space.drop('scenario',axis = 1)
                future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                future_l_space[draw_cols] = future_l_space[draw_cols].applymap(np.log)
                past_l_space = past[past.year_id == 2017]
                past_l_space[draw_cols] = past_l_space[draw_cols].applymap(np.log)
                past_l_space = past_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                past_l_space[draw_cols] = past_l_space[draw_cols] - future_l_space[draw_cols]
                future_l_space = future[future.scenario == i]
                future_l_space = future_l_space.drop('scenario',axis = 1)
                future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                future_l_space[draw_cols] = future_l_space[draw_cols].applymap(np.log)
                future_l_space = future_l_space.reset_index()
                past_l_space = past_l_space.reset_index()
                dfs = []
                for year in list(set(future_l_space.year_id)):
                    df = past_l_space.copy()
                    df['year_id'] = year
                    print year
                    dfs.append(df)
                past_l_space = pd.concat(dfs, ignore_index = True)
                future_l_space = future_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
                past_l_space = past_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
                future_l_space[draw_cols] = (future_l_space[draw_cols] + past_l_space[draw_cols])
                future_l_space[draw_cols] = np.e**(future_l_space[draw_cols])
                future_l_space['scenario'] = i
                future_dfs.append(future_l_space)
            future = pd.concat(future_dfs, ignore_index= True)
        else:
            index_cols.remove('scenario')
            future_l_space = future[(future.year_id ==2017)]
            future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            future_l_space[draw_cols] = future_l_space[draw_cols].applymap(np.log)
            past_l_space = past[past.year_id == 2017]
            past_l_space[draw_cols] = past_l_space[draw_cols].applymap(np.log)
            past_l_space = past_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            past_l_space[draw_cols] = past_l_space[draw_cols] - future_l_space[draw_cols]
            future_l_space = future.copy()
            future_l_space.loc[:, draw_cols] = future_l_space.loc[:, draw_cols].clip(lower = 1e-12)
            future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            future_l_space[draw_cols] = future_l_space[draw_cols].applymap(np.log)
            future_l_space = future_l_space.reset_index()
            past_l_space = past_l_space.reset_index()
            dfs = []
            for year in list(set(future_l_space.year_id)):
                df = past_l_space.copy()
                df['year_id'] = year
                print year
                dfs.append(df)
            past_l_space = pd.concat(dfs, ignore_index = True)
            future_l_space = future_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
            past_l_space = past_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
            future_l_space[draw_cols] = (future_l_space[draw_cols] + past_l_space[draw_cols])
            future_l_space[draw_cols] = np.e**(future_l_space[draw_cols])
            future = future_l_space.copy()
    elif log == 'logit':
        if scenarios:
            future_dfs = []
            for i in [-1,0,1]:
                future_l_space = future[(future.year_id ==2017) & (future.scenario == i)]
                future_l_space = future_l_space.drop('scenario',axis = 1)
                future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.logit)
                past_l_space = past[past.year_id == 2017]
                past_l_space = past_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                past_l_space[draw_cols] = past_l_space[draw_cols].applymap(scipy.special.logit)
                past_l_space[draw_cols] = past_l_space[draw_cols] - future_l_space[draw_cols]
                future_l_space = future[future.scenario == i]
                future_l_space = future_l_space.drop('scenario',axis = 1)
                future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
                future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.logit)
                future_l_space = future_l_space.reset_index()
                past_l_space = past_l_space.reset_index()
                dfs = []
                for year in list(set(future_l_space.year_id)):
                    df = past_l_space.copy()
                    df['year_id'] = year
                    print year
                    dfs.append(df)
                past_l_space = pd.concat(dfs, ignore_index = True)
                future_l_space = future_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
                past_l_space = past_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
                future_l_space[draw_cols] = (future_l_space[draw_cols] + past_l_space[draw_cols])
                future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.expit)
                future_l_space['scenario'] = i
                future_dfs.append(future_l_space)
            future = pd.concat(future_dfs, ignore_index= True)
        else:
            index_cols.remove('scenario')
            future_l_space = future[(future.year_id ==2017)]
            future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.logit)
            past_l_space = past[past.year_id == 2017]
            past_l_space = past_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            past_l_space[draw_cols] = past_l_space[draw_cols].applymap(scipy.special.logit)
            past_l_space[draw_cols] = past_l_space[draw_cols] - future_l_space[draw_cols]
            future_l_space = future.copy()
            future_l_space = future_l_space.set_index(['location_id', 'age_group_id','sex_id']).sort_index()
            future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.logit)
            future_l_space = future_l_space.reset_index()
            past_l_space = past_l_space.reset_index()
            dfs = []
            for year in list(set(future_l_space.year_id)):
                df = past_l_space.copy()
                df['year_id'] = year
                print year
                dfs.append(df)
            past_l_space = pd.concat(dfs, ignore_index = True)
            future_l_space = future_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
            past_l_space = past_l_space.sort_values(by = ['location_id', 'age_group_id','sex_id','year_id'], axis = 0).reset_index().drop('index',axis = 1 )
            future_l_space[draw_cols] = (future_l_space[draw_cols] + past_l_space[draw_cols])
            future_l_space[draw_cols] = future_l_space[draw_cols].applymap(scipy.special.expit)
            future = future_l_space.copy()
        future[draw_cols] = future[draw_cols].clip(upper=1)
    past['indicator_id'] = indicator_id
    future['indicator_id'] = indicator_id
    future = future[index_cols + draw_cols]
    future = future.loc[future.year_id > 2017]
    past['scenario'] = 0
    past = past[index_cols + draw_cols]
    if full:
        df = pd.concat([past,future], ignore_index=True)
    else:
        df = future
    df.sort_values(['location_id', 'year_id'], ascending=[True, True], inplace=True)
    return df