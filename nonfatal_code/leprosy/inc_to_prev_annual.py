"""

AUTHOR: USERNAME

"""

from sys import argv, path as sys_path
import pandas as pd
from os import path,makedirs
from glob import glob
from scipy.integrate import odeint

def gen_zero_prev_df(year_start,ages,drawnum):
    col_dict = {('prev_'+str(i)):0 for i in range(drawnum)}
    index = pd.MultiIndex.from_tuples([(year_start,j) for j in ages],names=['year','age'])
    return pd.DataFrame(col_dict,index=index)
    
def rhs_inc_and_mort(prev,time,inc,mort):
    return inc - (inc + mort) * prev

def rhs_inc_only(prev,time,inc):
    return inc * (1 - prev)
    
def rhs_mort_only(prev,time,mort_or_rem):
    return -mort_or_rem * prev
    
def ode_func(df,prev_col,t,has_inc=True,has_mort=True,inc_col=None,mort_col=None):
    if has_inc==True and has_mort==True:
        return odeint(rhs_inc_and_mort,df[prev_col],[0,t],args=(df[inc_col],df[mort_col]))[1]
    elif has_inc==True:
        return odeint(rhs_inc_only,df[prev_col],[0,t],args=(df[inc_col],))[1]
    elif has_mort==True:
        return odeint(rhs_mort_only,df[prev_col],[0,t],args=(df[mort_col],))[1]
    
def integrate_one_step(df_input,time=1,has_inc=True,has_mort=True,mort_or_rem_str='mort',inc_df=None,mort_df=None,drawnum=1000):
    df = df_input.copy()
    prev_cols = ['prev_' + str(i) for i in range(drawnum)]
    inc_mort_cols = []
    if has_inc:
        df = df.join(inc_df,how='left')
        inc_cols = [i.replace('prev','inc') for i in prev_cols]
        inc_mort_cols = inc_mort_cols + inc_cols
    else:
        inc_cols = [None] * drawnum
    if has_mort:
        df = df.join(mort_df,how='left')
        mort_cols = [i.replace('prev',mort_or_rem_str) for i in prev_cols]
        inc_mort_cols = inc_mort_cols + mort_cols
    else:
        mort_cols = [None] * drawnum
    for i in range(drawnum):
        prev_col = prev_cols[i]
        inc_col = inc_cols[i]
        mort_col = mort_cols[i]
        df[prev_col] = ode_func(df,prev_col,time,has_inc,has_mort,inc_col,mort_col)
    df = df.drop(inc_mort_cols, axis=1)
    return df

def merge_pop(df,pop_df):
    return df.join(pop_df, how='left')
    
def increment_ages_years(df):
    result = df.reset_index()
    result[['year','age']] = result[['year','age']] + 1
    result = result.set_index(['year','age'])
    return result

    
def age_collapse(df,age_dict,interp_module,has_under1=False):
    if has_under1:
        result = df.reset_index(level='age')
        under1 = result[result['age'] < 1].drop('pop',axis=1)
        under1 = under1.set_index('age',append=True)
        result = result[result['age'] >=1]
        result = result.set_index('age',append=True)
    else:
        result = df.copy()
    result = interp_module.get_cases(result,'prev','cases')
    prev_cols = result.filter(like='prev').columns
    result = result.drop(prev_cols,axis=1)
    result = interp_module.map_age_to_age_grp(result,age_dict)
    result = result.groupby(level=result.index.names).sum().drop('age',axis=1)
    result.index = result.index.rename(['year','age'])
    result = interp_module.get_cases(result,'cases','prev',to_cases=False)
    result = result.reindex(columns = prev_cols)
    if has_under1:
        result = result.append(under1)
        result.index = pd.MultiIndex.from_tuples(result.index,names=['year','age'])
    return result.sort_index()

def append_under1_as_zero(df,under1_ages):
    ix = df.index.names
    under1 = df.iloc[:3,:].reset_index()
    under1['age'] = under1_ages
    under1 = under1.set_index(ix)
    under1[:] = 0
    result = df.append(under1)
    result.index = pd.MultiIndex.from_tuples(result.index,names=['year','age'])
    return result

def get_single_age_dict(terminal_age=95):
    result = {float(i):[float(i),i+1.] for i in range(terminal_age)}
    result[float(terminal_age)] = [float(terminal_age), float(terminal_age) + 5]
    return result

def save_draws(df,iso3,sex,out_dir):
    year = df.index.get_level_values('year')[0]
    save_df = df.reset_index(level='year',drop=True)
    save_df = order_draws(save_df,'prev')
    save_df = save_df.rename(columns=(lambda x: x.replace('prev','draw')))
    save_df.to_csv(path.join(out_dir,'prevalence_%s_%i_%s.csv' %(iso3,year,sex)))
    
def order_draws(df,prefix):
    result = df.copy()
    draw_cols = df.filter(like=prefix).columns
    total_draws = len(draw_cols)
    result_cols = [prefix + "_" + str(i) for i in range(total_draws)]
    result = result.reindex(columns=result_cols)
    return result    
    
def progress_half_year_and_save(interp_module,df,pop_df,out_dir,iso3,sex,drawnum,age_grps_dict,
                                has_inc=True,has_mort=True,inc_df=pd.DataFrame(),
                                mort_df=pd.DataFrame()):
    # Get list of ages under 1
    list_of_under1 = df.index.levels[df.index._get_level_number('age')]
    list_of_under1 = list_of_under1[list_of_under1 < 1]
    
    # Integrate ODE over one year if there was any incidence or excess mortality during that year
    if has_inc or has_mort:
        result = integrate_one_step(df,.5,has_inc,has_mort,'mort',inc_df,mort_df,drawnum)
    else: result = df.copy()
    
    # add on pops
    result = merge_pop(result,pop_df)
    # collapse to gbd_age_groups
    result = age_collapse(result,age_grps_dict,interp_module,has_under1=True)
    
    #save
    save_draws(result,iso3,sex,out_dir)
    
def progress_one_year(interp_module,df,pop_df,drawnum,has_inc=True,has_mort=True,inc_df=pd.DataFrame(),mort_df=pd.DataFrame()):
    # Get list of ages under 1
    list_of_under1 = df.index.levels[df.index._get_level_number('age')]
    list_of_under1 = list_of_under1[list_of_under1 < 1]
    # Integrate ODE over one year if there was any incidence or excess mortality during that year
    if has_inc or has_mort:
        result = integrate_one_step(df,1,has_inc,has_mort,'mort',inc_df,mort_df,drawnum)
    else: result = df.copy()
    # add on pops
    result = merge_pop(result,pop_df)
    # increment ages and years 
    result = increment_ages_years(result)
    single_age_dict = get_single_age_dict()
    result = age_collapse(result,single_age_dict,interp_module)
    # append on new 0 prevalences for <1 ages
    result = append_under1_as_zero(result,list_of_under1)
    # return sorted prevalence 1 year later
    result = result.sort_index()
    return result

def get_unique_level_values(df,levelname):
    return list(df.index.levels[df.index._get_level_number(levelname)])  
    
def get_starting_prev(prev0_path,year,ages,drawnum,pop_grp,pop_s,age_grp_dict,interp_func):
    if not prev0_path:
        prev = gen_zero_prev_df(year,ages,drawnum)
    else:
        prev = pd.read_csv(prev0_path)
        prev['year'] = year
        prev = prev.set_index(['year','age'])
        prev = interp_func(prev,'draw',
            pop_grp,pop_s,age_grp_dict).rename(columns= lambda x: x.replace('draw','prev'))
    return prev
    
def inc_prev_integration(dfs,ages,reporting_years,iso3,sex,has_any_mort,prev0_path,out_dir,drawnum,pop_s,pop_grp,age_dict,interp_module):
    
    # get start and end years
    years = get_unique_level_values(dfs['inc'],'year')
    year_start = min(years)
    year_end = max(reporting_years)
    
    # Generate starting prev
    prev = get_starting_prev(prev0_path,year_start,ages,drawnum,pop_grp,pop_s,age_dict,interp_module.ipolate_to_single_year_ages)
        
    years_w_inc = set(dfs['inc'].index.get_level_values('year'))
    if has_any_mort:
        years_w_mort = set(dfs['mort'].index.get_level_values('year'))
    else:
        years_w_mort = set()
    for yr in range(year_start,year_end+1):
        if yr in years_w_inc: has_inc = True
        else: has_inc = False
        if yr in years_w_mort: has_mort = True
        else: has_mort = False
        if yr in reporting_years:
            progress_half_year_and_save(
                    interp_module,
                    prev,
                    dfs['pop_total'],
                    out_dir,
                    iso3,
                    sex,
                    drawnum,
                    age_dict,
                    has_inc=has_inc,
                    has_mort=has_mort,
                    inc_df=dfs['inc'],
                    mort_df=dfs['mort'])
                    
        prev = progress_one_year(
                    interp_module,
                    prev,
                    dfs['pop_total'],
                    drawnum,
                    has_inc=has_inc,
                    has_mort=has_mort,
                    inc_df=dfs['inc'],
                    mort_df=dfs['mort'])
        
        
def aggregate_data(dir,iso3,sex):
    files = glob(path.join(dir,'*%s*_%s.csv' %(iso3,sex)))
    result = pd.DataFrame()
    for i in files:
        df = pd.read_csv(i)
        filename = path.split(i)[1].rstrip('.csv')
        components = filename.split('_')
        # pop sex
        components.pop()
        df['year'] = int(components.pop())
        assert len(components) <= 3, "too many components: %s" %components
        result = result.append(df)
        # appending turns int into object, so must turn back
        result['year'] = result['year'].astype(int)
    result = result.set_index(['year','age'])
    return result
    
def get_pop_df(path,iso3,sex):
    """Get single year age group pops from the cleaned version of the single-yr-age-grp
    pops file."""
    result = pd.read_csv(path,index_col=['iso3','sex','year','age']).sort_index()
    result = result.ix[iso3,sex]
    return result
    
def get_pop_total(pop_s,pop_grp):

    ps = pop_s.reset_index(level='age')
    ps = ps[ps['age'] != 0].set_index('age',append=True)
    pg = pop_grp.reset_index(level='age')
    pg = pg[pg['age'] < 1].set_index('age',append=True)
    result = ps.append(pg)
    result.index = pd.MultiIndex.from_tuples(result.index,names=['year','age'])
    result = result.sort_index()
    return result
    
def main(ages_dir,pyhme_parent,iso3,sex,pop_s_path,pop_grp_path,out_dir,inc_dir,
            mort_dir=None,prev0_path=None):
    # Imports
    sys_path.append(pyhme_parent)
    from pyHME import interpolate_ages,params_annual
    p = params_annual.Params(age_files_dir=ages_dir)
    # empty output directory
    try:
        makedirs(out_dir)
    except:
        pass
    # Get population dataframes
    pop_s = get_pop_df(pop_s_path,iso3,sex)
    pop_grp = get_pop_df(pop_grp_path,iso3,sex)
    pop_total = get_pop_total(pop_s,pop_grp)
    # Get incidence and mortality (if exists) dataframes
    inc_df = aggregate_data(inc_dir,iso3,sex)
    interp_inc = interpolate_ages.ipolate_to_single_year_ages(inc_df,'draw',
            pop_grp,pop_s,p.gbd_ages_dict).rename(columns= lambda x: x.replace('draw','inc'))
    drawnum = len(list(interp_inc.filter(like='inc').columns))
    if mort_dir:
        mort_df = aggregate_data(mort_dir,iso3,sex)
        interp_mort = interpolate_ages.ipolate_to_single_year_ages(mort_df,'draw',
            pop_grp,pop_s,p.gbd_ages_dict).rename(columns = lambda x: x.replace('draw','mort'))
    else: interp_mort=None
    # get list of ages in interpreted dataframe
    ages = get_unique_level_values(interp_inc,'age')
    # get dict of dataframes
    dfs = {'pop_s':pop_s,'pop_grp':pop_grp,'pop_total':pop_total,'inc':interp_inc,'mort':interp_mort}
    # integrate over years
    inc_prev_integration(dfs,ages,p.reporting_years,iso3,sex,mort_dir,prev0_path,
                    out_dir,drawnum,pop_s,pop_grp,p.gbd_ages_dict,interpolate_ages)
    

if __name__ == '__main__':
    iso3 =          argv[1]
    sex =           argv[2]
    has_mort =      bool(int(argv[3]))
    has_prev0 =     bool(int(argv[4]))
    ages_dir =      argv[5]
    code_dir =      argv[6]
    pop_s_path =    argv[7]
    pop_grp_path =  argv[8]
    out_dir =       argv[9]
    inc_dir =       argv[10]
    if has_mort:
        mort_dir =  argv[11]
    else:
        mort_dir =  None
    if has_prev0 and has_mort:
        prev0_path = argv[12]
    elif has_prev0:
        prev0_path = argv[11]
    else:
        prev0_path = None
        
    main(ages_dir,code_dir,iso3,sex,pop_s_path,pop_grp_path,out_dir,inc_dir,mort_dir,prev0_path)
