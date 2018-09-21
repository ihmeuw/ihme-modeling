"""
PURPOSE: This script takes the results of the empirical hierarchy and applies some custom
adjustments to arrive at an expert-driven hierarchy.
AUTHOR: USERNAME
DATE: DATE
"""

# Imports
import sys
from os.path import join
import pandas as pd

def manual_ordering_of_ncodes(df):
    """Order non-0 long-term DW N-codes that are assigned the same DWs based on expert-driven notions of severity"""
    result = df.copy()
    result['order'] = 0
    result['order'][result.ncode.isin(['N4','N9','N35','N14','N35'])] = 1
    result['order'][result.ncode.isin(['N5','N10','N36','N47','N36'])] = 2
    result['order'][result.ncode=='N3'] = 3
    return result
    
def apply_custom_adjustments(df):
    """These are the custom, expert-driven adjustments applied to the hierarchy."""
    result = df.copy()
    ## Put LA burns just below burns >=20% b/c LA burns do not yet show up in dataset
    result['dw_inp'][result.ncode=='N10'] = result['dw_inp'][result.ncode=='N9'].iloc[0]
    
    ## Put asphyxiation just below submersion b/c asphyxiation does not yet show up in dataset
    for i in ['inp','noninp','pool']:
        result['dw_'+i][(result.ncode=='N36') & (result['dw_'+i].isnull())] = result['dw_'+i][(result.ncode=='N35')].iloc[0]
    
    ## Put Complications of Medical Treatement at bottom of hierarchy
    for i in ['inp','noninp','pool']:
        result['dw_'+i][result.ncode=='N46'] = -9
        
    ## Order non-0 long-term DW N-codes that are assigned the same DWs based on expert-driven
    ## notions of severity
    result = manual_ordering_of_ncodes(result)
    
    return result

def use_pooled_when_needed(df):
    """This function replaces the inpatient and outpatient DWs with the pooled
    DW when the outpatient DW > inpatient DW (implausible situation)."""
    result = df.copy()
    result['use_pooled'] = (df.dw_noninp > df.dw_inp) & (df.dw_noninp != 9)
    result.dw_inp[result.use_pooled] = result.dw_pool[result.use_pooled]
    result.dw_noninp[result.use_pooled] = result.dw_pool[result.use_pooled]
    return result
    
def add_comments(dw):
    """Add comments describing how we treated a given N-code."""
    if dw == -9: return "Only selected when no other N-codes present"
    if dw == 9: return "Inpatient only"
    
def get_rank(df,dw_col,dw_func):
    """Develop a ranking of N-codes"""
    result = df.copy()
    
    # rank those that are inpatient only 
    inp_only = result[result[dw_col] == 9]
    inp_only['rank'] = 0
    inp_only.set_index('rank',inplace=True)
    
    # rank those that fall in empirical hierarchy
    hierarchy = result[(result[dw_col] < 9) & (result[dw_col] > 0)]
    hierarchy.reset_index(inplace=True,drop=True)
    hierarchy.index = hierarchy.index + 1
    hierarchy.index.name = 'rank'
    max_rank = max(hierarchy.index)
    
    # rank those that have no LT DW in regression
    no_lt_dw = result[result[dw_col] == 0]
    no_lt_dw = rank_by_st_DW(no_lt_dw,dw_func,start = max_rank)
    max_rank = max(no_lt_dw.index)
    
    # rank those that are arbitrarily set to only occur if no other N-codes
    abs_bottom = result[result[dw_col] == -9]
    abs_bottom['rank'] = max_rank + 1
    abs_bottom.set_index('rank',inplace=True)
    
    result = inp_only.append(hierarchy.append(no_lt_dw.append(abs_bottom)))
    return result
    
def rank_by_st_DW(df,dw_func,start=0):
    """rank a given set of N-codes by their short-term GBD DWs"""
    result = df.copy()
    ## load dws
    dws = dw_func(category='st',summ=True)
    dws = dws.reindex(columns=['mean_dw'])
    
    ## merge dws onto N-codes
    result = result.join(dws,how='left',on='ncode')
    
    ## rank
    result['rank'] = result.rank(axis=0,ascending=False)['mean_dw']
    ## Order non-0 long-term DW N-codes that are assigned the same DWs based on expert-driven
    ## notions of severity
    result = manual_ordering_of_ncodes(result)
    
    ## re-order them by appropriate rank, incorporating these tiebreakers
    result = result.sort(columns=['rank','order'],ascending=[True,True])
    result = result.drop(['rank','order','mean_dw'],axis=1)
    result = result.reset_index(drop=True)
    result['rank'] = start + 1 + result.index.get_values()

    ## set index using ncode
    result = result.set_index('rank')
    return result

def main(hierarchy_path,py_modules_parent,out_dir):
    # Import needed functions
    sys.path.append(py_modules_parent)
    from py_modules.funcs import get_ncode_dw_map
    
    # Import completed empirical hierarchy
    empirical = pd.read_csv(join(hierarchy_path,'FILEPATH.csv'))
    
    # round N values to avoid confusion for experts with probabilistic mapping
    for i in ['inp','noninp','pool']:
        empirical['N_'+i] = empirical['N_'+i].map(lambda x: round(x))
        
    # Save empirical hierarchy as worksheet
    writer = pd.ExcelWriter(join(out_dir,'FILEPATH.xls'))
    empirical.to_excel(writer,'empirical_hierarchy',index=False)

    adjusted = use_pooled_when_needed(empirical)
    adjusted = apply_custom_adjustments(adjusted)

    # Remaining N-codes that did not show up in dataset are given 0 DW
    for i in ['inp','noninp','pool']:
        adjusted['dw_'+i][adjusted['dw_'+i].isnull()] = 0
    
    to_save = {}
    for i in ['inp','noninp']:
        dw_col = 'dw_'+i
        n_col = 'N_'+i
        to_save[i] = adjusted[['ncode','name',dw_col,n_col,'use_pooled','order']].sort(columns=[dw_col,'order'],ascending=[False,True])
        
        # Make comments
        to_save[i]['Comments'] = to_save[i][dw_col].map(add_comments)
        
        # develop rank
        to_save[i] = get_rank(to_save[i],dw_col,get_ncode_dw_map)
        
        # delete columns we don't want
        for j in ['order',dw_col]:
            del to_save[i][j]
            
        # sort in order of rank
        to_save[i] = to_save[i].sort()
        
        # Make sure columns are in order
        to_save[i] = to_save[i].reindex(columns=['ncode','name','N_'+i,'use_pooled','Comments'])
        to_save[i]['use_pooled'] = to_save[i]['use_pooled'].astype(bool)
        
        # save worksheet
        to_save[i].reset_index().to_excel(writer,i,index=False)
    
    # save excel file
    writer.save()

if __name__ == '__main__':
    hierarchy_path = sys.argv[1]
    py_modules_parent = sys.argv[2]
    out_dir = sys.argv[3]
    main(hierarchy_path,py_modules_parent,out_dir)
