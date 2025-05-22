import pandas as pd
from cms.lookups.inputs import location_type as lt
from cms.helpers.intermediate_tables.location_selection import Location_Select
from cms.helpers import build_CI_lookups

'''
Convert MAX locations to FIPS codes
'''


def fmt_county_codes(df):
    df.EL_RSDNC_CNTY_CD_LTST.fillna(0, inplace=True)
    df['EL_RSDNC_CNTY_CD_LTST'] = df.EL_RSDNC_CNTY_CD_LTST.astype(int).astype(str)
    df['EL_RSDNC_CNTY_CD_LTST'] = df['EL_RSDNC_CNTY_CD_LTST'].str.zfill(3)
    
    return df
    
def convert_MAX_loc(df):
    loc = build_CI_lookups.build_state_map(lt.loc_lookup)
    loc = loc[loc.code_type.isin(['MAX_STATE_CD', 'STATE_CNTY_FIPS'])]
    loc = loc.pivot(index='location_id', columns='code_type', 
                  values='code').reset_index()
    loc.rename({'MAX_STATE_CD' : 'STATE_CD'}, axis=1, inplace=True)
    
    return df.merge(loc, on='STATE_CD')

def create_fips(df):
    df = fmt_county_codes(df)
    df = convert_MAX_loc(df)
    df['location_id'] = df['STATE_CNTY_FIPS'] + df['EL_RSDNC_CNTY_CD_LTST']
    
    return df.drop(['STATE_CNTY_FIPS', 'EL_RSDNC_CNTY_CD_LTST', 'STATE_CD'], axis=1)