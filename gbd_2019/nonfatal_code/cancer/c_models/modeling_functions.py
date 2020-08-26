
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    modeled_locations
)
from cancer_estimation.a_inputs.a_mi_registry import (
    populations as pop,
    mi_dataset
)
from cancer_estimation.b_staging import staging_functions
from cancer_estimation.c_models import modeling_functions
from cancer_estimation._database import cdb_utils as cdb
import pandas as pd


def load_sdi_map():  
    ''' Loads a map of sdi quintiles by location_id
    '''
    sdi_map = {'Low SDI': 1, 'Low-middle SDI':2, 'Middle SDI':3, 'High SDI':4, 
                'High-middle SDI':5}
    sdi_data = pd.read_csv(utils.get_path("sdi_quintiles"))
    sdi_data.rename(columns={'sdi_quintile':'sdi_quintile_name'}, inplace=True)
    sdi_data = sdi_data.loc[sdi_data['sdi_quintile_name'].notnull(),:]
    sdi_data['sdi_quintile'] = sdi_data['sdi_quintile_name'].apply(lambda x: sdi_map[x])
    sdi_data = modeled_locations.add_country_id(sdi_data)
    sdi_data.loc[sdi_data['country_id'].eq(62),'sdi_quintile'] = 4 
    sdi_data.loc[sdi_data['country_id'].eq(90),'sdi_quintile'] = 5 
    return(sdi_data[['location_id', 'sdi_quintile']])
