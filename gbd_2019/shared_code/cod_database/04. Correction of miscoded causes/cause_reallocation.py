import pandas as pd
import numpy as np
from cod_process import CodProcess
from cod_prep.claude.configurator import Configurator
from cod_prep.utils.misc import print_log_message
from cod_prep.utils import report_if_merge_fail
from cod_prep.downloaders.ages import get_cod_ages
from cod_prep.downloaders.engine_room import (get_cause_map,
	remove_five_plus_digit_icd_codes)

CONF = Configurator()


def remap_causes(df, remap_codes, target_dict):
	# check that all the remap codes are garbage, if not that means
	# the mapping for these codes changed and we need to rethink this step
	assert (df.loc[df.code_id.isin(remap_codes)].cause_id == 743).all(), \
		'Map has changed, leukemia adjustment should only affect garbage'
	for key in target_dict.keys():
		df.loc[
			(df.code_id.isin(remap_codes)) &
			(df.age_group_id.isin(target_dict[key])),
			'code_id'
		] = key
	return df

def adjust_leukemia_subtypes(df, code_system_id, code_map_version_id):
	# will be remapping onto 4 different leukemia subtypes, get the code ids to map to
    cause_map = get_cause_map(code_system_id, code_map_version_id=code_map_version_id,
                    force_rerun=False, block_rerun=True)
    cause_map = remove_five_plus_digit_icd_codes(cause_map, code_system_id=code_system_id)
    ll_acute = int(cause_map.query("cause_id == 845").iloc[0]['code_id'])
    ll_chronic = int(cause_map.query("cause_id == 846").iloc[0]['code_id'])
    ml_acute = int(cause_map.query("cause_id == 847").iloc[0]['code_id'])
    other = int(cause_map.query("cause_id == 943").iloc[0]['code_id'])

    # splitting between 0-19 and 20+
    age_meta_df = get_cod_ages(force_rerun=False, block_rerun=True)
    under_19 = age_meta_df.loc[age_meta_df.age_group_years_start < 20].age_group_id.tolist()
    twenty_plus = age_meta_df.loc[age_meta_df.age_group_years_start >= 20].age_group_id.tolist()

    # remapping
    if code_system_id == 1:
    	# leukemia by age 1
    	df = remap_causes(df,
    		remap_codes=[2747],
    		target_dict={ll_acute: under_19, ll_chronic: twenty_plus}
    	)
    	# leukemia by age 2
    	df = remap_causes(df,
    		remap_codes=[2756, 2760, 2768, 2769, 2770],
    		target_dict={ll_acute: under_19, other: twenty_plus}
    	)
    	# leukemia by age 3
    	df = remap_causes(df,
    		remap_codes=[2804, 2803, 2805, 2818, 2824, 2826],
    		target_dict={ml_acute: under_19, other: twenty_plus}
    	)
    elif code_system_id == 6:
    	# leukemia by age 1
    	df = remap_causes(df,
    		remap_codes=[50966],
    		target_dict={ll_acute:under_19, ll_chronic:twenty_plus}
    	)
    	# leukemia by age 2
    	df = remap_causes(df,
    		remap_codes=[50974, 50975, 50979],
    		target_dict={ll_acute:under_19, other:twenty_plus}
    	)
    	# leukemia by age 3
    	df = remap_causes(df,
    		remap_codes=[51000, 51017, 51021, 51004, 51025],
    		target_dict={ml_acute:under_19, other:twenty_plus}
    	)
    df.loc[df.code_id == ll_acute, 'cause_id'] = 845
    df.loc[df.code_id == ll_chronic, 'cause_id'] = 846
    df.loc[df.code_id == ml_acute, 'cause_id'] = 847
    df.loc[df.code_id == other, 'cause_id'] = 943
    return df

def manually_reallocate_gbdcauses(df):
    """Moving epilepsy, ibd, HIV deaths.

    Unrelrated to moving Alzheimer's/Parkinson's/afib, sort of like a special
    pre-redisribution package.
    """
    # storing incoming deaths to assure totals remain constant after reallocation
    orig_deaths = df.deaths.sum()

    # reading in cleaned prop_df
    prop_df = pd.read_excel(CONF.get_resource('manual_gbdcause_reallocation'))

    # merge prop_df onto existing data frame
    # changing code_ids to target and scaling deaths by prop where merge successful
    df = df.merge(prop_df, on=['sex_id', 'cause_id', 'age_group_id'], how='left')

    df.loc[df.target.notnull(), 'code_id'] = df['target']
    assert df.code_id.notnull().all()

    df.loc[df.prop.notnull(), 'deaths'] = df['deaths'] * df['prop']
    assert np.allclose(df.deaths.sum(), orig_deaths)

    # fixing cause_ids where merge successful
    # ZZZ
    df.loc[df.code_id == 94985, 'cause_id'] = 743
    # rest are "acause_digest_ibd", e.g.
    df.loc[df.code_id == 156952, 'cause_id'] = 532
    df.loc[df.code_id == 156953, 'cause_id'] = 545
    df.loc[df.code_id == 100373, 'cause_id'] = 298

    df.drop(['target', 'prop'], axis=1, inplace=True)
    assert df.notnull().values.all()

    return df