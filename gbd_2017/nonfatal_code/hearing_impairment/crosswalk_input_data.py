################################################################################
# Project: Hearing Impairments
# Date: May 2018, GBD 2017
# Purpose: The purpose of this script is to crosswalk the input data for the GBD
#          hearing impairments estimation process. This process is necessary
#		   because hearing loss estimates as reported in the literature are
#          often not consistent with GBD hearing loss categories. For instance,
#          many surveys report hearing loss using WHO hearing loss categories,
#		   which are different than GBD categories. In short, we use microdata
#          from NHANES to crosswalk data from studies that do not use GBD
#	       categories to convert point estimates (we are not currently
#          crosswalking uncertainty) to fit within GBD categories.
# Notes: This code borrows heavily from code previously written for the same
#        process. However, for GBD 2017, we decided to rewrite the hearing
#        impairments data process given issues with the previously used code.
#        Issues with the previous code include:
#        	1. Not allowing data points from a report to be crosswalked to
#			   multiple GBD categories. We updated the code to allow a data
#              point to be crosswalked to 2 categories
# 		 Potential Areas for Improvement Include:
#		    1. Adding unit tests of the code
#			2. Crosswalking uncertainty estimates
################################################################################

################################################################################
# Import packages
################################################################################

from __future__ import division
import pandas as pd
import numpy as np
from scipy.special import logit
from scipy.special import expit

# referenced https://stackoverflow.com/questions/842059/is-there-a-portable-way-to-get-the-current-username-in-python
import getpass
username = getpass.getuser()

################################################################################
# Read in datasets
################################################################################
in_dir = FILENAME
extraction = pd.read_excel(FILENAME)

# drop blank rows
extraction = extraction.dropna(subset = ['nid'])

crosswalk_map = pd.read_excel(FILENAME)

nhanes = pd.read_excel(FILENAME)

################################################################################
# Estimate crosswalk coefficients and prep crosswalk dataset
################################################################################
crosswalk_map_final = pd.merge(nhanes, crosswalk_map, on=['thresh_lower', 'thresh_upper'])
crosswalk_map_final.rename(columns={'prev': 'alt_prev', 'prev_se': 'alt_prev_se', 'thresh_lower': 'thresh_lower_data', 'thresh_upper': 'thresh_upper_data', 'gbd_lower': 'thresh_lower', 'gbd_upper': 'thresh_upper'}, inplace=True)
crosswalk_map_final = pd.merge(crosswalk_map_final, nhanes,  on=['thresh_lower', 'thresh_upper'], how='left')
crosswalk_map_final = crosswalk_map_final.rename(columns={'prev': 'ref_prev', 'prev_se': 'ref_prev_se'})
crosswalk_map_final['xwalk'] = crosswalk_map_final['ref_prev'] / crosswalk_map_final['alt_prev']
crosswalk_map_final['xwalk_logit'] = logit(crosswalk_map_final['ref_prev']) / logit(crosswalk_map_final['alt_prev'])

# categories that are included in the crosswalk map, but for which there are no gbd comparison
# have xwalk == null
crosswalk_map_final = crosswalk_map_final.dropna(subset = ['xwalk'])
crosswalk_map_final = crosswalk_map_final.dropna(subset = ['xwalk_logit'])

crosswalk_map_final = crosswalk_map_final[['thresh_lower_data', 'thresh_upper_data', 'xwalk', 'xwalk_logit', 'bundle_id', 'merge_number']]
crosswalk_map_final = crosswalk_map_final.rename(columns={'thresh_upper_data': 'thresh_upper', 'thresh_lower_data': 'thresh_lower'})

################################################################################
# Prep the extraction data
################################################################################
assert len(extraction[extraction.thresh_lower.isnull()]) == 0, \
	"all rows in the extraction sheet should have a thresh lower"
assert len(extraction[extraction.thresh_upper.isnull()]) == 0, \
	"all rows in the extraction sheet should have a thresh upper"

extraction = extraction.drop(['modelable_entity_id', 'modelable_entity_name'],
						     axis=1)

# set merge number equal to 1 for all observations, will update below for rows
# that need to be crosswalked to more than 1 GBD category
extraction['merge_number'] = 1

################################################################################
# Conduct the crosswalk
################################################################################
# determine which extraction thresholds need to be crosswalked to more than one
# 	gbd category
lowers = crosswalk_map_final.query("merge_number > 1").thresh_lower.values
uppers = crosswalk_map_final.query("merge_number > 1").thresh_upper.values
multiple_gbd_cat_lowers_and_uppers = zip(lowers, uppers)

# now make duplicate rows for the variables that will be crosswalked to multiple
# 	gbd categories
for cat in multiple_gbd_cat_lowers_and_uppers:
    duplicates = extraction.query("thresh_lower == @cat[0] and thresh_upper==@cat[1]").copy()
    duplicates['merge_number'] = 2
    extraction = extraction.append(duplicates)

crosswalked_extraction = pd.merge(extraction, crosswalk_map_final, on=['thresh_lower', 'thresh_upper', 'merge_number'], how='inner')

# put observations with crosswalk equal to 1 to the side for now, will append
# 	to the dataset later
xwalk1 = crosswalked_extraction.query("xwalk == 1").copy()
crosswalked_extraction = crosswalked_extraction.query("xwalk != 1")
assert np.all(crosswalked_extraction.xwalk.values != 1.), "do not crosswalk where crosswalk == 1"


# Calculate mean if mean is missing
crosswalked_extraction.loc[crosswalked_extraction['mean'].isnull(), 'mean'] = \
	crosswalked_extraction['cases'] / crosswalked_extraction['sample_size']

# Calculate standard error if standard error is missing
mean = crosswalked_extraction['mean']
st_err = crosswalked_extraction['standard_error']
sample_size = crosswalked_extraction['sample_size']
cases = crosswalked_extraction['cases']
crosswalked_extraction.loc[(st_err.isnull()) & (sample_size.isnull()), 'standard_error'] = \
	np.sqrt(mean*(1-mean) / (cases / mean))

assert len(crosswalked_extraction[crosswalked_extraction['mean'].isnull()]) == 0, \
	   "all rows need a mean"

# no propagating uncertainty for now
crosswalked_extraction['upper'] = ""
crosswalked_extraction['lower'] = ""
crosswalked_extraction['uncertainty_type_value'] = ""

# Calculate mean using the exponentiated (logit of the mean * xwalk_logit)
crosswalked_extraction['mean_1'] = crosswalked_extraction['mean'] * crosswalked_extraction['xwalk']
crosswalked_extraction['mean_2'] = expit(logit(crosswalked_extraction['mean']) * crosswalked_extraction['xwalk_logit'])

crosswalked_extraction.loc[crosswalked_extraction['mean'] != 0, 'mean'] = crosswalked_extraction['mean_2']

crosswalked_extraction.drop(['mean_1', 'mean_2'], axis=1, inplace=True)

crosswalked_extraction['xwalk'] = crosswalked_extraction['xwalk'].round(3)

crosswalked_extraction.loc[crosswalked_extraction['note_modeler'].isnull(), 'note_modeler'] = ""

crosswalked_extraction['note_modeler'] = crosswalked_extraction['note_modeler'].astype(str) + \
    "| crosswalked from data threshold (" + crosswalked_extraction['thresh_lower'].astype(str) + \
    "," + crosswalked_extraction['thresh_upper'].astype(str) + ") using xwalk coeff " + \
    crosswalked_extraction['xwalk'].astype(str)

# now append back on data points that were not crosswalked
crosswalked_extraction = crosswalked_extraction.append(xwalk1)

# get rid of some columns that do not need to be updated
crosswalked_extraction = crosswalked_extraction.drop(['thresh_lower', 'thresh_upper', 'xwalk', 'xwalk_logit'], axis=1)

# FIXME: Should just remove cv_passive 
crosswalked_extraction.loc[crosswalked_extraction['cv_passive'] == 1, 'is_outlier'] = 1 

# Export crosswalked extraction sheet
crosswalked_extraction.to_csv(FILENAME, encoding="utf-8", index=False)

# Export bundle_ids
for bundle in crosswalked_extraction.bundle_id.values:
    bundle_data = crosswalked_extraction.query("bundle_id == @bundle")
    bundle_data.to_csv(FILENAME, encoding="utf-8", index=False)
