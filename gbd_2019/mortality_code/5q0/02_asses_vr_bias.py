################################################################################
## Description: Calculates under-5 VR completeness
################################################################################
import argparse

import pandas as pd
import numpy as np
import statsmodels.api as sm

from db_queries import get_location_metadata


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id to run')
args = parser.parse_args()
version_id = args.version_id

# Set directories
output_dir = "FILEPATH{}".format(version_id)
location_file = "{}FILEPATH".format(output_dir)
input_data_file = "{}/data/input_5q0.csv".format(output_dir)
output_data_file = "{}/data/vr_bias_assessed_5q0.csv".format(output_dir)

data = pd.read_csv(input_data_file)
location_hierarchy = pd.read_csv(location_file)

data['log10.sd.q5'] = data['sd']

# Remove outliers and shock years
data = data.loc[(data['outlier'] != 1)]

# Rename columns
new_name = {'upload_5q0_data_id': 'ptid', 'viz_year': 'year', 'mean': 'q5'}
data = data.rename(columns=new_name)

data.loc[(data['method_id'] == 1) & (data['source_name'].isin(["Standard DHS", "MICS"])), 'method_id'] = 3

# Set type
data.loc[data['method_id'] == 3, 'type'] = "direct"
data.loc[data['method_id'] == 4, 'type'] = "indirect"
data.loc[data['method_id'] == 10, 'type'] = "indirect, mac only"


data.loc[(data['nid'] == 135570), 'source'] = "ZAF RapidMortality Report 2011 - based on VR"

data.loc[(data['nid'] == 106649), 'source'] = "VR-SSA"
data.loc[(data['location_id'].isin([196])) &
           (data['source'] == "vr") &
           (data['year'] <= 2002.5), "source"] = "VR_pre2002"
data.loc[(data['location_id'].isin([196])) &
           (data['source'] == "vr") &
           (data['year'] > 2002.5), "source"] = "VR_post2002"


# Make a VR dummy variable to indicate which observations are "vr"
data['vr'] = 0
data.loc[(data['source'].str.contains("VR", case = False)) |
           (data['source'].str.contains("vital registration", case = False)) |
           (data['source'].str.contains("DSP", case = False)) |
           (data['source'].str.contains("SRS", case = False)) |
           (data['source'].str.contains("MCHS", case = False)) |
           (data['source'].str.contains("Maternal and Child Health Surveillance", case = False)),
         'vr'] = 1

# Categorize data sources
data['category'] = np.nan
data.loc[((data['source'].str.contains("DHS", case = False)) |
            (data['source'].str.contains("demographic and health", case = False))) &
           (data['type'] == "direct"),
         'category'] = "dhs direct"
data.loc[((data['source'].str.contains("DHS", case = False)) |
            (data['source'].str.contains("demographic and health", case = False))) &
           ((data['type'] == "indirect")|(data['type']=="indirect, MAC only")),
         'category'] = "dhs indirect"

data.loc[(data['source'].str.contains("WFS", case = False))|
           (data['source'].str.contains("world", case = False)),
         'category'] = "wfs"

data.loc[((data['source'].str.contains("MICS", case = False))|
            (data['source'].str.contains("multiple indicator cluster", case = False)))&
           ((data['type'] == "indirect")|(data['type']=="indirect, MAC only")),
         'category'] = "mics"

data.loc[(data['source'].str.contains("census", case = False))&
           ((data['type'] == "indirect")|(data['type']=="indirect, MAC only")),
         'category'] = "census"

data.loc[(data['source'].str.contains("CDC", case = False))&
           ((data['type'] == "indirect")|(data['type']=="indirect, MAC only")),
         'category'] = "cdc"

data.loc[data['vr'] == 1, 'category'] = "vr"

data.loc[data['category'].isnull(), 'category'] = "other"

data.loc[data['q5'] == 0, 'q5'] = 0.0001

data.loc[data['nid'] == 118382, 'source'] = "vr"

data.loc[(data['ihme_loc_id'] == "SYR") & (data['vr'] == 1) & (data['year'] >= 2000), 'source'] = "vr_post2000"
data.loc[(data['ihme_loc_id'] == "GUY") & (data['vr'] == 1) & (data['year'] >= 1965), 'source'] = "vr_post1965"
data.loc[(data['ihme_loc_id'] == "KAZ") & (data['vr'] == 1) & (data['year'] >= 2010), 'source'] = "vr_post2010"
data.loc[(data['ihme_loc_id'] == "THA") & (data['vr'] == 1) & (data['year'] >= 2010), 'source'] = "vr_post2010"
data.loc[(data['ihme_loc_id'] == "KOR") & (data['vr'] == 1) & (data['year'] >= 1999), 'source'] = "vr_post2002"
data.loc[(data['ihme_loc_id'] == "KGZ") & (data['vr'] == 1) & (data['year'] >= 1988), 'source'] = "vr_post1988"
data.loc[(data['ihme_loc_id'] == "KGZ") & (data['vr'] == 1) & (data['year'] >= 2007), 'source'] = "vr_post2007"
data.loc[(data['ihme_loc_id'] == "NIC") & (data['vr'] == 1) & (data['year'] >= 2008), 'source'] = "vr_post2008"
data.loc[(data['ihme_loc_id'] == "ECU") & (data['vr'] == 1) & (data['year'] >= 1995), 'source'] = "vr_post1995"
data.loc[(data['ihme_loc_id'] == "GEO") & (data['vr'] == 1) & (data['year'] >= 2003), 'source'] = "vr_post2003"
data.loc[(data['ihme_loc_id'] == "HND") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "DOM") & (data['vr'] == 1) & (data['year'] >= 1985), 'source'] = "vr_post1985"
data.loc[(data['ihme_loc_id'] == "ARM") & (data['vr'] == 1) & (data['year'] >= 2006), 'source'] = "vr_post2006"
data.loc[(data['ihme_loc_id'] == "TJK") & (data['vr'] == 1) & (data['year'] >= 1995), 'source'] = "vr_post1995"
data.loc[(data['ihme_loc_id'] == "TJK") & (data['vr'] == 1) & (data['year'] >= 2008), 'source'] = "vr_post2008"
data.loc[(data['ihme_loc_id'] == "UZB") & (data['vr'] == 1) & (data['year'] >= 1995), 'source'] = "vr_post1995"
data.loc[(data['ihme_loc_id'] == "ALB") & (data['vr'] == 1) & (data['year'] >= 1996), 'source'] = "vr_post1996"
data.loc[(data['ihme_loc_id'] == "COL") & (data['vr'] == 1) & (data['year'] >= 1980), 'source'] = "vr_post1980"
data.loc[(data['ihme_loc_id'] == "EGY") & (data['vr'] == 1) & (data['year'] >= 2010), 'source'] = "vr_post2010"
data.loc[(data['ihme_loc_id'] == "IRQ") & (data['vr'] == 1) & (data['year'] >= 2000), 'source'] = "vr_post2000"
data.loc[(data['ihme_loc_id'] == "JOR") & (data['vr'] == 1) & (data['year'] >= 1967), 'source'] = "vr_post1967"
data.loc[(data['ihme_loc_id'] == "JOR") & (data['vr'] == 1) & (data['year'] >= 2000), 'source'] = "vr_post2000"
data.loc[(data['ihme_loc_id'] == "LBY") & (data['vr'] == 1) & (data['year'] >= 1990), 'source'] = "vr_post1990"
data.loc[(data['ihme_loc_id'] == "LBY") & (data['vr'] == 1) & (data['year'] >= 2000), 'source'] = "vr_post2000"
data.loc[(data['ihme_loc_id'] == "TUR") & (data['vr'] == 1) & (data['year'] >= 2006), 'source'] = "vr_post2006"
data.loc[(data['ihme_loc_id'] == "PHL") & (data['vr'] == 1) & (data['year'] >= 1965), 'source'] = "vr_post1965"
data.loc[(data['ihme_loc_id'] == "PHL") & (data['vr'] == 1) & (data['year'] >= 1988), 'source'] = "vr_post1988"
data.loc[(data['ihme_loc_id'] == "UKR") & (data['vr'] == 1) & (data['year'] >= 1998), 'source'] = "vr_post1998"
data.loc[(data['ihme_loc_id'] == "MEX_4652") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4654") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4655") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4658") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4667") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4669") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4670") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'] == "MEX_4672") & (data['vr'] == 1) & (data['year'] >= 2005), 'source'] = "vr_post2005"
data.loc[(data['ihme_loc_id'].str.match("MEX_")) & (data['vr'] == 1) & (data['year'] >= 2010), 'source'] = "vr_post2010"
data.loc[(data['ihme_loc_id'].str.match("BRA")) & (data['vr'] == 1) & (data['year'] >= 2000), 'source'] = "vr_post2000"
data.loc[(data['ihme_loc_id'].str.match("CHN")) &
           (~data['ihme_loc_id'].isin(['CHN_354', 'CHN_361'])) &
           (data['source_name'] == "DSP") &
           (data['vr'] == 1) & (data['year'] >= 2004), 'source'] = "DSP_post2004"
data.loc[(data['ihme_loc_id'] == "BGD") & (data['source'].str.contains("SRS", case = False)), 'source'] = "SRS"

data['source'] = data['source'].str.lower()


####################
# Determine VR Bias Countries
###################
data.to_csv("{}/data/pre_bias_assessment.csv".format(output_dir), index=False)
location_ids = data['location_id'].drop_duplicates()
for location_id in location_ids:
  # Get list of VR sources in this location
  vr_sources = data.loc[(data['vr'] == 1) &
                          (data['location_id'] == location_id),
                        'source'].drop_duplicates()
for src in vr_sources:
  other_years = data.loc[(data['vr'] == 0) &
                           (data['location_id'] == location_id)
                         ].copy(deep=True)
# If there is only vr
if len(other_years) == 0:
  data.loc[(data['vr'] == 1) &
             (data['location_id'] == location_id) &
             (data['source'] == src),
           'category'] = "vr_only"
else:
  # Get the start and end years of the source and the non-VR
  vr_years = data.loc[(data['source'] == src) &
                        (data['location_id'] == location_id)].copy(deep=True)
vr_year_start = int(vr_years['year'].min())
vr_year_end = int(vr_years['year'].max())
vr_year_range = set(range(vr_year_start, vr_year_end + 1))

other_year_start = int(other_years['year'].min())
other_year_end = int(other_years['year'].max())
other_year_range = set(range(other_year_start, other_year_end + 1))

# Skip VR systems where there is no overlap with other sources
if bool(other_year_range & vr_year_range):
  # Run a regression of 5q0 on year with VR dummy variable
  regression_data = data.loc[(data['location_id'] == location_id) &
                               ((data['vr'] == 0) | (data['source'] == src))
                             ].copy(deep=True)
regression_data['_intercept'] = 1
result = sm.OLS(np.log10(regression_data['q5']), regression_data[['year', 'vr', '_intercept']], missing = 'drop').fit()
# If the p-value for the VR dummy is significant, then it's biased
data.loc[(data['source'] == src) &
           (data['location_id'] == location_id),
         'biascoef'] = result.params['vr']
p_value = result.pvalues['vr']
if p_value < 0.05:
  data.loc[(data['source'] == src) &
             (data['location_id'] == location_id),
           'category'] = "vr_biased"
else:
  data.loc[(data['source'] == src) &
             (data['location_id'] == location_id),
           'category'] = "vr_unbiased"
else:
  data.loc[(data['source'] == src) &
             (data['location_id'] == location_id),
           'category'] = 'vr_no_overlap'


############################################
## Exceptions
############################################
#Three categories of (biased or corrected in some way) data here:
#1) Data that is biased and gets corrected - gets both types of variance added on  (to_correct = 1)
#2) Data that is unbiased doesn't get corrected, but runs through the correction code for the additional bias variance and also gets add'l source.type variance
#3) Data that is no-overlap but maybe biased, so can't go through the correction code, but gets add'l source.type variance (FSM, OMN, CarI)
#AND
#4) Data that is unbiased and receives no correction or additional variance

# actually run correction and adjust upwards: to_correct = 1
# add bias from correction code: corr_code_bias = 1
# source-type variance category: category = "biased" OR "unbiased"

#FIRST: variance category corrections
########################
data.loc[(data['category'] == "vr_biased") & (data['biascoef'] >= 0) & (data['vr'] == 1), 'category'] = "vr_unbiased"
data['biascoef'] = np.nan
data.loc[(data['ihme_loc_id'].isin(["SRB"])) & (data['vr'] == 1), 'category'] = "vr_unbiased"
data.loc[(data['ihme_loc_id'].isin(["LBY"])) & (data['vr'] == 1), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'].isin(["ZAF_483", "ZAF_484", "ZAF_487", "ZAF_488", "ZAF_489", "ZAF_490"])) & (data['vr'] == 1), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'].isin(["RUS"])) & (data['source'] == "vr"), 'category'] = "vr_unbiased"
data.loc[(data['ihme_loc_id'].isin(["COL", "PAN", "ALB", "ZAF_486", "JOR", "TKM"])) & (data['vr'] == 1), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'] == "UKR") & (data['source'] == "vr"), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'].isin(["KOR"])) & (data['source'] == "vr"), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'].isin(["BWA"])) & (data['source'] == "vr"), 'category'] = "vr_biased"

###################################################
#Everything that is "vr_biased" before here actually gets corrected upwards
#If to_correct is true and data is "overcomplete" [above most data], no bias variance is added
data["to_correct"] = False
data.loc[data['category'] == "vr_biased", "to_correct"] = True

##################################################
####Now switch countries that don't get corrected, but get run through code for variance
data.loc[((data['ihme_loc_id'].isin(["PAK", "BGD", "IND", "CHN_44533"])) |
            ((data['ihme_loc_id'].str.match("CHN")) &
               ~(data['ihme_loc_id'].isin(["CHN_354", "CHN_361"]))) |
            (data['ihme_loc_id'].str.match("IND"))) &
           (data['vr'] == 1), "category"] = "vr_biased"
data.loc[((data['ihme_loc_id'].isin(["PAK", "BGD", "IND", "CHN_44533"])) |
            ((data['ihme_loc_id'].str.match("CHN")) &
               ~(data['ihme_loc_id'].isin(["CHN_354", "CHN_361"]))) |
            (data['ihme_loc_id'].str.match("IND"))) &
           (data['vr'] == 1), "to_correct"] = False

data.loc[(data['ihme_loc_id'] == "IND") & (data['source'] == "vr"), "to_correct"] = True

data.loc[(data['ihme_loc_id'] == "IRQ") & (data['source'].str.contains("SRS", case = False)), "category"] = "vr_biased"
data.loc[(data['ihme_loc_id'] == "IRQ") & (data['source'].str.contains("SRS", case = False)), "to_correct"] = False


####################################################
#Correction code assignment
data['corr_code_bias'] = False
data.loc[data['category'] == "vr_biased", 'corr_code_bias'] = True
data.loc[data['source'].str.contains("Maternal and Child Health Surveillance|MCHS", case = False), 'corr_code_bias'] = False

####################################################
#Assign no overlap and vr only countries to be biased or unbiased (can't go through correction code, but add st variance to biased)
#and print names that are still unassigned to these two categories
#1) find countries that are no overlap or vr only
#2) assign to 'vr_biased' or 'vr_unbiased'
data.loc[(data['category'].isin(["vr_no_overlap", "vr_only"])) &
           (data['source'] == "vr") &
           (data['ihme_loc_id'].isin(["MNE", "ARE"])),
         'category'] = "vr_unbiased"
data.loc[(data['ihme_loc_id'] == "KOR") &
           (data['source'] == "vr_post2002"),
         'category'] = "vr_unbiased"
data.loc[(data['ihme_loc_id'] == "ZAF") &
           (data['source'].str.contains("rapidmortality")),
         'category'] = "vr_unbiased"
data.loc[(data['category'].isin(["vr_no_overlap", "vr_only"])) &
           (data['source'] == "vr") &
           (data['ihme_loc_id'].isin(["FSM", "OMN"])),
         'category'] = "vr_biased"
caribbean_island_ids = location_hierarchy.loc[location_hierarchy['region_name'] == "CaribbeanI", "location_id"]
data.loc[(data['category'] == "vr_only") &
           (data['location_id'].isin(caribbean_island_ids)),
         'category'] = "vr_biased"

data.loc[(data['ihme_loc_id'] == "ZAF") & (data['nid'] == 106649), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'] == "KGZ") & (data['source'].isin(["vr", "vr_post1988"])), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'] == "ZAF_486") & (data['source'] == "vr"), 'category'] = "vr_biased"
data.loc[(data['ihme_loc_id'].str.match("IRN")) & (data['vr'] == 1) & (data['year'] >= 2015), 'category'] = "vr_unbiased"


# All other vr-only sources are assigned to complete
data.loc[(data['category'] == "vr_only"), 'category'] = "vr_unbiased"



valid_categories = ["dhs", "dhs direct", "dhs indirect", "census", "mics",
                    "cdc", "other", "vr_biased", "vr_unbiased", "vr_no_overlap",
                    "papfam", "wfs"]
bad_data = data.loc[~(data['category'].isin(valid_categories))].copy(deep=True)
if len(bad_data) > 0:
  msg = ["The following country/sources need to be assigned to vr biased or vr unbiased"]
bad_data = bad_data[['location_id', 'ihme_loc_id', 'source', 'category']].drop_duplicates()
for i in bad_data.index:
  location_id = bad_data.loc[i, 'location_id']
ihme_loc_id = bad_data.loc[i, 'ihme_loc_id']
source = bad_data.loc[i, 'source']
category = bad_data.loc[i, 'category']
row_msg = ["location_id: ", str(location_id), ", ihme_loc_id: ", ihme_loc_id,
           ", source: ", source, ", category: ", category]
msg.append("".join(row_msg))
raise ValueError("\n".join(msg))


####################################################
## Keeping Relevant Variables and Saving the Dataset
####################################################
data = data.rename(columns={"q5": "mort"})

keep_cols = ["location_id", "year", "mort", "source", "source_year", "type",
             "vr", "category", "corr_code_bias", "to_correct",
             "log10.sd.q5", "ptid"]
data = data[keep_cols]

# Write data
data.to_csv(output_data_file, index=False)
