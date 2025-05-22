##############################
## Purpose: Prep for model
## Details: Unoutlier 10-14 and 50-54 data near the bounds outliered in script.
## Decisions made during the old-young data investigation. 
## 00_input_data.R.
#############################

# Outlier override for 50-54
input_data[ihme_loc_id == "TWN" & age_group_id == 15 & year_id >= 2006 &
             asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "USA" & age_group_id == 15 & year_id != 2017 &
             asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "GTM" & age_group_id == 15 & year_id > 2000 &
             asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "PER" & age_group_id == 15 & asfr_data < 0.011 & 
             asfr_data > 0, outlier := 0]
input_data[grepl("UKR", ihme_loc_id) & age_group_id == 15 & asfr_data > 0, outlier := 0]
input_data[grepl("POL", ihme_loc_id) & age_group_id == 15 & asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "WSM" & age_group_id == 15 & asfr_data < 0.2, outlier := 0]
input_data[ihme_loc_id == "LVA" & age_group_id == 15 & asfr_data < 0.2, outlier := 0]
input_data[ihme_loc_id == "DOM" & age_group_id == 15 & asfr_data < 0.1, outlier := 0]
input_data[ihme_loc_id == "MUS" & age_group_id == 15 & asfr_data < 0.001 & asfr_data > 0,
           outlier := 0]

unoutlier_nonzero_old <- c("NZL_44850", "BGR", "EST", "FIN", "IRL", "ISL",
                           "LTU", "LVA", "NZL", "NLD", "NOR", "AUS", "HND", 
                           "CHN_354", "HND", "IDN", "JOR", "KOR", "MDA",
                           "MKD", "MMR", "MNG", "PAK", "PHL", "ROU", "UZB", 
                           "MDG", "BRA", "MYS", "PAN", "QAT", "RUS", "SWE",
                           "UKR")
input_data[ihme_loc_id %in% unoutlier_nonzero_old & age_group_id == 15 
           & asfr_data > 0, outlier := 0]

# Outlier override for 10-14
input_data[grepl("BRA_", ihme_loc_id) & age_group_id == 7 & nid != 1590, outlier := 0]
input_data[ihme_loc_id == "ISR" & age_group_id == 7 & asfr_data > 0.00005, outlier := 0]
input_data[ihme_loc_id == "JPN" & age_group_id == 7 & asfr_data > 0.000025, outlier := 0]
input_data[ihme_loc_id == "MDA" & age_group_id == 7 & asfr_data > 0.00025, outlier := 0]
input_data[ihme_loc_id == "MEX" & age_group_id == 7 & asfr_data < 0.009 &
             asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "MUS" & age_group_id == 7 & asfr_data < 0.001 &
             asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "MYS" & age_group_id == 7 & asfr_data > 0.0001, outlier := 0]
input_data[ihme_loc_id == "NIC" & age_group_id == 7 & asfr_data < 0.002 &
             year_id < 1970 & asfr_data > 0, outlier := 0]
input_data[ihme_loc_id == "NLD" & age_group_id == 7 & asfr_data > 0.00001, outlier := 0]
input_data[ihme_loc_id == "PHL" & age_group_id == 7 & asfr_data > 0.0001, outlier := 0]
input_data[grepl("USA_", ihme_loc_id) & age_group_id == 7 & asfr_data > 0, outlier := 0]

unoutlier_nonzero_young <- c("AUS", "BGR", "BHR", "BHS", "BLR", "BRA", "CAN",
                             "CHE", "CHN_354", "DNK", "DOM", "EST", "FIN", 
                             "FJI", "FRA", "GTM", "IRL", "ISL", "LUX", "LTU",
                             "LVA", "MDG", "MNG", "NOR", "PAN", "PER", "PRI",
                             "ROU", "SGP", "SLV", "SUR", "THA", "TTO", "TWN",
                             "URK", "NZL")

input_data[ihme_loc_id %in% unoutlier_nonzero_young & age_group_id == 7
           & asfr_data > 0, outlier := 0]
