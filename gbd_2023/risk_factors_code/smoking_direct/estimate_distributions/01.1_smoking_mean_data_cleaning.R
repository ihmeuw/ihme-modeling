# Introduction ------------------------------------------------------------
# Purpose: Apply necessary changes to data utilized in the tobacco pipeline
#             - Usually this is overlapping or duplicated age or sex sets due to over-extractions
# Date Modified: 2024-11-14

message(paste0("Cleaning init/cess/amt data from ", tobdf, ". When a new best dataset or new_data is utilized, these codes can be archived and created again to avoid duplicates, etc."))

if (tobdf=="new_data"){
  df <- df[!(nid %in% c(20663))]
  df[nid == 20663 & year_start == 2010 & year_end == 2010, nid := 270469]
  df <- df[!(survey_name=="MACRO_DHS" & age_start>10 & survey_module=="BR")]
  df <- df[!(nid==25914 & survey_module=="CH")]
  df <- df[!(nid %in% c(76702, 80733, 194011) & mean<1)]

message("Done cleaning new data!")
}

######################

if (tobdf=="init_bun"){
  bundle_data_init <- bundle_data_init %>% group_by(across(any_of(c("nid", "ihme_loc_id", "location_id", "sex_id", "sex", "age_start", "year_start", "var")))) %>% 
                      dplyr::mutate(ranklow = order(order(val, decreasing=F))) %>% 
                      filter(ranklow==1) %>% 
                      dplyr::select(-ranklow)
    
  
message("Done cleaning init bundle!")
}

######################

if (tobdf=="init"){
  micro_2023_init <- micro_2023_init[!(nid %in% c(281642, 265055, 106684, 8167, 106684, 200838, 380182, 380135, 39999, 162317, 218613))]
  micro_2023_init <- micro_2023_init[!(location_id == 215 & age_group_id == 16 & sex == "Male")]
  micro_2023_init <- micro_2023_init[!(location_id == 215 & age_group_id == 10 & sex == "Female")]
  micro_2023_init <- micro_2023_init[!(nid == 21988 & location_id == 4846 & sex == "Female" & age_group_id == 20)]
  micro_2023_init <- micro_2023_init[!(nid == 8555 & year_id == 1999 & sex == "Female" & age_group_id == 19)]
  micro_2023_init <- micro_2023_init[!(nid == 398120 & age_group_id == 15)]
  micro_2023_init <- micro_2023_init[!(nid == 292644 & age_group_id == 14 & sex == "Female")]
  micro_2023_init <- micro_2023_init[!(nid == 325316 & sex == "Female" & age_group_id == 21)]
  micro_2023_init <- micro_2023_init[!(nid == 236205 & sex == "Female" & age_group_id == 19)]
  
message("Done cleaning new init data!")
}

######################

if (tobdf=="cess_bun"){
  bundle_data_cess <- bundle_data_cess %>% group_by(across(any_of(c("nid", "ihme_loc_id", "location_id", "sex_id", "sex", "age_start", "year_start", "var")))) %>% 
    dplyr::mutate(rankhigh = order(order(val, decreasing=T))) %>% 
    filter(rankhigh==1) %>% 
    dplyr::select(-rankhigh)
  
  
message("Done cleaning cess bundle!")
}

######################

if (tobdf=="cess"){
  micro_2023_cess <- micro_2023_cess[!(nid %in% c(47962, 233990, 48332))]
  micro_2023_cess <- micro_2023_cess[!(nid == 108817 & age_group_id == 17 & sex == "Female" & year_id == 2007)]
  micro_2023_cess <- micro_2023_cess[!(location_id == 6)]
  micro_2023_cess <- micro_2023_cess[!(nid %in% c(50183, 50205))]
  
message("Done cleaning new cess data!")
}

######################

if (tobdf=="amt_bun"){
  bundle_data_amt <- bundle_data_amt %>% group_by(across(any_of(c("nid", "ihme_loc_id", "location_id", "sex_id", "sex", "age_start", "year_start", "var")))) %>% 
    dplyr::mutate(rankhigh = order(order(val, decreasing=T))) %>% 
    filter(rankhigh==1) %>% 
    dplyr::select(-rankhigh)
  
}
  ######################
  
if (tobdf=="amt"){
    
  micro_2023_amt <- micro_2023_amt[!(nid %in% c(155791, 265055, 95336, 118473, 118474, 118476, 118483, 118491, 118493, 118495, 118478, 118486, 118487, 
                          96208, 118472, 118475, 118480, 118481, 118482, 118484, 118490, 118496, 118499, 49205, 47962, 47478, 110300, 165892, 25914, 48332, 
                          110300, 118498, 319361, 157064, 218613, 294139, 118494, 118477, 118479, 118485, 52110, 118497, 118488, 30095, 22286, 22293, 
                          22306, 22317, 22329, 121437, 126592, 218565, 237333, 260403, 93487))]
  micro_2023_amt <- micro_2023_amt[!(location_id == 126 & age_group_id %in% c(12, 14) & sex == "Male" & year_id == 2015)]
  micro_2023_amt <- micro_2023_amt[!(location_id == 126 & age_group_id %in% c(15) & sex == "Female" & year_id == 2015)]
  micro_2023_amt <- micro_2023_amt[!(location_id == 108 & sex == "Female" & nid == 264910)]
  micro_2023_amt <- micro_2023_amt[!(nid == 218581 & sex == "Female" & year_id == 2015 & age_group_id %in% c(10, 11))]
  micro_2023_amt <- micro_2023_amt[!(location_id == 190 & sex == "Female" & val > 10)]
  micro_2023_amt <- micro_2023_amt[!(location_id == 130 & nid == 111486 & year_id == 2009 & age_group_id == 20)]
  micro_2023_amt <- micro_2023_amt[!(nid == 81030 & location_id == 147 & age_group_id %in% c(10, 15, 16, 17) & sex == "Male")]
  micro_2023_amt <- micro_2023_amt[!(nid == 132625 & year_id == 2013 & location_id == 151 & age_group_id == 14 & sex == "Female")]
  micro_2023_amt <- micro_2023_amt[!(location_id == 155 & sex == "Male" & age_group_id == 19 & year_id == 2012 & nid == 130055)]
  micro_2023_amt <- micro_2023_amt[!(nid == 108817 & age_group_id == 16 & year_id == 2007 & location_id == 26 & sex == "Female")]
  micro_2023_amt <- micro_2023_amt[!(nid == 287143 & age_group_id %in% c(11, 14) & sex == "Female" & year_id == 2015)]
  micro_2023_amt <- micro_2023_amt[!(nid == 286785 & location_id == 19 & sex == "Female" & year_id == 2016)]
  micro_2023_amt <- micro_2023_amt[!(nid == 108823 & location_id == 215 & sex == "Male" & val > 10 & age_group_id == 13)]
  micro_2023_amt <- micro_2023_amt[!(nid == 237375 & location_id == 216 & sex == "Male" & year_id == 2015 & age_group_id == 15)]
  micro_2023_amt <- micro_2023_amt[!(nid == 325316 & location_id %in% c(4872))]
  micro_2023_amt <- micro_2023_amt[!(nid == 352861 & sex == "Male" & age_group_id %in% c(15, 16, 17))]
  micro_2023_amt <- micro_2023_amt[!(nid == 155298 & sex == "Male" & age_group_id %in% c(12, 14, 15, 18))]
  micro_2023_amt <- micro_2023_amt[!(nid == 237236 & age_group_id == 18 & sex == "Female")]
  micro_2023_amt <- micro_2023_amt[!(nid == 351361 & age_group_id %in% c(15, 16, 17) & sex == "Male")]
  micro_2023_amt <- micro_2023_amt[!(nid == 30191 & sex == "Female")]
  
message("Done cleaning new amt data!")
}
