
#' Age-split
#'
#' @details Age-split data points using the age pattern from a specified Dismod model. Code adapted from USERNAME.
#'
#' @params df Data frame in epi database format. Requires columns age_start, age_end, mean, lower, upper, standard_error.
#' @params model_id Modelable Entity ID (MEID) of the Dismod model you want to use for the age pattern.
#' @params decomp_step Decomp step, defaults to iterative. Should be formatted "step2" or "iterative".
#' @params measure Measure of interest. Character string such as "prevalence" or "incidence". Make sure it fits what's in DisMod.
#'
#' @return age_split_data Data frame of age-split data.
#'

#Age split angina
library(R.utils)
h <- paste0("/FILEPATH/", Sys.info()[["user"]], "/")
suppressMessages(sourceDirectory(paste0("/FILEPATH")))
source(paste0(h, "FILEPATH/age_split.R"))
#source(paste0(h, "FILEPATH/age_split.R"))

## Get bundle version angina 
bundle_id <- 115
ids_angina<- get_elmo_ids(gbd_round_id = 7, decomp_step = "iterative", bundle_id = bundle_id)
bv <- get_bundle_version(34049, fetch = 'all')
xwalk <- get_crosswalk_version(29492)

prevalence <- xwalk[measure=="prevalence"]
mtexcess <-  xwalk[measure=="mtexcess"]
#Get sex split data
#sex_split <- as.data.table(read_xlsx("/FILEPATH/angina_115_2020_07_28.xlsx"))

#sex_split <- sex_split[measure_real != "prevalence"]
#sex_split$measure <- sex_split$measure_real
#sex_split[, measure_real:=NULL]


## Get postcrosswalk prevalence data
#prev <- get_crosswalk_version(24596)

#setdiff(sex_split, prev)
#prev[, age_mid :=NULL]
#prev[, year_mid :=NULL]
#prev[, variance :=NULL]


## Append data
#data <- rbind(sex_split, prev, fill = TRUE)

## zeroes angina
#zeros <- angina[mean == 0]

#data <- rbind(data, zeros, fill = TRUE)

sex_split <- as.data.table(read_xlsx("/FILEPATH/xw_results_2020_07_21_cvd_ihd_115_all_measures.xlsx"))

#sex_split <- get_crosswalk_version(25220)
#prevalence <- sex_split[measure == "prevalence"]
prevalence <- prevalence[, group_review := 1]
prevalence <- prevalence[, group := 1]

mtexcess <- mtexcess[, group_review := 1]
mtexcess <- mtexcess[, group := 1]


age_split_prev <- age_split(df = prevalence, 
                model_id = 1817, 
                model_version_id = 512060, 
                measure = "prevalence",
                global_age_pattern = F) 



check2 <- copy(age_split_prev2[age_end - age_start > 5])
check <- copy(age_split_prev[age_end - age_start > 5])

#all <- as.data.table(read_xlsx("/FILEPATH/xw_results_2020_07_21_cvd_ihd_115_all_measures.xlsx"))
#all <- sex_split[measure != "prevalence"]

#for_modeling <- rbind(age_split_prev, all,  fill = TRUE)

#write.xlsx(for_modeling, 
#          file =  paste0("/FILEPATH/angina_115_final", date, ".xlsx"),
#                     sheetName = "extraction", showNA = FALSE)

#wzeros <- rbind(for_modeling, zeros, fill = TRUE)



#write.xlsx(wzeros, 
#           file =  paste0("/FILEPATH/angina_115_wzeros_", date, ".xlsx"),
#           sheetName = "extraction", showNA = FALSE)

save_crosswalk_result_file <- paste0("/FILEPATH/angina_115_", date, ".xlsx")
#save_crosswalk_result_file  <- "/FILEPATH/xw_results_2020_07_21_cvd_ihd_115_all_measures.xlsx"

#save_crosswalk_result_file <- paste0("/FILEPATH/angina_115_final_", date, ".xlsx")
#duplicated(for_modeling$seq)
###########################################
########################################################

#Upload crosswalk version
angina <- df
angina <- as.data.table(df[measure!="prevalence"])
angina <- rbind(angina, age_split_prev, fill=TRUE)
  
#save_crosswalk_mtexcess_file <- paste0("/FILEPATH/115_mtexcess_for_agesplit_",date,".xlsx")
#write.xlsx(mtexcess, file=save_crosswalk_mtexcess_file, sheetName="extraction")
#bundle_version_id <- 34049

#description <- "mtexcess for age split continuous"
#save_crosswalk_version_result <- save_crosswalk_version(
#  bundle_version_id=bundle_version_id,
#  data_filepath=save_crosswalk_result_file,
#  description=description)


#description <- "MR-BRT angina + age split"

#save_crosswalk_version_result <- save_crosswalk_version(
#  bundle_version_id=bundle_version_id,
#  data_filepath=save_crosswalk_result_file,
#  description=description)

#########################################################
#Upload crosswalk version
#age_split_prev <- as.data.table(age_split_prev)
#all <- as.data.table(read_xlsx("/FILEPATH/xw_results_2020_07_21_cvd_ihd_115_all_measures.xlsx"))
#all <- sex_split[measure != "prevalence"]
#write.xlsx(age_split_prev, 
#          file =  paste0("/FILEPATH/angina_115_prevalence_", date, ".xlsx"),
#           sheetName = "extraction", showNA = FALSE)


angina <- df
angina <- as.data.table(df[measure!="prevalence"])
angina <- rbind(angina, age_split_prev, fill=TRUE)
angina$standard_error[(angina$standard_error >=1)] <- 0.9999
angina <- angina[, specificity:= NA] 

test <- angina[66:70, ]

save_crosswalk_age_split_file <- paste0("/FILEPATH/115_w_age_split_prevalence_2020_08_30.xlsx")
write.xlsx(angina, file=save_crosswalk_age_split_file, sheetName="extraction")
bundle_version_id <- 34049

description <- "mrbrt claims vs angina quest w/ fix2 age split prevalence"
save_crosswalk_version_result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=save_crosswalk_age_split_file,
  description=description)



### age split plots angina prevalence

#########
#Vet age split data

split <- age_split_prev
xw_inc <- prevalence
subtype <- "angina"

age_split_plots(split, xw_inc, subtype = "angina") 





