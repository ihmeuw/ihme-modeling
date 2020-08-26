# sex crosswalk 

## SET UP FOCAL DRIVES

rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"FILEPATH"
  ADDRESS <-"FILEPATH"
} else {
  ADDRESS <-"FILEPATH"
  ADDRESS <-paste0("FILEPATH", Sys.info()[7], "/")
}

library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(data.table)
library(ggplot2)
library(openxlsx)


source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source("FILEPATH")

# MR-BRT

repo_dir <- paste0(ADDRESS, "FILEPATH")
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))



#############################################################################################
###                                      Set-Up                                           ###
#############################################################################################


##' [Set-up run directory]

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]

crosswalks_dir    <- paste0(run_dir, "FILEPATH")


tracker <- fread(paste0(crosswalks_dir, "FILEPATH"))
tracker[, date := NULL]
as_row <- data.table(bid = ADDRESS, bv = as_bv_md$bundle_version_id, cv = NA, note = "Actual Age-Split Root")
tracker <- rbind(tracker, as_row)
tracker[, date := Sys.Date()]
fwrite(tracker, paste0(crosswalks_dir, "FILEPATH"))


tracker <- fread(paste0(crosswalks_dir, "FILEPATH"))

#############################################################################################
###                                      Crosswalks                                       ###
#############################################################################################
source("FILEPATH")

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')



#'[Sex Crosswalk]
#the mansoni fit results
fit1_mansoni <- readRDS(paste0(crosswalks_dir, "FILEPATH"))


###all_data is the whole data set and we will do the following:
#1. subset by case name
#2. clean for dx
#3. include both sex and sex specific data here


all_data<- subset(all_data, sample_size!=cases)

#cleaning
all_data$mean[all_data$cases==0] <- 0
all_data[, age_start := round(age_start)]
all_data[, age_end := round(age_end)]
all_data$year_start[all_data$year_start==1889] <- 1989
all_data$year_end[all_data$year_end==1889] <- 1989


dat_original_mansoni<- subset(all_data, case_name=="S mansoni" | case_name=="S intercalatum"| case_name=="S mekongi")

dat_original_mansoni$case_diagnostics <- as.character(dat_original_mansoni$case_diagnostics)
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz, FEC" | dat_original_mansoni$case_diagnostics=="Kato-Katz, IHA" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation, serology" |  dat_original_mansoni$case_diagnostics=="Kato-Katz"] <- "Kato-Katz"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="sedimentation" | dat_original_mansoni$case_diagnostics=="saline sedimentation technique for protozoan and intestinal parasites; digestion technique to determine the intensity of infection"] <- "sed"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="fecal smear, formol ether" | dat_original_mansoni$case_diagnostics=="fecal smear"] <- "Kato-Katz"

dat_original_mansoni<- subset(dat_original_mansoni, dat_original_mansoni$case_diagnostics != "centrifugation" & dat_original_mansoni$case_diagnostics != "FLOTAC" & dat_original_mansoni$case_diagnostics != "gold" & dat_original_mansoni$case_diagnostics!="MFIC" & dat_original_mansoni$case_diagnostics != "" & dat_original_mansoni$case_diagnostics != "CCA or Kato-Katz")

dat_original_mansoni$num_samples <- as.double(dat_original_mansoni$num_samples)

#renaming values under case_diagnostics

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==1] <- "kk1"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==2] <- "kk2"
#
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==3] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$num_samples==4] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="ELISA"] <- "elisa"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="PCR"] <- "pcr"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="CCA"] <- "cca"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="FEC"] <- "fec"

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz"] <- "kk1"


unique(dat_original_mansoni$case_diagnostics)

length(which(dat_original_mansoni$case_diagnostics == "kk1")) 
length(which(dat_original_mansoni$case_diagnostics == "kk2")) 
length(which(dat_original_mansoni$case_diagnostics == "kk3")) 
length(which(dat_original_mansoni$case_diagnostics == "cca")) 
length(which(dat_original_mansoni$case_diagnostics == "pcr")) 
length(which(dat_original_mansoni$case_diagnostics == "elisa")) 
length(which(dat_original_mansoni$case_diagnostics == "sed")) 
length(which(dat_original_mansoni$case_diagnostics == "fec")) 
length(which(dat_original_mansoni$case_diagnostics == "NA")) 
length(which(dat_original_mansoni$case_diagnostics == "")) 



data_sex_adj_mansoni <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_mansoni, all_data = dat_original_mansoni, decomp_step = "step2")

write.csv(data_sex_adj_mansoni, file = "FILEPATH")



final_sex_split_data <- rbind(data_sex_adj_mansoni, data_sex_adj_hema, data_sex_adj_japon)
#apply ceiling of 1 to mean
final_sex_split_data$mean[final_sex_split_data$mean>1] <- 1
final_sex_split_data$upper[final_sex_split_data$upper>1] <- 1

#saving it as a flat file
openxlsx::write.xlsx(final_sex_split_data, sheetName = "extraction", file = paste0(crosswalks_dir, "FILEPATH"))


