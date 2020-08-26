
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

repo_dir <- paste0(j, "FILEPATH")
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
fit1_japon <- readRDS(paste0(crosswalks_dir, "FILEPATH"))


###all_data is the whole data set and we will do the following:
#1. subset by case name
#2. clean for dx
#3. include both sex and sex specific data here

all_data<- subset(all_data, sample_size!=cases)

all_data$mean[all_data$cases==0] <- 0


dat_original_japon<- subset(all_data, case_name=="S japonicum")

#dat_original_japon$case_diagnostics<- dat_original_japon$case_diagnostics
dat_original_japon$case_diagnostics <- as.character(dat_original_japon$case_diagnostics)

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" | dat_original_japon$case_diagnostics=="fecal smear" | dat_original_japon$case_diagnostics=="sedimentation"] <- "Kato-Katz"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="IHA"] <- "iha"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="nylon silk method"] <- "hatch"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="ELISA"] <- "elisa"

dat_original_japon<- subset(dat_original_japon, dat_original_japon$case_diagnostics != "NA")

dat_original_japon$num_samples <- as.double(dat_original_japon$num_samples)

#renaming values under case_diagnostics

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==1] <- "kk1"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==2] <- "kk2"


dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==3] <- "kk3"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==4] <- "kk3"

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz"] <- "kk1"


unique(dat_original_japon$case_diagnostics)

length(which(dat_original_japon$case_diagnostics == "kk1")) 
length(which(dat_original_japon$case_diagnostics == "kk2")) 
length(which(dat_original_japon$case_diagnostics == "kk3")) 
length(which(dat_original_japon$case_diagnostics == "iha")) 
length(which(dat_original_japon$case_diagnostics == "hatch")) 
length(which(dat_original_japon$case_diagnostics == "elisa")) 



data_sex_adj_japon <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_japon, all_data = dat_original_japon, decomp_step = "step2")

write.csv(data_sex_adj_japon, file = "FILEPATH")

