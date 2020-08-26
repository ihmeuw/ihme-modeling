# Chagas Crosswalker

## SET UP FOCAL DRIVES

rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"FILEPATH"
  ADDRESS <-"FILEPATH"
} else {
  ADDRESS <-"FILEPATH"
  ADDRESS <-paste0("homes/", Sys.info()[7], "/")
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


#############################################################################################
###                                      Crosswalks                                       ###
#############################################################################################

source("FILEPATH")

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')


#'[Sex Crosswalk]
#the hema fit results
fit1_hema <- readRDS(paste0(crosswalks_dir, "FILEPATH"))


###all_data is the whole data set and we will do the following:
#1. subset by case name
#2. clean for dx
#3. include both sex and sex specific data here

all_data<- subset(all_data, sample_size!=cases)
all_data$mean[all_data$cases==0] <- 0


dat_original_hema<- subset(all_data, case_name=="S haematobium")
dat_original_hema$case_diagnostics<- dat_original_hema$case_diagnostics
dat_original_hema$case_diagnostics <- as.character(dat_original_hema$case_diagnostics)

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="sedimentation" | dat_original_hema$case_diagnostics=="sedimentation of urine" |   dat_original_hema$case_diagnostics=="sedimentation, filtration"] <- "sed"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "centrifugation"| dat_original_hema$case_diagnostics=="sedimentation, centrifugation" |  dat_original_hema$case_diagnostics=="sedimentation, centrifugation, filtration, microscopy"] <- "cen"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "urine filtration technique" | dat_original_hema$case_diagnostics=="urine filtration technique, microscopy" | dat_original_hema$case_diagnostics=="microscopy"] <- "filt"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "haematuria"] <- "dip"

dat_original_hema<- subset(dat_original_hema, dat_original_hema$case_diagnostics != "ELISA" & dat_original_hema$case_diagnostics != "fecal smear" & dat_original_hema$case_diagnostics != "IFAT" & dat_original_hema$case_diagnostics!="IFTB" & dat_original_hema$case_diagnostics != "Kato-Katz" & dat_original_hema$case_diagnostics != "NA" & dat_original_hema$case_diagnostics != "nucleopore filtration" & dat_original_hema$case_diagnostics != "urinealysis, microscopy")

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="PCR"] <- "pcr"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="CCA"] <- "cca"

unique(dat_original_hema$case_diagnostics)

length(which(dat_original_hema$case_diagnostics == "sed")) 
length(which(dat_original_hema$case_diagnostics == "filt")) 
length(which(dat_original_hema$case_diagnostics == "dip")) 
length(which(dat_original_hema$case_diagnostics == "cca")) 
length(which(dat_original_hema$case_diagnostics == "pcr")) 
length(which(dat_original_hema$case_diagnostics == "cen")) 



data_sex_adj_hema <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_hema, all_data = dat_original_hema, decomp_step = "step2")

write.csv(data_sex_adj_hema, file = "FILEPATH")

