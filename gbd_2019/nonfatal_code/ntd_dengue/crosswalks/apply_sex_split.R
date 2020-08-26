# Chagas Crosswalker






os <- .Platform$OS.type
if (os=="windows") {
   <-"FILEPATH"
   <-"FILEPATH"
} else {
   <-"FILEPATH"
   <-paste0("homes/", Sys.info()[7], "/")
}

library(metafor, lib.loc = "FILEPATH")
library(msm)
library(data.table)
library(ggplot2)
library(openxlsx)
#source(paste0(FILEPATH"))
#source(paste0(FILEPATH"))
#source(paste0(FILEPATH"))
#source(paste0(FILEPATH"))


source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source("FILEPATH")

# MR-BRT

repo_dir <- paste0(FILEPATH")
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

# Custom


#############################################################################################
###                                      Crosswalks                                       ###
#############################################################################################

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')

#source the apply sex crosswalk function 
source("FILEPATH")




#'[Sex Crosswalk]
#the mansoni fit results from decomp 2

fit1_mansoni <- readRDS("FILEPATH")

readxl::excel_sheets("FILEPATH")
df <- readxl::read_xlsx("FILEPATH", sheet = 'extraction')

df$underlying_nid[df$underlying_nid=="."] <- NA


library(data.table)
dt<- as.data.table(df)


agg_data_mansoni<- subset(dt, case_name=="S mansoni" | case_name=="S intercalatum"| case_name=="S mekongi" |  case_name=="S guineesis")

openxlsx::write.xlsx(agg_data_mansoni, file = 'FILEPATH')


data_sex_adj_mansoni <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_mansoni, all_data = agg_data_mansoni, decomp_step = "step4")

openxlsx::write.xlsx(data_sex_adj_mansoni, file = 'FILEPATH')

###HEMATOBIUM###

#the hema fit results
fit1_hema <- readRDS("FILEPATH")

agg_data_hema<- subset(dt, case_name=="S haematobium")
#1061
openxlsx::write.xlsx(agg_data_hema, file = 'FILEPATH')


data_sex_adj_hema <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_hema, all_data = agg_data_hema, decomp_step = "step4")

openxlsx::write.xlsx(data_sex_adj_hema, file = 'FILEPATH')

###JAPONICUM####

fit1_japon <- readRDS("FILEPATH")

agg_data_japon<- subset(dt, case_name=="S japonicum")
openxlsx::write.xlsx(agg_data_japon, file = 'FILEPATH')

data_sex_adj_japon <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_japon, all_data = agg_data_japon, decomp_step = "step4")
openxlsx::write.xlsx(data_sex_adj_japon, file = 'FILEPATH')





final_sex_split_data <- rbind(data_sex_adj_mansoni, data_sex_adj_hema, data_sex_adj_japon)
#apply ceilings to upper and lower (no mena higher than 1 but just writing this in case)
final_sex_split_data$mean[final_sex_split_data$mean>1] <- 1
final_sex_split_data$upper[final_sex_split_data$upper>1] <- 1
final_sex_split_data$lower[final_sex_split_data$lower<0] <- 0

#saving it as a flat file
openxlsx::write.xlsx(final_sex_split_data, sheetName = "extraction", file = "FILEPATH")

