# Cholera CFR data prep pre-dismod
rm(list=ls())
library(data.table)
library(openxlsx)
library(reticulate)
invisible(sapply(list.files("/FILEPATH/", full.names = T), source))
source("/FILEPATH/bundle_crosswalk_collapse.R")
source("/FILEPATH/sex_split_mrbrt_weights.R")

gbd_year <- 2023
date <- format(Sys.Date(), format="%m_%d_%Y")
date <- "11_14_2024"
bundle <- 3083
bv <- 49114
cholera_bv <- get_bundle_version(bv)
adj_path <- paste0("FILEPATH",date,"/cholera/") 
dir.create(xwalk_dir,recursive=T)

############################# no age or sex splitting and no crosswalks - duplicate both sex into male and female
#############################
#############################
cholera_bv <- cholera_bv[is.na(group_review), group_review:=1]
cholera_bv$group <- 1
cholera_bv$specificity <- "temp_specificity"
cholera_bv$crosswalk_parent_seq <- NA
both_sex <- cholera_bv[sex %like% "Both"]
female <- copy(both_sex)
female$sex <- "Female"
both_sex$sex <- "Male"
crude_sexsplit <- rbind(both_sex, female)
crude_sexsplit$crosswalk_parent_seq <- crude_sexsplit$seq
crude_sexsplit$seq <- NA
cholera_bv <- cholera_bv[!seq %in% crude_sexsplit$crosswalk_parent_seq]
cholera_bv <- rbind(cholera_bv, crude_sexsplit)
cholera_bv <- cholera_bv[group_review != 0]
write.xlsx(cholera_bv, paste0(xwalk_dir, "bv_", bv, "_no_adj.xlsx"), sheetName="extraction")

result <- save_crosswalk_version(bundle_version_id=bv,
                                 data_filepath=paste0(xwalk_dir, "bv_", bv, "_no_adj.xlsx"),
                                 description="gbd23 cfr extraction update bv as-is: no age splitting, crude sex split, no crosswalk")

