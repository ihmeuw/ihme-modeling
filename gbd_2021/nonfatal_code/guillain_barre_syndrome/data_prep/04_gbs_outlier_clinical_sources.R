##################################################################################################
## Purpose: Guillian-Barre Syndrome outliering data part 2
## Creation Date: DATE
## Created by: USERNAME
##################################################################################################
#Outliering data which does not have at least 3 non-zero estimates by age, an imporvement on xwalk verison
# This was run after the bulk outlier was done for HCUP and karnataka

rm(list=ls())
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"}
}

library(mortdb, lib = "FILEPATH")
library(Hmisc)
library(data.table)
library(msm)
library(dplyr)
library(openxlsx)



##################################################################################################
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_bulk_outlier.R")

##################################################################################################
#Outliering data which does not have at least 3 non-zero estimates by age, an imporvement on xwalk verison
#Subsetting myself to nids where sum(mean != 0) > 2
crosswalk_version_id <- 19658
gbd_round_id <- 7
decomp_step <- "iterative"
date <- gsub("-", "_", Sys.Date())
bundle_id <- 278

path_to_data <- paste0("FILEPATH")
description <- "2020 GBS xwalk removing clinical data where there are less than 3 observed age groups with means not equal to 0"
crosswalk_278_version <- get_crosswalk_version(crosswalk_version_id)

test <- copy(crosswalk_278_version)


nrow(test[(mean != 0)])
nrow(test[(mean == 0)])

sum(test$is_outlier)

nids <- c()
i <- 317423

#Used in initial outliering
for(i in unique(test$nid)){
  if(((sum(test[nid == i, mean] == 0)) > 2) == T){ #If more than 90% of nid is 0 outlier
    test[nid == i & (clinical_data_type == "claims" | clinical_data_type == "inpatient"), is_outlier := 1]
    nids <- c(nids, (unique(test[nid == i, nid])))
  }
}

##The initial outliering was not severe enough so using a much more severe outliering process
#if more than 50% of the NIDs data has mean == 0. 
for(i in unique(test$nid)){
  if((sum(test[nid == i, mean] == 0)/nrow(test[nid == i,]) > 0.50)){ #If more than 50% of nid is 0 outlier
    test[nid == i & (clinical_data_type == "claims" | clinical_data_type == "inpatient"), is_outlier := 1]
    nids <- c(nids, (unique(test[nid == i, nid])))
  }
}

317423 %in% nids

myvec <- (c("nid", "field_citation_value", "mean", "is_outlier"))

View(test[test$nid %in% nids[1], ..myvec])

to_be_outliered <- test[, c("seq", "is_outlier")]


write.xlsx(to_be_outliered, file = path_to_data, sheetName="extraction")

result <- save_bulk_outlier(crosswalk_version_id = crosswalk_version_id,
                            gbd_round_id = gbd_round_id,
                            decomp_step = decomp_step,
                            filepath = path_to_data,
                            description = description
)

print(sprintf("New crosswalk Version ID with outliers: %s", result$crosswalk_version_id))