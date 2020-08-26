# Chagas Crosswalker
# Authors : Taren Gorman, Chase Gottlich
# Credits : Carrie Purcell, Jorge Ledesma

## SET UP FOCAL DRIVES


os <- .Platform$OS.type
if (os=="windows") {
   <-"FILEPATH"
  ADDRESS <-"FILEPATH"
} else {
   <-"FILEPATH"
  ADDRESS <-paste0("FILEPATH", Sys.info()[7], "/")
}

library(metafor, lib.loc = "FILEPATH")
library(msm)
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
source(paste0(repo_dir, "FILEPATH")
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

#sourcing the apply sex crosswalk function written by Chase
source("FILEPATH")



#'[Sex Crosswalk]

fit1_dengue <- readRDS("FILEPATH")


df<- dedup_dengue_data


library(data.table)
dt<- as.data.table(df)


data_sex_adj_dengue <- apply_sex_crosswalk(mr_brt_fit_obj = fit1_dengue, all_data = dt, decomp_step = "step4")

#dropping source types - not reliable
data_sex_adj_dengue<- subset(data_sex_adj_dengue, source_type!="Unidentifiable")
data_sex_adj_dengue<- subset(data_sex_adj_dengue, source_type!="News report")
#11612 observations

#dropping sex id from here cause the age split code does not work with this column (coming up next)

data_sex_adj_dengue_final<- subset(data_sex_adj_dengue, select = -c(sex_id))


#adding lcoation metadata


openxlsx::write.xlsx(data_sex_adj_dengue_final, file = 'FILEPATH')

