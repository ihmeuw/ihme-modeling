##' ***************************************************************************
##' Title: 1_run_pooling.R
##' Purpose: Compile other hemoglobinopathies: Get CoD VR data for 618 and pool cod data across 10 year spans 
##' ***************************************************************************
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}



library("openxlsx")
library("tidyverse")

source("FILEPATH")
source("FILEPATH")
source(paste0(repo_dir, "cod_functions.R"))


arg <- commandArgs(trailingOnly = TRUE)
gbd_round <- arg[1]
decomp_step <- arg[2]
repo_dir <- arg[3]
out_dir <- arg[4]


year_list <- c(1980:2022)


pooled_dfs <- compile_other_hemog(gbd_round_id = gbd_round, decomp_step = decomp_step,
                                  all_years = year_list)

write.xlsx(pooled_dfs,
           file = paste0(out_dir, "new_618_draws.xlsx"),
           sheetName = 'extraction', row.names = FALSE)


print("Pooled other hemog created - Cause ID 618")




