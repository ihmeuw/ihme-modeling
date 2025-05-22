message("Running source_for_setup.R")
# Set up MKLROOT directory (needed if using RStudio)
Sys.setenv(MKLROOT = "FILEPATH")
# get slightly better debug messages
options(error = NULL)

# load lbd.loader tool and libraries
library(lbd.loader, lib.loc = "FILEPATH")
suppressMessages(library("lbd.mbg", lib.loc = lbd.loader::pkg_loc("lbd.mbg")))

library("viridis")
library("ggnewscale")
library("spdep")
library("ggpubr")
library("gridExtra")
library("grid")
library("doParallel")
library("foreign")
library("data.table") 
library("INLA") 
library("rgdal") 
library("rgeos") 
library("raster") 
library("colorout")

# assign region
reg_tag <- reg

# cause-specific parameters
indicator <- "had_lf_w_resamp" 
indicator_group <- "lf" 

# create new run_date directory
run_date = sprintf(paste0(as.character(Sys.Date()),'_', user, '_', reg_tag))

# setup directory paths
fp_list$mbg_root <- "FILEPATH"
output_dir <- "FILEPATH"
repo_dir <- "FILEPATH"
custom_fun_dir <- "FILEPATH"

message(paste0("Regional run for: ", reg_tag))
message(paste0("Initializing for run_date: ", run_date))
