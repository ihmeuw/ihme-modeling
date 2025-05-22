# Fit by KS ensemble in parallel

rm(list = ls())

# Load packages
library(data.table)
library(foreach)
library(iterators)
library(doParallel)
library(dplyr)
library(fitdistrplus)
library(RColorBrewer)
library(ggplot2)
library(actuar)
library(grid)
library(lme4)
library(mvtnorm)
library(zipfR)
library(dfoptim)

# System Info and Filepaths
user <- "USER"
code_dir <- paste0("FILEPATH", user)
shared_functions <- "FILEPATH"

# Functions
source(paste0(code_dir, "/ensemble/eKS_parallel.R"))
source(paste0(code_dir, "/ensemble/pdf_families.R"))
source(paste0(shared_functions, "/get_location_metadata.R"))
source(paste0(shared_functions, "/get_ids.R"))


set.seed(8813)

# Read in parallel arguments
args <- commandArgs(trailingOnly = TRUE)
task_map_filepath <- args[1]
task_id <- ifelse(Sys.getenv('SLURM_ARRAY_TASK_ID') != '', as.integer(Sys.getenv('SLURM_ARRAY_TASK_ID')), NA)

task_map <- fread(task_map_filepath)

rei <-  task_map[task_id, rei]
s <- task_map[task_id, nid] # nid
lid <- task_map[task_id, location_id] # location_id
yid <- task_map[task_id, year_id] # year_id
sid <- task_map[task_id, sex_id] # sex_id
aid <- task_map[task_id, age_cat] # age_cat id
version <- task_map[task_id, version] # version of weights

# Location hierarchy
locs <- get_location_metadata(release_id=9, location_set_id = 35)
loc_name <- locs[location_id == lid, ]$location_ascii_name
sexes <- get_ids(table = "sex")

## load microdata 
print(paste0(format(Sys.time(), "%D %H:%M:%S"), " ", rei, " start"))
df <- fread(paste0("FILEPATH", rei, "/", version, "/microdata.csv"))

# Distribution list
dlist <- c(classA, classM)

print(paste0("NID ", s, ", location_id ", lid, ", year_id ", yid, ", sex_id ", sid, ", age_cat ", aid))

# Subset full microdata set to just source, location, year, and sex of interest
df_subset <- df[nid == s & location_id == lid & year_id == yid & sex_id == sid & age_cat == aid, ]
# Make a human readable sex name for saving and stuff
sex_name <- sexes[sex_id == sid, ]$sex 

# Generate vector of data to be fit
Data <- as.vector(df_subset$data)

## Only run if there is a reasonable amount of data (at least 50 datapoints, but should be much more)
print(paste0("sample size: ",length(Data)))
if (length(Data) >= 50) {
  
  ## Fit ensemble distribution
  FIT <- eKS(Data, distlist = dlist)
  ## Pull out relevant quantities of interest for diagnostic plot purposes
  M <- mean(Data, na.rm=T)
  VAR <- var(Data, na.rm=T)
  XMIN <<- min(Data, na.rm=T)
  XMAX <<- max(Data, na.rm=T)
  
  ## Get density function
  Edensity <- get_edensity(FIT$best_weights, min=XMIN, max=XMAX, mean=M,
                           variance=VAR, dlist=dlist)
  
  ## Plot fit function
  pdf(paste0("FILEPATH", rei, "/", version,
             "/diagnostics/eKS_fit_", s, "_", lid, "_", yid, "_", sid, "_", aid, ".pdf"))
  plot_fit(Data, FIT$best_weights, Edensity, 
           paste0(loc_name, ", ", yid, ", ", sex_name, ", NID ", s, ", agecat ", aid),
           distlist = dlist)
  dev.off()
  
  ## Plot KS
  pdf(paste0("FILEPATH", rei, "/", version,
             "/diagnostics/eKS_KS_", s, "_", lid, "_", yid, "_", sid, "_", aid, ".pdf"))
  plot_KS_fit(Data, FIT$best_weights, mean=M, variance=VAR, xmin=XMIN, xmax=XMAX, 
              paste0(loc_name, ", ", yid, ", ", sex_name, ", NID ", s,", agecat ", aid),
              dlist=dlist)
  dev.off()
  
  ## Write best weights file
  OUT <- data.table(FIT$best_weights, location_id = lid, nid = s, sex_id = sid,
                    year_id = yid, age_cat = aid, ks_stat = FIT$KS_statistic,
                    sample_mean = M, sample_var = VAR, sample_min = XMIN, sample_max = XMAX, sample_size = length(Data))
  ## Save detailed weights
  write.csv(OUT, paste0("FILEPATH", rei, "/", version, "/detailed_weights/",
                        s, "_", lid, "_", yid, "_", sid, "_", aid, ".csv"), na = "", row.names = FALSE)
}

print(paste0(format(Sys.time(), "%D %H:%M:%S"), " ", rei, " end"))
