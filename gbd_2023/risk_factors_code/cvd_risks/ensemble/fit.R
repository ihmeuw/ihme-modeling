# USERNAME
# DATE
# Fit by KS ensemble in parallel
# must be run on the cluster
rm(list = ls())

# Load packages
library(data.table)
library(foreach)
library(iterators)
library(doParallel)
library(dplyr)
library(dfoptim, lib = "/FILEPATH/")
library(fitdistrplus)
library(RColorBrewer)
library(ggplot2)
library(actuar)
library(grid)
library(lme4)
library(mvtnorm)
library(zipfR)
.libPaths("/FILEPATH/")

# functions
source("/FILEPATH/eKS_parallel.R")
source("/FILEPATH/pdf_families.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_ids.R")

set.seed(52506)

# Read in parallel arguments
args <- commandArgs(trailingOnly = TRUE)
task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
params <- fread(args[1])[task_id, ]

s <- unique(params$nid) # nid
lid <- unique(params$location_id) # location_id
yid <- unique(params$year_id) # year_id
rei <- args[2]
version <- as.numeric(args[3]) # version of weights
by_sex <- as.numeric(args[4]) # fit detailed weights by sex?

# Location hierarchy
locs <- get_location_metadata(location_set_id = 22)
loc_name <- locs[location_id == lid, ]$location_ascii_name
sexes <- get_ids(table = "sex")

## load microdata
print(paste0(format(Sys.time(), "%D %H:%M:%S"), " ", rei, " start"))
df <- fread(paste0("/FILEPATH/microdata.csv"))
df <- df[nid == s & location_id == lid & year_id == yid, ]
# Deal with custom sex groups
if (by_sex==0) df[, sex_id := 3]

# Distribution list
dlist <- c(classA, classB, classM)

## Subset to specific paralell sex group
for (sid in unique(df$sex_id)) {

    print(paste0("NID ", s, ", location_id ", lid, ", year_id ", yid, ", sex_id ", sid))

    # Subset full microdata set to just source, location, year, and sex of interest
    df_subset <- df[nid == s & location_id == lid & year_id == yid & sex_id == sid, ]
    # Make a human readable sex name for saving
    sex_name <- sexes[sex_id == sid, ]$sex

    # Generate vector of data to be fit
    Data <- as.vector(df_subset$data)

    ## Only run if there is a reasonable amount of data (at least 50 datapoints, but should be much more)
    if (length(Data) > 50) {

        ## Fit ensemble distribution
        FIT <- eKS(Data = Data, distlist = dlist)
        ## Pull out relevant quantities of interest for diagnostic plot purposes
        M <- mean(Data, na.rm=T)
        VAR <- var(Data, na.rm=T)
        XMIN <<- min(Data, na.rm=T)
        XMAX <<- max(Data, na.rm=T)

        ## Get density function
        Edensity <- get_edensity(FIT$best_weights, min=XMIN, max=XMAX, mean=M,
                                 variance=VAR, distlist=dlist)

        ## Plot fit function
        pdf(paste0("/FILEPATH/eKS_fit_", s, "_", lid, "_", yid, "_", sid, ".pdf"))
        plot_fit(Data, FIT$best_weights, Edensity,
                 paste0(loc_name, ", ", yid, ", ", sex_name, ", NID ", s),
                 distlist = dlist)
        dev.off()

        ## Plot KS
        pdf(paste0("/FILEPATH/eKS_KS_", s, "_", lid, "_", yid, "_", sid, ".pdf"))
        plot_KS_fit(Data, FIT$best_weights, mean=M, variance=VAR, xmin=XMIN, xmax=XMAX,
                    paste0(loc_name, ", ", yid, ", ", sex_name, ", NID ", s),
                    dlist=dlist)
        dev.off()

        ## Write best weights file
        OUT <- data.table(FIT$best_weights, location_id = lid, nid = s, sex_id = sid,
                          year_id = yid, ks_stat = FIT$KS_statistic)
        ## Save detailed weights
        write.csv(OUT, paste0("/FILEPATH/",
                              s, "_", lid, "_", yid, "_", sid, ".csv"), na = "", row.names = FALSE)
    }
}

print(paste0(format(Sys.time(), "%D %H:%M:%S"), " ", rei, " end"))
