# Pull covariate estimates for location scaling and calculate population not at risk

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH")

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    parser$add_argument("--release_id", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    data_root <- "FILEPATH"
    params_dir <- "FILEPATH"
    draws_dir <- "FILEPATH"
    interms_dir <- "FILEPATH"
    logs_dir <- "FILEPATH"
    release_id <- 16
}

# Source relevant libraries                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
library(data.table)
library(stringr)
source("FILEPATH")

study_dems <- get_demographics(gbd_team="epi", release_id=release_id)

### ======================= MAIN ======================= ###
    
cov_dts <- list()
for (cov in c(142, 1218)) {
        cov_data <- get_covariate_estimates(cov, location_id=study_dems$location_id, year_id=study_dems$year_id, release_id=release_id)
    
    # Rename and subset to vals
    cov_name <- unique(cov_data$covariate_name_short)
    setnames(cov_data, old=c("mean_value", "lower_value", "upper_value"), new=c(paste0(cov_name, "_mean"), paste0(cov_name, "_lower"), paste0(cov_name, "_upper")))
    sub_cols <- append(c("covariate_id", "location_id", "year_id", "age_group_id", "sex_id"), grep(paste0(cov_name, "*"), names(cov_data), value = TRUE))
    cov_data <- cov_data[ , ..sub_cols]
    
    # Generate 1000 draw columns
    cov_data <- cov_data[, paste0(cov_name, "_", 0:999) := lapply(0:999, function(x) get(paste0(cov_name, "_mean")))]
    cov_dts <- append(cov_dts, list(cov_data), 0)
}

# Merge the covariate estimates and calculate population not at risk
all_cov_draws <- as.data.table(merge(cov_dts[1], cov_dts[2], all.x=TRUE, all.y=TRUE, by=c("location_id", "year_id")))
all_cov_draws <- all_cov_draws[, paste0("religion_sanit_par_prop_", 0:999) := lapply(0:999, function(x) 1 - (1 - get(paste0("religion_muslim_prop_", x))) * (1 - get(paste0("sanitation_prop_", x))))]

sub_cols <- append(c("location_id", "year_id"), grep("religion*", names(all_cov_draws), value = TRUE))
all_cov_draws <- all_cov_draws[, ..sub_cols]

write.csv(all_cov_draws, "FILEPATH", row.names=FALSE)

    
 
