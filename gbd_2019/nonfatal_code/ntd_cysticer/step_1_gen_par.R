# Pull covaraite estimates to be used in later parllel by location scaling,
# also calculate population not at risk
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) 
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    params_dir <- paste0(data_root, "FILEPATH")
    draws_dir <- paste0(data_root, "FILEPATH")
    interms_dir <- paste0(data_root, "FILEPATH")
    logs_dir <- paste0(data_root, "FILEPATH")
}

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH")
source("FILEPATH")

# Constants
gbd_round_id <- 6
study_dems <- readRDS(paste0(data_root, "FILEPATH", gbd_round_id, "FILEPATH"))

### ======================= MAIN ======================= ###

cov_dts <- list()
for (cov in c(ADDRESS)) {
    cov_data <- get_covariate_estimates(cov, location_id=study_dems$location_id, year_id=study_dems$year_id, decomp_step="step4")
    
    # Rename and subset to vals
    cov_name <- unique(cov_data$covariate_name_short)
    setnames(cov_data, old=c("mean_value", "lower_value", "upper_value"), new=c(paste0(cov_name, "_mean"), paste0(cov_name, "_lower") ,paste0(cov_name, "_upper") ) )
    sub_cols <- append(c("covariate_id", "location_id", "year_id", "age_group_id", "sex_id"), grep(paste0(cov_name, "*"), names(cov_data), value = TRUE) )
    cov_data <- cov_data[ , ..sub_cols]
    
    # Generate 1000 draw copy cols (NOTE: not necessary unless upper lower being propogated, to review.)
    cov_data <- cov_data[, paste0(cov_name, "_", 0:999) := lapply(0:999, function(x) get(paste0(cov_name, "_mean")) ) ]
    cov_dts <- append(cov_dts, list(cov_data), 0)
}
# Merge the two covs and calculate values of pnar
all_cov_draws <- as.data.table(merge(cov_dts[1], cov_dts[2], all.x=TRUE, all.y=TRUE, by=c("location_id", "year_id")))
all_cov_draws <- all_cov_draws[, paste0("religion_sanit_par_prop_", 0:999) := lapply(0:999, function(x) 1 - (1 - get(paste0("religion_muslim_prop_", x)) ) * (1 - get(paste0("sanitation_prop_", x)) ) ) ]

sub_cols <- append(c("location_id", "year_id"), grep("religion*", names(all_cov_draws), value = TRUE) )
all_cov_draws <- all_cov_draws[, ..sub_cols]

write.csv(all_cov_draws, paste0(interms_dir, "FILEPATH"), row.names=FALSE)
