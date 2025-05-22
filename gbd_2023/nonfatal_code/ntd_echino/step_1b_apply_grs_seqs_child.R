#' [Title: CE post-processing
#' [Notes: Array job for GRs

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0(FILEPATH)
data_root <- FILEPATH

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("FILEPATH", type = "character")
    parser$add_argument("FILEPATH", type = "character")
    parser$add_argument("FILEPATH", type = "character")
    parser$add_argument("FILEPATH", type = "character")
    parser$add_argument("--location_id", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    params_dir <- paste0(FILEPATH)
    draws_dir <- paste0(FILEPATH)
    interms_dir <- paste0(FILEPATH)
    logs_dir <- paste0(FILEPATH)
    location_id <- ADDRESS
}

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

#############################################################################################
###'                              [Estimate]                                             ###
#############################################################################################

draws  <- get_draws(gbd_id_type = "ADDRESS",
                    gbd_id = ADDRESS,
                    source = "ADDRESS",
                    measure_id = c(5, 6),
                    location_id = location_id,
		    release_id = ADDRESS,
                    version_id = ADDRESS,
                    sex_id = c(1,2)
)
  

#Abdominal or pelvic cyst localization
n_abd <- 50

#thoracic cyst localization (lungs & mediastinum)
n_thr <- 47

#brain cyst localization
n_brn <- 3

prop_abd <- rgamma(n = 1000, shape = n_abd)
prop_thr <- rgamma(n = 1000, shape = n_thr)
prop_brn <- rgamma(n = 1000, shape = n_brn)

# vectorized ie prop_abd[1] + prop_thr[1] + prop_brn[1] == scal_fct[1]  
scal_fct <- prop_abd + prop_thr + prop_brn

# vectorized again, scale so sum of proportions == 1 (internally consistent with parent)
prop_abd <- prop_abd / scal_fct
prop_thr <- prop_thr / scal_fct
prop_brn <- prop_brn / scal_fct

# append to column
prop_draws <- data.table(uniq = 1)
prop_draws[, paste0("abd_draw_", 0:999) := as.list(prop_abd)]
prop_draws[, paste0("thr_draw_", 0:999) := as.list(prop_thr)]
prop_draws[, paste0("brn_draw_", 0:999) := as.list(prop_brn)]

draws <- cbind(draws, prop_draws)

# apply proportions vectorizes
draws[, paste0("draw_abd_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("abd_draw_", x)))]
draws[, paste0("draw_thr_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("thr_draw_", x)))]
draws[, paste0("draw_brn_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("brn_draw_", x)))]

# clean

abd_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_abd_", 0:999))]
thr_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_thr_", 0:999))]
brn_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_brn_", 0:999))]

setnames(abd_draws, paste0("draw_abd_", 0:999), paste0("draw_", 0:999))
setnames(thr_draws, paste0("draw_thr_", 0:999), paste0("draw_", 0:999))
setnames(brn_draws, paste0("draw_brn_", 0:999), paste0("draw_", 0:999))

# write out

abd_draws <- abd_draws[!(age_group_id %in% c(164, 27))]
abd_draws[, model_id:= ADDRESS]
write.csv(abd_draws, paste0(FILEPATH), row.names = FALSE)

thr_draws <- thr_draws[!(age_group_id %in% c(164, 27))]
thr_draws[, model_id:= ADDRESS]
write.csv(thr_draws, paste0(FILEPATH), row.names = FALSE)

brn_draws <- brn_draws[!(age_group_id %in% c(164, 27))]
brn_draws[, model_id:= ADDRESS]
write.csv(brn_draws, paste0(FILEPATH), row.names = FALSE)
    `