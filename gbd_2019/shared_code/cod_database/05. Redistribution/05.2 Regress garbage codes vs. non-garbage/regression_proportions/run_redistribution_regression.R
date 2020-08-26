library("ihme", lib.loc = "FILEPATH")
ihme::setup(j_root_name = "j", h_root_name = "h")

library(data.table)
library(lme4)
library(MuMIn, lib.loc="FILEPATH")
library(lmerTest, lib.loc = paste0(j, "FILEPATH"))

ihme::source_functions(get_location_metadata = T, get_covariate_estimates = T, get_envelope = T)

# Load in helper functions
source(paste0(h, "FILEPATH/regression_input_data_cleaning.R"))
source(paste0(h, "FILEPATHs/regression_engine_room.R"))
source(paste0(h, "FILEPATH/visualize_unspec_regression_fits.R"))


# GLOBAL CONSTANTS --------------------------------------------------------
loc_data <- get_location_metadata(35)

REGDIR <- paste0(j, "FILEPATH/regression_proportions/")
DECOMP_STEP <- "step1"

# TODO: these offsets are arbitrary and may exaggerate differences in logit transform
# may want to make offsets equal to highest/lowest value in the real data
LOWER_OFFSET <- .01 
UPPER_OFFSET <- .99
MAX_PROP_GARBAGE <- .50
LOWER_LIMIT_STARS <- 4
DECOMP_STEP <- "step1"

launch_set <- get_regression_launch_set()

# Run regression ----------------------------------------------------------
START_TIME <- Sys.time()

#' @indir Directory of shared_package_id, aka diabetes unspecified
#' @outdir where outputs are written fora given regression_launch_set_id
indir  <- paste0(REGDIR, "FILEPATH", launch_set$shared_package_id)
outdir <- paste0(indir, "/", launch_set$regression_launch_set_id)

dir.create(paste0(outdir), recursive = T)
sink(paste0(outdir, "/_model_summaries.txt"))

print_launch_set(launch_set)

input_dt  <- prep_input_dt(indir, launch_set$data_id)
square_dt <- prep_square_dt(indir, launch_set$data_id, year_setting = get_year_setting(launch_set$formula))

# Data prepping/cleanup ---------------------------------------------------

# manually handle data cleaning based on the shared package
results <- clean_reg_input_data(launch_set$regression_launch_set_id, input_dt, square_dt)

input_dt <- results[["input_dt"]]
square_dt <- results[["square_dt"]]
forced_predictions <- results[["forced_predictions"]]

# Begin modeling ----------------------------------------------------------

cat_ln("*************************************************************")
cat_ln("BEGIN MODELING")
cat_ln("*************************************************************")
cat_ln()
cat_ln()

regression_results(outdir = outdir, launch_set = launch_set,
                   input_dt = input_dt, square_dt = square_dt,
                   forced_predictions = forced_predictions,
                   random_effects = has_random_effects(launch_set$formula),
                   write = TRUE)

END_TIME <- Sys.time()
cat_ln("\nWall run time:", format(END_TIME - START_TIME), "\n")

sink()



