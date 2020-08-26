############################################################################
# Description: Generate draws of GW incidence for endemic locations with   #
#              missing data. Then, merge with pops data for ALL locations. #
#              Finally, sex-splitting is performed.                        #
############################################################################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <- FILEPATH
data_root <- FILEPATH
cause <- "ntd_guinea"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
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
    params_dir  <- FILEPATH
    draws_dir   <- FILEPATH
    interms_dir <- FILEPATH
    logs_dir    <- FILEPATH
}

##	Source relevant libraries
source(FILEPATH)
source(FILEPATH)


## Define Constants
gbd_round_id <- 7
decomp_step <- 'iterative'
study_dems <- get_demographics("epi", gbd_round_id=gbd_round_id)
final_study_year <- last(study_dems$year_id)

### ======================= MAIN EXECUTION ======================= ###

## (1) Read in country-year-specific case data for endemic countries with beta draws
draws <- fread(paste0(draws_dir, FILEPATH))[,-1]


## (2) Cleaning
# drop any national level data when you have subnational level data present
# drop Ethiopia (179), Nigeria (214), Pakistan (165), Kenya (180), and India (163) national location ids
draws <- draws[! (location_id %in% c(179, 214, 165, 180, 163))]


## (3) Run Poisson regression by country
for (location in unique(draws$location_id)){

    # fit the model one location at a time
    m1 <- glm(cases ~ year_id, offset=log(sample_size), family = "poisson",
              data=draws[location_id==location, .(location_id, year_id, cases, sample_size)])

    # extract the predicted value and SE for all years
    temp <- predict.glm(m1, newdata = data.frame(year_id=1984:final_study_year, sample_size=1))
    tempSe <- predict(m1, newdata = data.frame(year_id=1984:final_study_year, sample_size=1), se.fit = TRUE)$se.fit

    # add predicted value and SE into the dataset for the appropriate location and years
    draws[location_id == location, `:=` (predicted = temp, predictedSe = tempSe)]
}

# update draws for location years with missing case counts
draws[is.na(cases), paste0("draw_", 0:999) := lapply(0:999, function(x) 
    exp(rnorm(sum(is.na(cases)), predicted, predictedSe)) )]

# ensure all draws are non-negative
draws[, paste0("draw_", 0:999) := lapply(0:999, function(x)
    ifelse(get(paste0("draw_",x))<0, 0, get(paste0("draw_",x))) )]


## (4) Apply sex splitting
# * prop of GW disease reported in males = 47%
# * prop of GW disease reported in females = 53%
# * sex-specific cases = total incidence x sex splitting proportion x total national pop

# Initialize each sex with both sex rows
row_list <- rep(seq_len(nrow(draws)), each = 2)
draws <- draws[row_list,][, sex_id := seq_len(.N), by=.(location_id, year_id)]
setnames(draws, old="sample_size", new="total_pop")

# MALES:
draws[sex_id==1, paste0("sex_cases_", 0:999) := lapply(0:999, function(x)
 get(paste0("draw_", x)) * 0.47 * get("total_pop") )]

# FEMALES:
draws[sex_id==2, paste0("sex_cases_", 0:999) := lapply(0:999, function(x)
get(paste0("draw_", x)) * 0.53 * get("total_pop") )]

draws[, paste0("draw_", 0:999) := NULL]
draws[, c("age_group_id", "beta", "id", "predicted", "predictedSe") := NULL]


## (5) Save intermediate file
saveRDS(draws, file = paste0(interms_dir,FILEPATH))
