###########################################################################
# Description: Age splitting                                              #
#                                                                         #
###########################################################################

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
library(readstata13)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)

## Define Constants
gbd_round_id <- 7
decomp_step <- 'iterative'
study_dems <- get_demographics("epi", gbd_round_id=gbd_round_id)
final_study_year <- last(study_dems$year_id)


### ======================= MAIN EXECUTION ======================= ###

## (1) Import incidence data
incidence <- readRDS(file = paste0(interms_dir, FILEPATH))

## (2) Merge all-age incidence onto age-specific population
locations <- unique(incidence$location_id)
pops <- get_population(location_id= locations,
                       year_id=1990:final_study_year,
                       age_group_id=study_dems$age_group_id,
                       sex_id=c(1,2),
                       gbd_round_id=gbd_round_id,
                       decomp_step = decomp_step)

# within a location year, calculate pop across sex and age groups
pops[, totalPop := sum(population), by=.(location_id, year_id)]
inc <- merge(pops, incidence, by=c("location_id", "year_id", "sex_id"))


## (3) Merge Age-Curve from Dismod
# Import age splitting draws file from dismod
draws <- read.dta13(paste0(params_dir, FILEPATH))
draws <- as.data.table(draws)[,c("location_id","year_id") := NULL] # age curve is for all years and locations
setnames(draws, old=paste0("draw_",0:999), new=paste0("ageCurve_",0:999))


# (4) Adjust to GBD2020 age groups
ageADDRESS1 <- draws[age_group_id == ADDRESS1]
ageADDRESS2 <- draws[age_group_id == ADDRESS2]
age_ADDRESS3 <- copy(ageADDRESS1[, age_group_id := ADDRESS3])
age_ADDRESS4 <- copy(ageADDRESS1[, age_group_id := ADDRESS4])
age_ADDRESS5 <- copy(ageADDRESS2[, age_group_id := ADDRESS5])
age_ADDRESS6 <- copy(ageADDRESS2[, age_group_id := ADDRESS6])
draws <- rbind(draws, age_ADDRESS3, age_ADDRESS4, age_ADDRESS5, age_ADDRESS6)
split <- merge(inc, draws, by=c("sex_id", "age_group_id"), all.x=TRUE, all.y=FALSE)


## (5) Apply age split
split[, paste0("casesCurve_",0:999) := lapply(0:999, function(x)
  get(paste0("ageCurve_", x)) * get("population") )]

# Sum casesCurve_i across age groups (within a location/year/sex)
split[, paste0("totalCasesCurve_",0:999) := lapply(0:999, function(x)
  sum(get(paste0("casesCurve_",x))) ), by=.(location_id, year_id, sex_id)]

# Calculate age-specific incidence
split[, paste0("inc_age_draw_",0:999) := lapply(0:999, function(x)
  get(paste0("casesCurve_",0:999)) * (get(paste0("sex_cases_",0:999)) / get(paste0("totalCasesCurve_",0:999))) / get("population") )]

# Drop casesCurve_i and totalCasesCurve_i
split <- split[, paste0("casesCurve_",0:999):=NULL]
split <- split[, paste0("totalCasesCurve_",0:999):=NULL]


## (6) Prepare data for upload
me_ADDRESS <- split[, c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", paste0("inc_age_draw_",0:999))]
setnames(me_ADDRESS, old=paste0("inc_age_draw_",0:999), new=paste0("draw_",0:999))
me_ADDRESS$modelable_entity_id <- ADDRESS
me_ADDRESS$measure_id <- 6

# For under 1 yrs, set incidence to 0
me_ADDRESS[age_group_id < ADDRESS, paste0("draw_",0:999) := 0]

# Create prevalence == incidence
row_list <- rep(seq_len(nrow(me_ADDRESS)), each = 2)
me_ADDRESS <- me_ADDRESS[row_list,]
me_ADDRESS$measure_id <- rep(c(6,5), length.out=nrow(me_ADDRESS))


# Save file
write.csv(me_ADDRESS, file = paste0(interms_dir, FILEPATH))
