##############################################################################
## Purpose: Generate age model results, run them through stage 1 predictions
## Steps:
##    1. Load in sex model results, generate sex-specific 5q0 from the ratio in
##          the model and the birth sex ratio
##    2. Read in age model coefficents, generated in 02_fit models
##    3. Make stage 1 predictions, with and without random effects for input
##          into space time
##            a) predictions are in conditional probability space, first put it
##                into qx space and then put it into logit space
##############################################################################

rm(list=ls())

library(pacman)
pacman::p_load(RMySQL, data.table, foreign, plyr, haven, assertable, devtools,
               methods, argparse, splines, plm)
library(mortdb, lib.loc = "FILEPATH")

# Parse arguments
# Get arguments
parser <- ArgumentParser()
parser$add_argument("--version_id", type = "integer", required = TRUE,
                    help = "The version_id for this run of age-sex")
parser$add_argument("--sex", type = "character", required = TRUE,
                    help = "The sex to run, male or female")
parser$add_argument("--version_5q0_id", type = "integer", required = TRUE,
                    help = "The 5q0 version for this run of age-sex")
parser$add_argument("--version_ddm_id", type = "integer", required = TRUE,
                    help = "The DDM version for this run of age-sex")
parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                    help = "GBD Year")
parser$add_argument("--end_year", type = "integer", required = TRUE,
                    help = "last year we produce estimates for")
parser$add_argument("--code_dir", type = "character", required = TRUE,
                    help = "Directory where age-sex code is cloned")
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
sex_arg <- args$sex
version_5q0_id <- args$version_5q0_id
version_ddm_id <- args$version_ddm_id
gbd_year <- args$gbd_year
end_year <- args$end_year
code_dir <- args$code_dir

# Load central functions
source(paste0(code_dir, "/space_time.r"))

# Get file path
output_dir <- paste0("FILEPATH")
output_5q0_dir <- paste0("FILEPATH")
output_ddm_dir <- paste0("FILEPATH")
data_density_5q0_file <- paste0("FILEPATH")

age_sex_input_file <- paste0("FILEPATH")
live_births_file <- paste0("FILEPATH")
model_input_5q0_file <- paste0("FILEPATH")
estimates_5q0_file <- paste0("FILEPATH")
vr_child_completeness_file <- paste0("FILEPATH")

# Make folders
dir.create(paste0(output_dir), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)
dir.create(paste0("FILEPATH"), showWarnings = FALSE)

# Load location data
st_locs <- fread(paste0("FILEPATH"))
regs <- fread(paste0("FILEPATH"))[,.(ihme_loc_id, region_name)]
location_data <- fread(paste0("FILEPATH"))

# Define locations to assert
assert_ihme_loc_ids <- location_data[, ihme_loc_id]

# Define years to assert
assert_year_ids = c(1950:end_year)
assert_mid_years = c(1950:end_year) + 0.5

# Define age groups
age <- c("enn", "lnn", "pnn", "pna", "pnb", "inf", "ch", "cha", "chb")

# data density ------------------------------------------------------------

get_data_density <- function() {
  
  st_locs <- fread(paste0("FILEPATH"))[,.(ihme_loc_id, region_name)]
  
  # Read in age-sex input data
  data <- fread(age_sex_input_file)
  
  # Keep sex-specific data
  data <- data[sex == sex_arg]
  
  # Keep just the columns we need
  keep1 <- names(data)[grepl("exclude_", names(data))]
  keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource",
            "q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch",
            "q_cha", "q_chb", "q_u5", keep1)
  data <- data[,names(data) %in% keep, with = F]
  
  # Copy the data temporarily
  temp <- copy(data)
  
  # melt long on age
  data <- melt.data.table(
    data,
    id.vars = c("region_name", "ihme_loc_id", "year", "sex", "source",
                "broadsource", keep1),
    measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch",
                   "q_cha", "q_chb"),
    variable.name = "age_group_name" ,
    value.name = "qx_data"
  )
  data[, age_group_name := gsub("q_", "", age_group_name)]
  
  temp <- temp[, c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch",
                   "q_cha", "q_chb") := NULL]
  temp <- melt.data.table(
    temp,
    id.vars = c("region_name", "ihme_loc_id", "year", "sex", "source",
                "broadsource"),
    measure.vars = keep1,
    variable.name = "age_group_name",
    value.name = "exclude"
  )
  temp[, age_group_name := gsub("exclude_", "", age_group_name)]
  
  data <- data[, !names(data) %in% keep1, with = F]
  data <- data[!is.na(qx_data)]
  data <- merge(data, temp,
                by = c("region_name", "ihme_loc_id", "year", "sex",
                       "age_group_name", "source", "broadsource"),
                all.x =T)
  data <- unique(data, by = c())
  
  data <- data[, "region_name" := NULL]
  data <- merge(data, st_locs, by = "ihme_loc_id", allow.cartesian = T)
  data <- data[!is.na(qx_data)]
  
  data <- data[exclude == 0]
  data <- data[sex != "both"]
  data_den <- copy(data)
  structure_dt <- data.table(
    expand.grid(
      region_name = unique(data$region_name),
      sex= unique(data$sex),
      age_group_name = unique(data$age_group_name)
    )
  )
  data_den <- data[, n := .N, by = .(region_name, sex, age_group_name)]
  data_den <- merge(structure_dt, data_den,
                    by = c("region_name", "sex", "age_group_name"), all = T)
  data_den[is.na(n), n := 0]
  setnames(data_den, "age_group_name", "age")
  data_den <- data_den[, .(region_name, sex, age, n)]
  data_den <- unique(data_den)
  
  return(data_den)
}

data_den <- get_data_density()

# prep input data ---------------------------------------------------------

# age-sex dataset
data <- fread(age_sex_input_file)
data <- data[!is.na(q_u5)]

# covariates
covs <- fread(model_input_5q0_file)
vr_list <- fread(vr_child_completeness_file)
covs[, year_id := floor(year)]
covs <- covs[,.(ihme_loc_id, year_id, hiv, maternal_edu)]
covs <- unique(covs)

# locations
locs <- fread(paste0("FILEPATH"))
locs <- locs[level_all == 1, .(ihme_loc_id, standard)]
fake_regions <- fread(paste0("FILEPATH"))
fake_regions <- fake_regions[keep == 1]
setnames(fake_regions, "region_name", "gbdregion")
fake_regions[, c("location_id","keep") := NULL]
locs <- merge(locs, fake_regions, by = "ihme_loc_id")

# calculate sex-specific 5q0 ----------------------------------------------

# read in means of GPR sex model
compiled_sex_model_file <- paste0("FILEPATH")

if (file.exists(compiled_sex_model_file)) {
  sexmod <- fread(compiled_sex_model_file)
} else {
  file_locs <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)$ihme_loc_id
  sexmod <- assertable::import_files(
    filenames = paste0("FILEPATH")
  )
  readr::write_csv(sexmod, compiled_sex_model_file)
}
assertable::assert_ids(
  sexmod,
  list(ihme_loc_id = assert_ihme_loc_ids, year = assert_mid_years),
  assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE, warn_only = FALSE,
  quiet = FALSE
)

# format sex model estimates
sexmod[, year_id := floor(year)]
sexmod[, c("lower", "upper") := NULL]

# inverse logit (scaled domain to [0.8, 1.5])
sexmod[, med := exp(med) / (1 + exp(med))]
sexmod[, med := (med * 0.7) + 0.8]

# read and format births
births <- fread(live_births_file)
if("ihme_loc_id" %in% names(births)) births[, ihme_loc_id := NULL]
births <- merge(births,
                location_data[, c("location_id", "ihme_loc_id")],
                by = c("location_id"))
births <- births[, c("location_name", "sex_id", "location_id") := NULL]
births <- dcast.data.table(births, ihme_loc_id + year ~ sex, value.var = "births")
births[, birth_sexratio := male / female]
births <- births[, c("female", "male", "both") := NULL]
setnames(births, "year", "year_id")
assertable::assert_ids(
  births,
  list(ihme_loc_id = assert_ihme_loc_ids, year_id = assert_year_ids),
  assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE, warn_only = FALSE,
  quiet = FALSE
)

# read in and format both-sex 5q0
child <- fread(estimates_5q0_file)
child <- child[estimate_stage_id == 3, ]
qu5 <- copy(child)
child <- merge(child,
               location_data[, c("location_id", "ihme_loc_id")],
               by = c("location_id"))
child <- child[, c("ihme_loc_id", "year_id", "mean")]
setnames(child, "mean", "both")
assertable::assert_ids(
  child,
  list(ihme_loc_id = assert_ihme_loc_ids, year_id = assert_year_ids),
  assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE, warn_only = FALSE,
  quiet = FALSE
)

# Merge sex model, births, and both-sex 5q0 estimates
sexmod <- sexmod[,year := NULL]
sexmod <- merge(sexmod, births, all.x = T, by = c("ihme_loc_id", "year_id"))
sexmod <- merge(sexmod, child, all.x = T, by = c("ihme_loc_id", "year_id"))
assertable::assert_ids(
  sexmod,
  list(ihme_loc_id = assert_ihme_loc_ids, year_id = assert_year_ids),
  assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE, warn_only = FALSE,
  quiet = FALSE
)

# calculating sex specific 5q0
sexmod[, female := (both * ( 1 + birth_sexratio)) / (1 + (med * birth_sexratio))]
sexmod[, male := female * med]
sexmod[, c("med") := NULL]

# reshape
sexmod <- melt.data.table(
  sexmod,
  id.vars = c("ihme_loc_id", "year_id", "birth_sexratio"),
  measure.vars = c("both", "male", "female"),
  variable.name = "sex",
  value.name = "q_u5"
)

# HIV-free 5q0 for enn & lnn ----------------------------------------------

# get a list of Group 1A HIV locations
hiv_location_information <- fread(paste0("FILEPATH"))
hiv_group_1a_locations <- hiv_location_information[group == "1A", ihme_loc_id]

# import the calculated hiv_free_ratios
hiv_free_ratios <- fread(paste0("FILEPATH"))

# merge HIV-free ratios
sexmod <- merge(sexmod, hiv_free_ratios,
                by = c("ihme_loc_id", "year_id", "sex"),
                all.x = T)

cdr <- copy(covs)
cdr <- cdr[, c("ihme_loc_id", "year_id", "hiv")]
cdr <- unique(cdr, by = c())
setnames(cdr, "hiv", "hiv_cdr")

hivfree <- copy(cdr)
hivfree$hiv_beta <- 1.3868677
hivfree <- hivfree[order(ihme_loc_id, year_id)]
hivfree[, year_id := floor(year_id)]

sexmod <- merge(hivfree, sexmod, by = c("ihme_loc_id", "year_id"))

# convert 5q0 to 5m0, this represent with-HIV
sexmod[,hiv_mx := -log(1-q_u5)/5]

# calculate hiv-free mx
sexmod[,no_hiv_mx := hiv_mx - (1 * hiv_cdr)]

# get the hiv free mx by multiplying the HIV free ratio by the mx with HIV
sexmod[ihme_loc_id %in% hiv_group_1a_locations & !is.na(hiv_free_ratio),
       no_hiv_mx := hiv_mx * hiv_free_ratio]

# convert HIV-free mx back to qx
sexmod[, no_hiv_qx := 1 - exp(-5 * no_hiv_mx)]
sexmod <- sexmod[, .(ihme_loc_id, year_id, no_hiv_qx, q_u5, sex, birth_sexratio)]

readr::write_csv(sexmod, paste0("FILEPATH"))

# make square and merge back onto main dataset
temp <- data.table(
  expand.grid(
    ihme_loc_id = sexmod$ihme_loc_id,
    age_group_name = age
  )
)
temp <- unique(temp)
sexmod <- merge(sexmod, temp, all = T, by = "ihme_loc_id", allow.cartesian = T)

for_later <- copy(sexmod)

# for enn and lnn use HIV free 5q0 in prediction
sexmod[age_group_name %in% c("enn", "lnn"), q_u5 := no_hiv_qx]

sexmod[, log_q_u5:= log(q_u5)]

## keep only relevant sex
sexmod <- sexmod[sex==sex_arg]

d <- copy(sexmod)
d[, no_hiv_qx := NULL]

# prep for predict --------------------------------------------------------

# merge on covariates
d <- merge(d, covs, by = c("ihme_loc_id", "year_id"))

# add s_comp = 1 for hypothetically complete dataset
d[, s_comp := 1]

# log-transform HIV
d[, hiv := log(hiv + 0.000000001)]

# merge on locs for gbdregion
d <- merge(d, locs, by = "ihme_loc_id")

# read in model fit objects
model_enn <- readRDS(paste0("FILEPATH"))
model_lnn <- readRDS(paste0("FILEPATH"))
model_pnn <- readRDS(paste0("FILEPATH"))
model_pna <- readRDS(paste0("FILEPATH"))
model_pnb <- readRDS(paste0("FILEPATH"))
model_inf <- readRDS(paste0("FILEPATH"))
model_ch  <- readRDS(paste0("FILEPATH"))
model_cha  <- readRDS(paste0("FILEPATH"))
model_chb  <- readRDS(paste0("FILEPATH"))

setnames(d,"log_q_u5","log_q5_var")

# make stage 1 predictions ------------------------------------------------

for(aa in age){
  
  temp <- d[age_group_name == aa]
  temp$pred_log_qx_s1 <- predict(get(paste0("model_", aa)),
                                 newdata = temp,
                                 allow.new.levels = T)
  
  # for pna, pnb, use pnn HIV fixed effect
  if(aa %in% c("pna", "pnb")) {
    temp[, pred_log_qx_s1 := pred_log_qx_s1 + hiv * fixef(model_pnn)[["hiv"]]]
  }
  
  # for cha, chb, use ch HIV fixed effect
  if(aa %in% c("cha", "chb")) {
    temp[, pred_log_qx_s1 := pred_log_qx_s1 + hiv * fixef(model_ch)[["hiv"]]]
  }
  
  d <- rbind(d, temp, fill = T)
}
d <- d[!is.na(pred_log_qx_s1),]
setnames(d, "log_q5_var", "log_q_u5")

# format stage 1 predictions ----------------------------------------------

# transform, keep naming for convenience
d[, pred_log_qx_s1 := exp(pred_log_qx_s1)]

# convert to qx space
d[, q_u5 := NULL]
d <- merge(d, for_later, by = c("ihme_loc_id", "year_id", "sex",
                                "birth_sexratio", "age_group_name"), all.x=T)
d <- melt.data.table(d,
  id.vars = c("ihme_loc_id", "year_id", "age_group_name", "log_q_u5",
              "birth_sexratio", "sex", "q_u5", "no_hiv_qx"),
  measure.vars = c("pred_log_qx_s1")
)

d <- dcast.data.table(d,
  ihme_loc_id + year_id + birth_sexratio + sex + q_u5 + variable + no_hiv_qx ~ age_group_name,
  value.var = "value"
)

# convert from conditional probability space back to qx space
d[, enn := no_hiv_qx * enn]
d[, lnn := (no_hiv_qx * lnn) / ((1 - enn))]
d[, pnn := (q_u5 * pnn) / ((1 - enn) * (1 - lnn))]
d[, pna := (q_u5 * pna) / ((1 - enn) * (1 - lnn))]
d[, pnb := (q_u5 * pnb) / ((1 - enn) * (1 - lnn) * (1 - pna))]
# TODO: compare replacing (1-pnn) with (1-pna)*(1-pnb) in ch calculation below
d[, ch := (q_u5 * ch)  / ((1 - enn) * (1 - lnn) * (1 - pnn))]
d[, cha := (q_u5 * cha) / ((1 - enn) * (1 - lnn) * (1 - pnn))]
d[, chb := (q_u5 * chb) / ((1 - enn) * (1 - lnn) * (1 - pnn) * (1 - cha))]
d[, inf:= (q_u5 * inf)]

# reshape back long, coverting to logit qx space
d <- melt.data.table(
  d,
  id.vars = c("ihme_loc_id", "year_id", "birth_sexratio", "sex", "q_u5", "variable"),
  measure.vars = c("ch", "cha", "chb", "enn", "inf", "lnn", "pnn", "pna", "pnb"),
  variable.name = "age_group_name"
)
d[, value := log(value)]
d <- dcast.data.table(
  d,
  ihme_loc_id + year_id + birth_sexratio + sex + q_u5 + age_group_name ~ variable,
  value.var = "value"
)

# reshape age long
data <- data[sex == sex_arg]
keep1 <- names(data)[grepl("exclude_", names(data))]
keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource",
          "q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha",
          "q_chb", "q_u5", keep1)
data <- data[,names(data) %in% keep, with = F]
temp <- copy(data)
data <- melt.data.table(
  data,
  id.vars = c("region_name", "ihme_loc_id", "year", "sex", "source",
              "broadsource", keep1),
  measure.vars = c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_ch", "q_cha",
                   "q_chb", "q_inf"),
  variable.name = "age_group_name" ,
  value.name = "qx_data"
)
data[, age_group_name := gsub("q_", "", age_group_name)]

temp <- temp[, c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_ch", "q_cha",
                 "q_chb", "q_inf") := NULL]
temp <- melt.data.table(
  temp,
  id.vars = c("region_name", "ihme_loc_id", "year", "sex", "source",
              "broadsource"),
  measure.vars = keep1,
  variable.name = "age_group_name",
  value.name = "exclude"
)
temp[, age_group_name := gsub("exclude_", "", age_group_name)]

data <- data[, !names(data) %in% keep1, with = F]
data <- data[!is.na(qx_data)]

data <- merge(data, temp,
              by = c("region_name", "ihme_loc_id", "year", "sex",
                     "age_group_name", "source", "broadsource"),
              all.x = T)
data <- unique(data, by = c())

data[, year_id := round(year)]
data[, log_qx_data := log(qx_data)]

d <- merge(d, data, all.x = T,
           by = c("ihme_loc_id", "year_id", "sex", "age_group_name"))

# save outputs
readr::write_csv(d, paste0("FILEPATH"))
readr::write_csv(data, paste0("FILEPATH"))

# hyperparameters ---------------------------------------------------------

# Get data density and corresponding hyperparameter values from 5q0
keep_param_cols <- c("ihme_loc_id", "complete_vr_deaths", "scale", "amp2x",
                     "lambda", "zeta", "best")
params <- fread(data_density_5q0_file)
params[, lambda := lambda * 2]
params <- params[, keep_param_cols, with = FALSE]

# save hyperparameters
for(ages in unique(d$age_group_name)) {
  readr::write_csv(params, paste0("FILEPATH"))
}
