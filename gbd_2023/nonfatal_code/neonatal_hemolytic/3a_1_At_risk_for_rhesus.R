################################################################################
## Purpose:   Calculate births at risk for rhesus
## Input:     Rh negative model results (MEID: 32006)
##            RhoD to live births model results (MEID: 28989)
##            Nonfirstborn proportion (MEID: 28665)
## Output:    At risk for rhesus prevalence
################################################################################

# set up -----------------------------------------------------------------------
library(data.table)
library(dplyr)
library(stringr)
params_global <- readr::read_rds("params_global.rds")

locs <- ihme::get_location_metadata(
  location_set_id = 35,
  release_id = params_global$release_id,
)

# calculate rhod doses for every location year and sex -------------------------
rhod_draws <- ihme::get_draws(
  'modelable_entity_id',
  28989,
  release_id = params_global$release_id,
  location_id = params_global$location_id,
  sex_id = nch::id_for("sex", c("Male", "Female")),
  year_id = params_global$year_id,
  source = 'epi',
  measure_id = nch::id_for("measure", "proportion"),
  metric_id = nch::id_for("metric", "Rate"),
  age_group_id = nch::id_for("age_group", "Birth")
)

# pull total live births
live_births <- ihme::get_covariate_estimates(covariate_id = nch::id_for("covariate", "Live births (by sex)"),
                                             sex_id = c(1,2),
                                             year_id = params_global$year_id,
                                             location_id = params_global$location_id,
                                             release_id = params_global$release_id
)[,.(location_id, year_id, sex_id, mean_value)]
setnames(live_births, 'mean_value', 'births') #unlike covariate 60, covariate 1106 is not in thousands

# merge rhod_draws with live_births
rhod_to_livebirths <- merge(
  live_births,
  rhod_draws,
  by = c("location_id", "year_id", "sex_id"),
  all = TRUE
)
rhod_to_livebirths <- rhod_to_livebirths[, -c('modelable_entity_id',
                                              'model_version_id',
                                              'metric_id',
                                              'measure_id')]

# calculate number of rhod doses
rhod_to_livebirths <- rhod_to_livebirths %>%
  mutate(across(starts_with("draw_"), ~ .x * births)) %>%
  rename_with(~ gsub("^draw_", "rhod_draw_", .x), starts_with("draw_"))

# calculate count of rh negative mothers not covered by rhod -------------------
# pull modeled prevalence of Rh negativity (there is no variation in age, sex, years)
rh_neg <- ihme::get_draws(
  'modelable_entity_id',
  32006,
  release_id = params_global$release_id,
  location_id = params_global$location_id,
  sex_id = nch::id_for("sex", c("Male", "Female")),
  year_id = 2022,
  source = 'epi',
  measure_id = nch::id_for("measure", "prevalence"),
  age_group_id = nch::id_for("age_group", "Birth")
)

rh_neg <- rh_neg[, -c('year_id', 'measure_id', 'age_group_id', 'metric_id', 'modelable_entity_id', 'model_version_id')]

rh_neg <- rh_neg %>%
  rename_with(~ gsub("^draw_", "rhneg_draw_", .x), starts_with("draw_"))

# merge Rh neg draws with RhoD draws (not merging by year bc no variation in Rh neg draws by year)
rhneg_rhod <-
  merge(rhod_to_livebirths,
        rh_neg,
        by = c('location_id', 'sex_id'),
        all = TRUE)
setDT(rhneg_rhod)

# calculate births from Rh negative mothers
rhneg_draw_cols <- paste0("rhneg_draw_", 0:999)
rhneg_rhod[, (rhneg_draw_cols) := lapply(.SD, function(x) x * births), .SDcols = rhneg_draw_cols]

# calculate count of Rh negative mothers not covered by RhoD
# resulting value of 0 means full coverage
for(i in 0:999) {
  # Create product column
  rhneg_rhod[[paste0("not_covered_draw_", i)]] <- pmax(rhneg_rhod[[paste0("rhneg_draw_", i)]] - rhneg_rhod[[paste0("rhod_draw_", i)]], 0)
  
  # Remove original draw columns
  rhneg_rhod[[paste0("rhneg_draw_", i)]] <- NULL
  rhneg_rhod[[paste0("rhod_draw_", i)]] <- NULL
}

# apply proportion that would be rh positive -----------------------------------
rh_incompatible <- merge(
  rhneg_rhod,
  rh_neg,
  by = c("location_id", "sex_id"),
  all = TRUE
)

# Loop through the draw columns to create rh incompatible births
for(i in 0:999){
  # Create product column
  rh_incompatible[[paste0("rh_inc_draw_", i)]] <- rh_incompatible[[paste0("not_covered_draw_", i)]] * (1-rh_incompatible[[paste0("rhneg_draw_", i)]])
  
  # Remove original draw columns
  rh_incompatible[[paste0("not_covered_draw_", i)]] <- NULL
  rh_incompatible[[paste0("rhneg_draw_", i)]] <- NULL
}

# apply nonfirstborn proportion ------------------------------------------------
nonfirstborn_draws <- ihme::get_draws('modelable_entity_id', gbd_id = 28665,
                                      source = 'epi',
                                      status = 'best',
                                      release_id = params_global$release_id,
                                      year_id = params_global$year_id,
                                      location_id = params_global$location_id)
nonfirstborn_draws <- nonfirstborn_draws[, -c('sex_id', # there's only both sex
                                              'modelable_entity_id',
                                              'model_version_id',
                                              'metric_id',
                                              'measure_id',
                                              'age_group_id')]

at_risk_for_rhesus <- merge(
  rh_incompatible,
  nonfirstborn_draws,
  by = c("location_id", "year_id"),
  all = TRUE
)

for(i in 0:999){
  # Create product column
  at_risk_for_rhesus[[paste0("at_risk_for_rhesus_draw_", i)]] <- at_risk_for_rhesus[[paste0("rh_inc_draw_", i)]] * at_risk_for_rhesus[[paste0("draw_", i)]]
  
  # Remove original draw columns
  at_risk_for_rhesus[[paste0("rh_inc_draw_", i)]] <- NULL
  at_risk_for_rhesus[[paste0("draw_", i)]] <- NULL
}

# save csv ---------------------------------------------------------------------
data.table::fwrite(
  at_risk_for_rhesus,
  fs::path(
    "FILEPATH"
  )
)