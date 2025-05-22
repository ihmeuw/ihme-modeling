################################################################################
## model rh negativity using lme4 package ######################################
################################################################################

params_global <- readr::read_rds("params_global.rds")
library(data.table)
library(dplyr)
library(tidyr)
library(lme4)
locs <- ihme::get_location_metadata(location_set_id = 35, release_id = 16)
locs_countryid <- locs %>%
  mutate(
    country_id = case_when(
      level == 3 ~ location_id,
      level == 4 ~ parent_id,
      grepl("Urban", location_name) | grepl("Rural", location_name) ~ as.integer(163),
      parent_id == 4749 ~ parent_id
    )
  )

# prep data for modeling -------------------------------------------------------
crosswalk_version_id <- 46949
rhneg_crosswalk <- ihme::get_crosswalk_version(crosswalk_version_id = crosswalk_version_id)

rhneg <- merge(
  rhneg_crosswalk,
  locs_countryid,
  by = c("location_id", "location_name"),
  all.x = TRUE,
  all.y = FALSE
)

rhneg <- rhneg[, .(mean, lower, upper, standard_error, sample_size,
                   location_name, location_id, region_id, region_name,
                   super_region_id, super_region_name, country_id)]

# set up for binomial model ----------------------------------------------------
rhneg <- rhneg %>%
  mutate(negative = mean * sample_size,
         positive = (1-mean) * sample_size)

# convert values to numbers for binomial model
rhneg <- rhneg %>%
  mutate(
    negative = floor(negative),
    positive = floor(positive)
  )

# model ------------------------------------------------------------------------
model_glmer <- glmer(cbind(negative, positive) ~ 1 + (1 | super_region_name/region_name/country_id), data = rhneg, family = binomial("logit"))
summary(model_glmer)

# create pred frame ------------------------------------------------------------
pred_frame <- locs_countryid[,.(location_name, location_id, region_id, region_name,
                                super_region_id, super_region_name, country_id)]
pred_frame <- pred_frame[location_id %in% params_global$location_id]

# assigning the New Zealand Maori population with the Oceania region model fit as they are an indigenous Polynesian people
pred_frame <- pred_frame %>%
  mutate(
    super_region_name = if_else(location_name == "New Zealand Maori population", "Southeast Asia, East Asia, and Oceania", super_region_name),
    super_region_id = if_else(location_name == "New Zealand Maori population", 4, super_region_id),
    region_name = if_else(location_name == "New Zealand Maori population", "Oceania", region_name),
    region_id = if_else(location_name == "New Zealand Maori population", 21, region_id),
    country_id = if_else(location_name == "New Zealand Maori population", location_id, country_id)
  ) #note: reassigned the location_id as the country_id so it doesn't get pulled into country fit

# column names for draws -------------------------------------------------------
new_names <- paste0("draw_logit_", 0:999)

# preds with nested random effects to country level ----------------------------
# for locations where we have country data
pred_frame_country <- pred_frame[country_id %in% rhneg$country_id]
pred_FUN_country <- function(model_glmer) {
  predict(model_glmer, newdata = pred_frame_country, re.form = ~ (1 | super_region_name/region_name/country_id))
}
Rhneg_draws_object_country <- bootMer(model_glmer, pred_FUN_country, nsim = 1000, use.u = TRUE, type = "parametric", re.form = NULL)
Rhneg_draws_country <- Rhneg_draws_object_country$t |> data.frame()
Rhneg_draws_country <- as.data.frame(t(Rhneg_draws_country))
colnames(Rhneg_draws_country)[1:1000] <- new_names
Rhneg_draws_country <- bind_cols(Rhneg_draws_country, pred_frame_country)

# preds with nested random effects to region level -----------------------------
# for locations where we don't have country data
pred_frame_region <- pred_frame[!pred_frame$country_id %in% rhneg$country_id, ]
pred_FUN_region <- function(model_glmer) {
  predict(model_glmer, newdata = pred_frame_region, re.form = ~ (1 | super_region_name/region_name))
}
Rhneg_draws_object_region <- bootMer(model_glmer, pred_FUN_region, nsim = 1000, use.u = TRUE, type = "parametric", re.form = NULL)
Rhneg_draws_region <- Rhneg_draws_object_region$t |> data.frame()
Rhneg_draws_region <- as.data.frame(t(Rhneg_draws_region))
colnames(Rhneg_draws_region)[1:1000] <- new_names
Rhneg_draws_region <- bind_cols(Rhneg_draws_region, pred_frame_region)

# combine draws ----------------------------------------------------------------
Rhneg_draws <- rbind(Rhneg_draws_country, Rhneg_draws_region)

# inverse logit transformation
for(i in 0:999) {
  # Create product column
  Rhneg_draws[[paste0("draw_", i)]] <- nch::inv_logit(Rhneg_draws[[paste0("draw_logit_", i)]])
  
  # Remove original draw columns
  Rhneg_draws[[paste0("draw_logit_", i)]] <- NULL
}

# drop country_id column and reassign NZ Maori to correct location IDs ---------
Rhneg_draws <- Rhneg_draws %>%
  select(-country_id)

Rhneg_draws <- Rhneg_draws %>%
  mutate(
    super_region_name = if_else(location_name == "New Zealand Maori population", "High-income", super_region_name),
    super_region_id = if_else(location_name == "New Zealand Maori population", 64, super_region_id),
    region_name = if_else(location_name == "New Zealand Maori population", "Australasia", region_name),
    region_id = if_else(location_name == "New Zealand Maori population", 70, region_id)
  )

# prep for saving as ME --------------------------------------------------------
Rhneg_draws <- Rhneg_draws %>% mutate(
  age_group_id = nch::id_for("age_group", "Birth"),
  measure_id = nch::id_for("measure", "prevalence"),
  metric_id = nch::id_for("metric", "Rate"),
  sex_id = nch::id_for("sex", "Male")
)
# repeat for females
Rhneg_draws <- bind_rows(Rhneg_draws, Rhneg_draws %>%
                           mutate(sex_id = nch::id_for("sex", "Female")))

# repeat for every required year
Rhneg_draws <- Rhneg_draws %>%
  mutate(year_id = list(params_global$year_id)) %>%
  unnest(cols = c(year_id))

data.table::fwrite(
  Rhneg_draws,
  "FILEPATH"
)

# save model results -----------------------------------------------------------
index_df <- ihme::save_results_epi(
  input_dir = "FILEPATH",
  input_file_pattern = 'all_draws.csv',
  modelable_entity_id = 32006,
  description = "GBD23 - rhneg prevalence lme4 model results with maori adjustment",
  measure_id = nch::id_for("measure", "prevalence"),
  metric_id = nch::id_for("metric", "Rate"),
  release_id = nch::id_for("release", "GBD 2023"),
  mark_best = TRUE,
  birth_prevalence = TRUE,
  bundle_id = 389,
  crosswalk_version_id = 46949
)