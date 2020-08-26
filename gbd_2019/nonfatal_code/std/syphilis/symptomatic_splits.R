#######################################################################################
#' Purpose: Calculate the duration of syphilis, weighting by stage
#'          1) Merge haq draws with syphilis draws
#'          2) Calculate the percent symptomatic that receive treated, scaled with haq and calc 
#'             mean duration by stage
#'          3) Calculate the mean duration overall, weighting by percentage of cases that reach each stage
#'
#######################################################################################
library(dplyr)
library(magrittr)
library(data.table)


# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir     <- args[1]
location_id <- args[2]

constants <- readr::read_csv(paste0(FILEPATH,"/sti_symptomatic_splits.csv"))
syphilis  <- readr::read_csv(paste0(out_dir, "FILEPATH/syphilis_draws.csv"))
haq       <- readr::read_csv(paste0(out_dir, "FILEPATH", location_id, "_haq.csv"))

# DIAGNOSTICS
library(ggplot2)
 haq %>% 
   ggplot(aes(haq_draw)) +
   geom_histogram() +
   facet_wrap(~year_id)
# 
 syphilis %>% 
   ggplot() +
   geom_histogram(aes(symp_treat_max_draw, fill = infection)) +
   geom_histogram(aes(symp_treat_min_draw, fill = infection))


# Calculate the proportion symptomatic that receive treatment -------------

# merge on draws, expands data set * 4 and calc symptomatic and treated
merged <- left_join(haq, syphilis, by = "draw_name") %>% 
  mutate(symptomatic_treated = haq_draw * (symp_treat_max_draw - symp_treat_min_draw) + symp_treat_min_draw)

# DIAGNOSTICS
 ggplot(merged, aes(symptomatic_treated)) +
   geom_histogram(aes(fill = infection)) +
   facet_wrap(~year_id)

# Aggregate mean durations by syphilis stage into one ---------------------

# calculate mean durations
durations <- merged %>% 
  mutate(duration = sympt_draw * symptomatic_treated * dur_rx_draw    + # symptomatic patients who get treatment
           (1 - sympt_draw)                          * dur_no_rx_draw + # asymptomatic and assumed no treatment
           sympt_draw * (1 - symptomatic_treated)    * dur_no_rx_draw + # symptomatic but don't get treatment
           sympt_draw * symptomatic_treated * seroreversion)            # time to serorevert for those treated

# cast wide for calculating total syphilis duration
stage_durations <- dcast(durations, location_id + location_name + year_id + sex + draw_name ~ infection,
                         value.var = "duration") %>% 
  select(location_id:draw_name,
         primary   = `primary syphilis`,
         secondary = `secondary syphilis`,
         latent    = `latent syphilis`,
         tertiary  = `tertiary syphilis`)

# cast percent symptomatic who receive treatment long
# remember this was drawn out using HAQi
# we want both perc symptomatic and the durations long so we can use them for the aggregate mean
stage_symp_treated <- durations %>% 
  select(location_id:year_id, draw_name, sex, infection, symptomatic_treated) %>% 
  dcast(location_id + location_name + year_id + draw_name + sex ~ infection, value.var = "symptomatic_treated") %>% 
  select(location_id:sex,
         symp_treated_pri    = `primary syphilis`,
         symp_treated_sec    = `secondary syphilis`,
         symp_treated_latent = `latent syphilis`,
         symp_treated_tert   = `tertiary syphilis`)

# calculate the percentage of cases for each stage
symptomatic_constants <- select(constants, infection, sex, symptomatic) %>% 
  dcast(sex ~ infection, value.var = "symptomatic") %>% 
  select(sex, 
         const_pri  = `primary syphilis`,
         const_sec  = `secondary syphilis`,
         const_late = `latent syphilis`)

latent_to_tert <- 0.275 #Blencowes save lives tool paper 

# Note: using latent seroreversion duration for tertiary b/c we don't have 
# any data on that and it makes sense to carry that duration over
syphilis_durations <- stage_durations %>% 
  left_join(symptomatic_constants, by = "sex") %>% 
  left_join(stage_symp_treated, by = c("location_id", "location_name", "year_id", "sex", "draw_name")) %>% 
  mutate(p_cases_primary = 1,
         p_cases_secondary = p_cases_primary   * (1 - const_pri  * symp_treated_pri),
         p_cases_latent    = p_cases_secondary * (1 - const_sec  * symp_treated_sec),
         p_cases_tertiary  = p_cases_latent    * (1 - const_late * symp_treated_latent) * latent_to_tert,
         syph_duration = primary   * p_cases_primary   + 
                         secondary * p_cases_secondary + 
                         latent    * p_cases_latent    +
                         tertiary  * p_cases_tertiary)


# cut off unnecessary cols
syphilis_durations %>% 
  select(location_id, location_name, year_id, sex, draw_name, duration = syph_duration) %>% 
  readr::write_csv(paste0(out_dir, "FILEPATH", location_id, "_syphilis.csv"))





