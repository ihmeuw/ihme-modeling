#######################################################################################
#' Purpose: Calculate the duration of syphilis, weighting by stage
#'          1) Merge HAQi draws with syphilis draws
#'          2) Calculate the percent symptomatic that receive treated, scaled with HAQi and calc 
#'             mean duration by stage
#'          3) Calculate the mean duration overall, weighting by percentage of cases that reach each stage
#######################################################################################

setup()

library(dplyr)
library(magrittr)
library(data.table)


# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

constants <- readr::read_csv(paste0("FILEPATH"))
syphilis  <- readr::read_csv(paste0("FILEPATH"))


# Calculate 1000 HAQ draws from a binom distribution ----------------------
haq <- get_covariate_estimates()

# scale HAQ from 0 - 1
  mutate(mean_scaled  = (mean_value -  min(mean_value)) / (max(mean_value) - min(mean_value)),
         upper_scaled = (upper_value - min(mean_value)) / (max(mean_value) - min(mean_value)),
         lower_scaled = (lower_value - min(mean_value)) / (max(mean_value) - min(mean_value)),
         se = (upper_scaled - lower_scaled) / (2 * 1.96)) %>% 
  select(location_id, location_name, year_id, haq = mean_scaled, se)

cross <- function(data1, data2) {
  df1_prep <- mutate(data1, merge_var = 1)
  df2_prep <- mutate(data2, merge_var = 1)
  
  left_join(df1_prep, df2_prep, by = "merge_var") %>% 
    select(-merge_var)
}

draw_names <- as.data.frame(paste0("draw_", 0:999))
draw_names <- setNames(draw_names, "draw_name")

haq_long <- cross(haq, draw_names)

# take 1000 draws of a binomial distribution
binomial_draws <- function(size, std_error, mean) {
  n <- mean * (1 - mean) / std_error ^ 2
  rbinom(size, round(n), mean) / round(n)
}

haq_long <- as.data.table(haq_long)

haq_long[, haq_draw := rnorm(n = nrow(haq_long), mean = haq, sd = se)]


# Calculate the proportion symptomatic that receive treatment -------------

# merge on draws, expands data set * 4 and calc symptomatic and treated
merged <- left_join(haq, syphilis, by = "draw_name") %>% 
  mutate(symptomatic_treated = haq_draw * (symp_treat_max_draw - symp_treat_min_draw) + symp_treat_min_draw)


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

latent_to_tert <- 0.275

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

syphilis_durations %>% 
  select(location_id, location_name, year_id, sex, draw_name, duration = syph_duration) %>% 
  readr::write_csv(paste0(out_dir, "syphilis/", location_id, "_syphilis.csv"))








