#######################################################################################
#' Purpose: Calculate STI durations
#'
#######################################################################################
library(dplyr)
library(magrittr)
library(data.table)

# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir     <- args[1]
location_id <- args[2]

constants <- readr::read_csv("FILEPATH")
other     <- readr::read_csv(paste0(out_dir, "FILEPATH"))


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
merged <- left_join(haq, other, by = "draw_name") %>% 
  mutate(symptomatic_treated = haq_draw * (symp_treat_max_draw - symp_treat_min_draw) + symp_treat_min_draw)

# DIAGNOSTICS
library(ggplot2)
ggplot(merged, aes(symptomatic_treated)) +
  geom_histogram(aes(fill = infection)) +
  facet_wrap(~year_id)


# Aggregate mean durations by other sti stage into one ---------------------

# calculate mean durations
durations <- merged %>% 
  mutate(duration = sympt_draw * symptomatic_treated * dur_rx_draw / 2 + # symptomatic patients who get treatment
           (1 - sympt_draw)                          * dur_no_rx_draw  + # asymptomatic and assumed no treatment
           sympt_draw * (1 - symptomatic_treated)    * dur_no_rx_draw)   # symptomatic but don't get treatment




# Save by infection ------------------------------------------------------

infections <- c("chlamydia",   "gonorrhea" ,  "trichomonas")
durations <- as.data.table(durations)

save_infection <- function(infection_name, df, location_id) {
  cat(paste0("Saving ", infection_name, " for location id ", location_id, "\n"))
  subset <- df[infection == infection_name, ]
  readr::write_csv(subset, "FILEPATH")
}

invisible(lapply(infections, save_infection, df = durations, location_id = location_id))




