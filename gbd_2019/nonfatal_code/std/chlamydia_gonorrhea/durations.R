#######################################################################################
#' Purpose: Calculate remaining STI durations
#'
#######################################################################################
library(dplyr)
library(magrittr)
library(data.table)

# Read in country-specific data -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
out_dir     <- args[1]
location_id <- args[2]

constants <- readr::read_csv("FILEPATH/sti_symptomatic_splits.csv")
other     <- readr::read_csv(paste0(out_dir, "FILEPATH/other_sti_draws.csv"))
haq       <- readr::read_csv(paste0(out_dir, "FILEPATH", location_id, "_haq.csv"))


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
  readr::write_csv(subset, paste0(out_dir, infection_name, "/", location_id, "_", infection_name, ".csv"))
}

invisible(lapply(infections, save_infection, df = durations, location_id = location_id))




