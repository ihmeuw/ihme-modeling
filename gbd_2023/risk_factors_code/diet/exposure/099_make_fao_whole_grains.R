rm(list = ls())

library(ggplot2)
library(data.table)
library(openxlsx)
library(reshape2)
library(dplyr)
library("data.table")

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is mainly for running the script individually)

if(interactive()) {
  gbd_round <- 'gbd2019'
  version <- '2019_5' 
} else {
  args <- commandArgs(trailingOnly = TRUE)
  version <- args[1]
  gbd_round <- args[2]
}


data_path <- paste0("FILEPATH") # this is generally where data will be output throughout the pipeline
output_loc <- paste0("FILEPATH")
compiled_data <- paste0("FILEPATH")
proc_ratio_output <- paste0("FILEPATH")
whole_grains_output <- paste0("FILEPATH")

# Import the CSV file and drop the back_se and variance columns
df <- read.csv(compiled_data) %>% 
  dplyr::select(-c("back_se", "variance"))

# Filter rows where ihme_risk is "diet_refined_grains"
refined_grains_unadj <- copy(df %>% filter(ihme_risk == "diet_refined_grains")) # copy required because setnames modifies by reference

write_csv(refined_grains_unadj, paste0(output_loc, "refined_grains_unadj.csv"))

# Filter rows where ihme_risk is "diet_total_grains"
total_grains <- df %>% filter(ihme_risk == "diet_total_grains") %>%
  setnames(c("gpr_mean"), c("total_grain_mean"))

# Generate the total_grain_se column
total_grains$total_grain_se <- (total_grains$gpr_upper - total_grains$gpr_lower) / (2 * 1.96)

dftest <- total_grains

# Generate the total_grain_draw columns
for(d in 0:999){
  dftest <- dftest %>%
    mutate(!!paste0("total_grain_draw_", d) := rnorm(n(), total_grain_mean, total_grain_se))
}

# Drop the gpr and total_grain columns
dftest <- dftest %>% dplyr::select(-starts_with("gpr"), -total_grain_mean, -total_grain_se)

# Merge with the refined_grains_unadj dataframe
dftest_merge <- merge(dftest, refined_grains_unadj, by = c("location_id", "year_id", "age_group_id", "sex_id"))

dftest_merge %>%
  setnames(c("gpr_mean"), c("refined_grain_mean"))

# Generate the refined_grain_se column
dftest_merge$refined_grain_se <- (dftest_merge$gpr_upper - dftest_merge$gpr_lower) / (2 * 1.96)

# Generate the refined_grain_draw columns
for(d in 0:999){
  dftest_merge <- dftest_merge %>%
    mutate(!!paste0("refined_grain_draw_", d) := rnorm(n(), refined_grain_mean, refined_grain_se))
}

dftest_merge_proc <- dftest_merge

# Drop the gpr and refined_grain columns
dftest_merge_proc <- dftest_merge_proc %>% dplyr::select(-starts_with("gpr"), -refined_grain_mean, -refined_grain_se)

dftest_merge_proc <- as.data.table(dftest_merge_proc)

# proc ratio
for(d in 0:999){
  dftest_merge_proc[, paste0("proc_ratio_", d) := get(paste0("refined_grain_draw_", d)) / get(paste0("total_grain_draw_", d))]
  dftest_merge_proc[, c(paste0("total_grain_draw_", d), paste0("refined_grain_draw_", d)) := NULL]
  dftest_merge_proc[get(paste0("proc_ratio_", d)) > 1, paste0("proc_ratio_", d) := NA]
}

final_df <- dftest_merge_proc

# drop columns containing .x and .y
final_df <- final_df %>%
  dplyr::select(-contains(".x"), -contains(".y"))

final_df$ihme_risk <- 'grain_proc_ratio'

# Generate gpr_mean, gpr_upper, and gpr_lower
proc_ratio_cols <- grep("proc_ratio_", names(final_df), value = TRUE)
final_df$gpr_mean <- rowMeans(final_df[, ..proc_ratio_cols], na.rm = TRUE)
final_df$gpr_upper <- apply(final_df[, ..proc_ratio_cols], 1, function(x) quantile(x, probs = 0.975, na.rm = TRUE))
final_df$gpr_lower <- apply(final_df[, ..proc_ratio_cols], 1, function(x) quantile(x, probs = 0.025, na.rm = TRUE))

# drop proc_ratio columns
final_df <- final_df %>%
  dplyr::select(-contains('proc_ratio')) %>%
  drop_na(gpr_mean) %>%
  mutate(back_se = (gpr_upper - gpr_lower) / (2 * 1.96),
         variance = back_se^2)

# save
write_csv(final_df, paste0(output_loc, "proc_ratio_output.csv"))

# whole grains
wg_df <- as.data.table(dftest_merge)

for (d in 0:999) {
  wg_df[, paste0("wg_draw_", d) := get(paste0("total_grain_draw_", d)) - get(paste0("refined_grain_draw_", d))]
  
  # Drop total_grain_draw_d and refined_grain_draw_d
  wg_df[, c(paste0("total_grain_draw_", d), paste0("refined_grain_draw_", d)) := NULL]
  
}

# drop columns containing .x and .y
wg_df <- wg_df %>%
  dplyr::select(-contains(".x"), -contains(".y"))

wg_df$ihme_risk <- 'diet_whole_grains'
wg_df$gbd_cause <- 'whole_grains_g_unadj'

# Generate gpr_mean, gpr_upper, and gpr_lower
proc_ratio_cols <- grep("wg_draw_", names(wg_df), value = TRUE)
wg_df$gpr_mean <- rowMeans(wg_df[, ..proc_ratio_cols], na.rm = TRUE)
wg_df$gpr_upper <- apply(wg_df[, ..proc_ratio_cols], 1, function(x) quantile(x, probs = 0.975, na.rm = TRUE))
wg_df$gpr_lower <- apply(wg_df[, ..proc_ratio_cols], 1, function(x) quantile(x, probs = 0.025, na.rm = TRUE))

# drop proc_ratio columns
wg_df <- wg_df %>%
  dplyr::select(-contains('wg_draw_')) %>%
  drop_na(gpr_mean) %>%
  mutate(back_se = (gpr_upper - gpr_lower) / (2 * 1.96),
         variance = back_se^2)

# save
write_csv(wg_df, paste0(output_loc, "whole_grains_output.csv"))
