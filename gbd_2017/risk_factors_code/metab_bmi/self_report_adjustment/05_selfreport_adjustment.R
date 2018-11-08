#
# 05_selfreport_adjustment.R
#
# April 2018
#

library(dplyr)
library(data.table)
library(lme4)

rm(list = ls())

j_drive <- ifelse(Sys.info()["sysname"] == "Linux", "/home/j/", "J:/")

#####
# Parameters

dev <- FALSE

if (dev) {
  date <- "2018_01_29"
} else {
  args <- commandArgs(trailingOnly = TRUE)
  date <- args[1]
}


## get location metadata

df_geo <- readRDS("FILEPATH/geo_hierarchy.RDS") %>%
  select(ihme_loc_id, location_id, super_region_name, region_name)

## get age metadata
df_age_groups <- readRDS("FILEPATH/df_age_groups.RDS")




#####
for (indic in c("overweight_mean", "obese_mean")) {

  # create object to store the results, to row-bind male and female results later
  sex_results <- vector("list", length = 2)
  names(sex_results) <- as.character(1:2)

  # run separate models for males and females
  for (sex_id_var in 1:2) {
    
    dev_loop <- FALSE
    if (dev_loop) {
      indic <- "overweight_mean" # dev
      sex_id_var <- 1 # dev
    }
    
    # set up naming conventions
    if (indic == "overweight_mean") shortname <- "ow"; filename <- "overweight"
    if (indic == "obese_mean") shortname <- "ob"; filename <- "obese"
    if (indic == "bmi_mean") shortname <- "bmi"; filename <- "bmi"
    sexname <- ifelse(sex_id_var == 1, "Male", "Female")
    
    # otherwise, use the post-age-sex splitting data
    dat_path <- paste0(j_drive, "FILEPATH", date, "/to_model/")
    df_in <- as.data.frame(fread(paste0(dat_path, shortname, "_final.csv")))
    
    # prep the data for self-report adjustment analysis
    df <- df_in %>%
      left_join(df_geo, by = "location_id") %>%
      left_join(df_age_groups, by = "age_group_id") %>%
      mutate(
        # generate the measured indicator
        measured = ifelse(cv_diagnostic == "measured", 1, 0),
        # generate time period indicator (splitting into 10 yr periods with a remainder)
        time_period4 = "",
        time_period4 = ifelse(year_id %in% 1980:1989, "1980-1989", time_period4),
        time_period4 = ifelse(year_id %in% 1990:1999, "1990-1999", time_period4),
        time_period4 = ifelse(year_id %in% 2000:2009, "2000-2009", time_period4),
        time_period4 = ifelse(year_id %in% 2010:2017, "2010-2017", time_period4),
        time_period4 = as.factor(time_period4),
        time_period2 = "",
        time_period2 = ifelse(time_period4 %in% c("1980-1989", "1990-1999"), "1980-1999", time_period2),
        time_period2 = ifelse(time_period4 %in% c("2000-2009", "2010-2017"), "2000-2017", time_period2),
        age_mid = ((age_start + age_end) / 2) + 0.5,
        age_group_5yr = as.factor(age_mid),
        age_group_10yr = cut(age_mid, breaks = seq(15, 75, by = 10)) ) %>%
      filter(age_start >= 15) %>% # running the self-report adjustment only for adults
      filter(sex_id == sex_id_var) # subset to a particular sex
    
    
    # # transform the outcome variable, log or logit
    # -- this is the dataset that will take the predicted difference in [measured-selfreport]
    if (indic == "bmi_mean") {

      df$data_transformed <- log(df$data)

    } else if (indic %in% c("overweight_mean", "obese_mean")) {

      df <- df %>%
        mutate(
          data_tmp = data,
          data_tmp = ifelse(data_tmp == 1, 0.999, data_tmp),
          data_tmp = ifelse(data_tmp == 0, 0.001, data_tmp),
          data_transformed = log(data_tmp / (1-data_tmp)) ) %>%
        select(-data_tmp)
    }
    
    
    # match the data, if applicable
    # -- use complete matching; within a matched group, 
    #    each unique combination of self-reported and measured data points
    # -- the modeled outcome is measured minus self-reported [bmi, overweight, obese]
    
    df_matched <- df
    
    matched_data <- TRUE
    
    if (matched_data) {
      
      matching_vars <- c("sex_id", "age_group_5yr", "time_period2", "ihme_loc_id")
      
      tmp_meas <- df_matched %>%
        filter(cv_diagnostic == "measured")
      
      tmp_sr <- df_matched %>%
        filter(cv_diagnostic == "self-report") %>%
        select_(.dots = c(matching_vars, "data_transformed", "data"))
      
      tmp <- full_join(tmp_meas, tmp_sr, by = matching_vars) %>%
        mutate(
          diff = data.x - data.y,
          diff_transformed = data_transformed.x - data_transformed.y )
      
      df_matched <- tmp %>%
        filter(!is.na(diff)) %>%
        mutate(
          super_region_name = as.factor(super_region_name),
          region_name = as.factor(region_name)
        )
    }
    
    
    # perform the adjustment
    if (indic == "bmi_mean") {
      # for bmi_mean, absolute difference on the linear (non-transformed) scale
      fit1 <- lmer(
        diff ~ age_mid + (1 | super_region_name/region_name), 
        data = df_matched
      )
      df2 <- df %>%
        mutate(
          # making predictions as though the year is 2000-2009
          adj = predict(fit1, newdata = ., allow.new.levels = TRUE),
          data2 = ifelse(cv_diagnostic == "self-report",  data + adj, data) )
      
    } else if (indic %in% c("overweight_mean", "obese_mean")) {
      
      # for proportions, absolute difference on the logit scale
      # then take inverse logit after adjustment
      fit1 <- lmer(
        diff_transformed ~ age_mid + (1 | super_region_name/region_name), 
        data = df_matched
      )
      df2 <- df %>%
        mutate(
          adj = predict(fit1, newdata = ., allow.new.levels = TRUE),
          # predict self-report adjustment for each row
          data_transformed2 = ifelse(
            cv_diagnostic == "self-report",  data_transformed + adj, data_transformed) )
      
      df2$data2 <- exp(df2$data_transformed2) / (1 + exp(df2$data_transformed2)) # inverse logit
      
    }
    
    
    #-- check the result
    tmp <- df2[, c("cv_diagnostic", "data", "data2", "data_transformed", "adj")]
    
    # out<-out[,.(nid, year_id, age_group_id, sex_id, location_id, data, variance, sample_size, cv_urbanicity, cv_diagnostic)]
    # out<-out[, lapply(.SD, mean, na.rm=TRUE), by=c("location_id", "year_id", "age_group_id", "sex_id", "nid", "cv_urbanicity", "cv_diagnostic"), .SDcols=c("data", "variance", "sample_size") ]
    df3 <- df2 %>%
      select(
        nid, year_id, age_group_id, sex_id, location_id, 
        data = data2, variance, sample_size, cv_urbanicity, cv_diagnostic, me_name)
    
    # add back the child results, keeping only measured values (no need for self-report adjustment)
    df_child <- df_in %>%
      filter(age_group_id <= 7 & cv_diagnostic == "measured")
    
    df4 <- bind_rows(df3, df_child)
    
    sex_results[[as.character(sex_id_var)]] <- df4
    
  }
  
  # combine results from separate male and female models
  out_final <- bind_rows(sex_results)
  out_path <- paste0(j_drive, "FILEPATH", date, "/adjusted/")
  write.csv(out_final, file = paste0(out_path, shortname, "_final.csv"), row.names = FALSE)
    # age_end location_id year_id age_start data variance
    # export delimited using "`prefix'/WORK/05_risk/risks/metab_bmi/pipeline/datasets/`date'/adjusted/wide_full_`shortname'_child.csv", replace
  
  
}
