###COPD Sex Split

#load in library and packages
library(dplyr)
library(knitr)
library(ggplot2)
library(kableExtra)
library(purrr)
library(data.table)
library(openxlsx)
library(tidyr)
library(zoo)
library(stringr)

invisible(sapply(list.files("filepath", full.names = T), source))

#' @param topic_name string, for output file names
#' @param output_dir string, filepath to directory to save outputs (including MR-BRT model, post sex split data, vetting plots, etc). This should end with a "/"
#' @param bundle_version_id only input if pulling bundle version to get sex-specific data for sex splitting model, and to get data that needs to be sex split (must have 'splitting' variable)
#' @param data_all_csv string, if pulling in all the data (sex-specific and both sex) in a csv format (must have 'splitting' variable)
#' @param data_to_split_csv string, if pulling data that needs to be sex split via csv
#' @param data_raw_sex_specific_csv string, if pulling sex-specific data for sex ratio model via csv
#' @param nids_to_drop numeric vector, list of NIDs which the user does want used to inform the sex split model.
#' @param cv_drop string, study-level covariates in sex-specific dataset that user does not want to use to find sex matches between observations. Covariates must be of the form "cv_xxx"
#' @param mrbrt_model string, identifies filepath of previously run MR-BRT sex ratio model. If null, a new model will be run
#' @param mrbrt_model_age_cv TRUE if user is running a new MR-BRT model with age covariate, default is FALSE
#' @param mrbrt_model_linear_age_cv TRUE if user is running a new MR-BRT model with age covariate and would like that age covariate to be linear, default is TRUE
#' @param mrbrt_model_age_spline_knots numeric vector specifying the location (and therefore number) of knots for the nonlinear MR-BRT model age covariate, default is c(0, 0.25, 0.5, 0.75, 1)
#' @param mrbrt_model_age_spline_degree numeric, specifying the degree for the MR-BRT model age covariate (1 = linear, 2 = quadratic, 3 = cubic), default is 3
#' @param mrbrt_model_age_spline_linear_tail_left TRUE if user would like the MR-BRT model age covariate to have a left linear tail, default is TRUE
#' @param mrbrt_model_age_spline_linear_tail_right TRUE if user would like the MR-BRT model age covariate to have a right linear tail, default is TRUE
#' @param release_id Release ID, for pulling in population data. No default
#' @param measure Character vector of measures that user wants to sex-split (prevalence, incidence, proportion, and/or continuous)
#' @param vetting_plots logical, Binary variable for if user wants vetting plots generated or not, default is true
#'
#' @return
#' @export
#'
#' @examples
sex_split <- function(topic_name, output_dir, bundle_version_id = NULL, data_all_csv = NULL, data_to_split_csv = NULL,
                      data_raw_sex_specific_csv = NULL, nids_to_drop = NULL, cv_drop = NULL, mrbrt_model = NULL,
                      mrbrt_model_age_cv = FALSE, mrbrt_model_linear_age_cv = TRUE, mrbrt_model_age_spline_knots = c(0, 0.25, 0.5, 0.75, 1),
                      mrbrt_model_age_spline_degree = 3, mrbrt_model_age_spline_linear_tail_left = TRUE, mrbrt_model_age_spline_linear_tail_right = TRUE,
                      release_id = NULL, measure = NULL, vetting_plots = TRUE) 

#copd global
copd_age_pattern <- get_draws(
  gbd_id_type = "modelable_entity_id",
  gbd_id = 24543,
  source = "epi",
  location_id = 1,
  age_group_id = age_ids,
  sex_id = c(1,2),
  measure_id = 5, #prevalence
  metric_id = 3, 
  version_id = 750463, # 2021 best
  release_id = 9)


#copd sex split
sex_split(topic_name = "copd",
          output_dir = "filepath",
          bundle_version_id = NULL,
          data_all_csv = NULL,
          data_to_split_csv = "/filepath/copd_both_sex.csv",
          data_raw_sex_specific_csv = "filepath/copd_sex_specific_prev_inc.csv",
          nids_to_drop = c(nids) #nids to drop  

          cv_drop = c(),
          mrbrt_model = NULL,
          mrbrt_model_age_cv = TRUE,
          mrbrt_model_age_spline_degree = 3,
          mrbrt_model_age_spline_knots = c(0, .21, .27, .68, 1),
          mrbrt_model_linear_age_cv = FALSE,
          #mrbrt_model_age_spline_linear_tail_left = FALSE,
          #mrbrt_model_age_spline_linear_tail_right = TRUE,
          release_id = 16,
          measure = c("prevalence", "incidence"),
          vetting_plots = TRUE)


#gold1
gold1_bv <- gold1_bv %>%
  mutate(group = "",
       specificity = "",
       group_review = "")


gold1_both <- gold1_bv %>%
  filter(sex == "Both")
write.csv(gold1_both, file = "filepath")

gold1_sex_spec <- gold1_bv %>%
  filter(sex != "Both")
write.csv(gold1_sex_spec, file = "filepath/gold1_sex_specific.csv")


sex_split(topic_name = "Gold I",
          output_dir = "filepath",
          bundle_version_id = NULL,
          data_all_csv = NULL,
          data_to_split_csv = "filepath/gold1_both.csv",
          data_raw_sex_specific_csv = "filepath/gold1_sex_specific.csv",
          cv_drop = c(),
          mrbrt_model = NULL,
          mrbrt_model_age_cv = FALSE,
          mrbrt_model_linear_age_cv = FALSE,
         # mrbrt_model_age_spline_knots = c(0, .33, 1),
          #mrbrt_model_age_spline_linear_tail_left = FALSE,
          #mrbrt_model_age_spline_linear_tail_right = TRUE,
          release_id = 16,
          measure = c("proportion"),
          vetting_plots = TRUE)

#gold II
gold2_bv <- gold2_bv %>%
  mutate(group = "",
         specificity = "",
         group_review = "")

gold2_both <- gold2_bv %>%
  filter(sex == "Both")
write.csv(gold2_both, file = "filepath/gold2_both.csv")

gold2_sex_spec <- gold2_bv %>%
  filter(sex != "Both")
write.csv(gold2_sex_spec, file = "filepath/gold2_sex_specific.csv")


sex_split(topic_name = "Gold II",
          output_dir = "/filepath/",
          bundle_version_id = NULL,
          data_all_csv = NULL,
          data_to_split_csv = "/filepath/gold2_both.csv",
          data_raw_sex_specific_csv = "/filepath/gold2_sex_specific.csv",
          cv_drop = c(),
          mrbrt_model = NULL,
          mrbrt_model_age_cv = FALSE,
          #mrbrt_model_linear_age_cv = FALSE,
          # mrbrt_model_age_spline_knots = c(0, .33, 1),
          #mrbrt_model_age_spline_linear_tail_left = FALSE,
          #mrbrt_model_age_spline_linear_tail_right = TRUE,
          release_id = 16,
          measure = c("proportion"),
          vetting_plots = TRUE)


#gold3
gold3_bv <- gold3_bv %>%
  mutate(group = "",
         specificity = "",
         group_review = "")

gold3_both <- gold3_bv %>%
  filter(sex == "Both")
write.csv(gold3_both, file = "/filepath/gold3_both.csv")

gold3_sex_spec <- gold3_bv %>%
  filter(sex != "Both")
write.csv(gold3_sex_spec, file = "/filepath/gold3_sex_specific.csv")


sex_split(topic_name = "Gold III",
          output_dir = "/filepath/",
          bundle_version_id = NULL,
          data_all_csv = NULL,
          data_to_split_csv = "filepath/gold3_both.csv",
          data_raw_sex_specific_csv = "/filepath/gold3_sex_specific.csv",
          cv_drop = c(),
          mrbrt_model = NULL,
          mrbrt_model_age_cv = FALSE,
          #mrbrt_model_linear_age_cv = FALSE,
          # mrbrt_model_age_spline_knots = c(0, .33, 1),
          #mrbrt_model_age_spline_linear_tail_left = FALSE,
          #mrbrt_model_age_spline_linear_tail_right = TRUE,
          release_id = 16,
          measure = c("proportion"),
          vetting_plots = TRUE)
