## Purpose: Master script that contains the 2019 MR-BRT crosswalk process.

date<-gsub("-", "_", Sys.Date())

pacman::p_load(ggplot2, data.table, gtools, boot)

# wrappers, cleaning functions
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

# central computation functions - interact with database
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

mad.stats <- function(x) {
  c(
    mad = mad(x),
    median = median(x)
  )
}

"%ni%" <- Negate("%in%")

master_mrbrt_crosswalk <- function(
  model_abbr,
  save_crosswalk_result_file,
  data,
  alternate_defs=NULL,
  temp_folder,
  xw_measure,
  upload_crosswalk_version=F,
  bundle_version_id=NULL,
  sex_split_only=F,
  age_overlap=5,
  year_overlap=5,
  age_range=10,
  year_range=10,
  subnat_to_nat=F,
  sex_age_overlap=0,
  sex_year_overlap=0,
  sex_age_range=0,
  sex_year_range=0,
  sex_fix_zeros=T,
  reference_def=NULL,
  addl_x_covs=NULL,
  use_lasso=F,
  id_vars="id_var",
  spline_covs=NULL,
  opt_method="trim_remL",
  trim_pct=0.1,
  include_x_intercept=F,
  sex_include_x_intercept=T,
  sex_covs="age_scaled",
  decomp_step="step2",
  gbd_round_id=6,
  logit_transform=NULL
) {

  data <- data[is_outlier == 0 & measure == xw_measure]

  message(paste0("You ", ifelse(nrow(data[sex=="Both",])>0, "do", "don't"), " need to sex-split."))
  message(paste0("You ", ifelse(sex_split_only, "won't be", "will also be"), " crosswalking."))

  ########### Naming variables #####################
  ##################################################

  ## We can either use logit transformation or log transformation - but are encouraged to use logit unless prevalence is less than 50%. This ensures
  ## that prevalence doesn't go over 1.
  if(logit_transform) {
    response <- "logit_diff"
    data_se <- "logit_diff_se"
  } else {
    response <- "ratio_log"
    data_se <- "ratio_se_log"
  }

  sex_split_response <- "ratio_log"
  sex_split_data_se <- "ratio_se_log"

  ## If we don't have a "reference" column, making one so we can figure out what we need to XW
  if (!sex_split_only) {
    if ((is.null(reference_def) | (missing(reference_def)))) {
      if (length(alternate_defs) > 1) data[rowSums(data[, ..alternate_defs])==0, reference := 1]
      if (length(alternate_defs) == 1) data[get(alternate_defs) == 0, reference := 1]
      data[is.na(reference), reference := 0]
      reference_def <- "reference"
    }
  }

  ## NRVDs don't have "note_modeler" as a column, which we need later.
  if (!("note_modeler" %in% names(data))) data[, note_modeler := ""]

  ############## Working on sex-splitting: #######################
  ################################################################

  num_both_sex <- nrow(data[sex=="Both"])

  if (nrow(data[sex=="Both",]) > 0) {

    ## Give a heads up about how many sex-specific data points you have to XW.
    num_both_sex <- nrow(data[sex=="Both"])
    message(paste0("Starting to sex-split. You have ", num_both_sex, " both-sex data points to be split."))

    ## Finding matches:
    sex_matches <- sex_match(data, reference_def = reference_def, alternate_defs = alternate_defs,
                             age_overlap = 0, year_overlap=0, fix_zeros = T, exact_match = T, quiet=T)

    ## Pull out this object to return later.
    matches_result <- sex_matches$matches

    ## Log transform the matched data points.
    sex_matches <- log_transform_mrbrt(sex_matches$data)

    ## Adjust the age, sex, etc., make an ID variable
    sex_matches <- adj_mr_brt(sex_matches, sex = F)
    sex_matches[, id_var := .GRP, by=.(nid_ref, nid_alt)]

    ## Pull out both-sex data and sex-specific data. both_sex is used for predicting and sex_specific will be the data we use.
    both_sex <- adj_mr_brt(data[sex=="Both",], original_data = T, year = F, sex=F, fix_zeros = F)
    both_sex[, id := 1:nrow(both_sex)]
    sex_specific <- data[sex!="Both",]
    n <- names(sex_specific)

    ## Launch the MR-BRT model itself
    sex_results <- launch_mr_brt(data=sex_matches, response=sex_split_response, data_se=sex_split_data_se, id_vars=id_vars,
                                 x_covs=sex_covs,include_x_intercept=sex_include_x_intercept,
                                 use_lasso=use_lasso, spline_covs=spline_covs,
                                 opt_method=opt_method, trim_pct=trim_pct,
                                 temp_folder=temp_folder, delete_model = F, subfolder_name = "sex_split")

    ## Pull out covariates to return later.
    sex_covariates <- sex_results$model_coefs

    ## Predict onto original data (the both-sex data points)
    sex_preds <- predict_mr_brt(sex_results, both_sex, temp_folder=temp_folder, subfolder_name = "sex_preds")
    sex_draws <- data.table(sex_preds$model_draws)

    ## Using the draws to get the predicted ratio & SE of male/female
    sex_draws[, c("X_intercept", "Z_intercept") := NULL]
    draws <- names(sex_draws)[grepl("draw", names(sex_draws))]
    sex_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]

    ## We need the population for each data point. Instead of pooling the population (for ex if you were age 3-10) for all years, we just take the midpoint (3-10 would become 6)
    both_sex[, year_mid := (year_start + year_end)/2]
    both_sex[, year_floor := floor(year_mid)]
    both_sex[, age_floor:= floor(age_mid)]

    ## Pull out population data. We need age- sex- location-specific population.
    pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = decomp_step, year_id = unique(floor(both_sex$year_mid)),
                          location_id=unique(both_sex$location_id), single_year_age = T)
    ids <- get_ids("age_group") ## age group IDs
    pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
    # Age group names are sometimes characters... I only really need the ones that are numeric on their own.
    pop$age_group_name <- as.numeric(pop$age_group_name)
    pop <- pop[!(is.na(age_group_name)) | age_group_id == 28]
    pop[age_group_id == 28, age_group_name := 0]
    pop[, age_group_id := NULL]

    ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
    both_sex <- merge(both_sex, pop[sex_id==3,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_both")
    both_sex <- merge(both_sex, pop[sex_id==1,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_male")
    both_sex <- merge(both_sex, pop[sex_id==2,], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
    setnames(both_sex, "population", "population_female")

    ## There are some rows where popuation is NA - get rid of them
    sex_draws[, id := 1:nrow(sex_draws)]
    both_sex[, id := 1:nrow(both_sex)]
    both_sex <- both_sex[!is.na(population_both)]
    ids <- unique(both_sex$id)
    num <- nrow(data[sex=="Both",]) - length(ids)
    sex_draws <- sex_draws[id %in% ids,]
    if (num > 0) message(paste0("WARNING: You lost ", num, " both-sex data point(s) because we couldn't find a population for that point."))

    ## Calculate means that will be reported in the note_modeler column
    ratio_mean <- round(sex_draws[, rowMeans(.SD), .SDcols = draws], 2)
    ratio_se <- round(sex_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

    ## Apply predictions of the ratio
    both_sex <- merge(both_sex, sex_draws, by="id", allow.cartesian = T)
    # below is the algebra that makes the adjustments - see documentation
    both_sex[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (population_both/(population_male + (get(paste0("draw_", x)) * population_female))))]
    both_sex[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
    both_sex[, m_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
    both_sex[, f_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
    both_sex[, m_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
    both_sex[, f_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
    # get rid of draw columns
    both_sex[, paste0(draws) := NULL]
    m_draws <- names(both_sex)[grepl("male_", names(both_sex))]
    f_draws <- names(both_sex)[grepl("female_", names(both_sex))]
    both_sex[, paste0(m_draws[m_draws %in% names(both_sex)]) := NULL]
    for (f in f_draws[f_draws %in% names(both_sex)]) both_sex[, paste0(f) := NULL]

    ## Make male- and female-specific dts
    male_dt <- copy(both_sex)
    male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
    male_dt <- dplyr::select(male_dt, n)
    female_dt <- copy(both_sex)
    female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = "", lower = "",
                      cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                      note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                            ratio_se, ")"))]
    female_dt <- dplyr::select(female_dt, n) ## original names of data

    data <- rbind(sex_specific, female_dt)
    data <- rbind(data, male_dt)

    sex_specific_data <- copy(data)

    message("Done with sex-splitting.")
    
  }

  #### Running MR-BRT model on sex-specific data ###############################
  ##############################################################################

  if (!sex_split_only) {

    ## Pull out original data. Later we will apply the XW to this dataset.
    data[, age_mid := (age_start + age_end)/2]
    data[, year_mid := (year_start + year_end)/2]
    original_data_to_xw <- data[measure==xw_measure & get(reference_def) == 0,]
    original_data_no_xw <- data[measure==xw_measure & get(reference_def) == 1,]

    ## Find matches/create ratios. Function in "clean_mr_brt.R".
    message("Beginning crosswalks between alternate definition(s).")
    data <- match_mr_brt(
      data=data,
      reference_def=reference_def,
      alternate_defs=alternate_defs,
      age_overlap=age_overlap,
      year_overlap=year_overlap,
      age_range=age_range,
      year_range=year_range,
      subnat_to_nat = subnat_to_nat,
      fix_zeros=T,
      quiet=T
    )

    ## Pull out these objects to return later
    matches_count <- data$counts
    match_data_copy <- data

    # Pull out prevalence matches, log-transform / logit-transform. Prep the matches for MR-BRT.
    if(logit_transform) {
      matches <- logit_transform_mrbrt(data$data)
    } else {
      matches <- log_transform_mrbrt(data$data)
    }

    lapply(names(matches),check_missing, df=matches, warn=T)
    matches <- adj_mr_brt(matches) ## Scaling age and year variables. Making sex into a dummy variable. Function in 'clean_mr_brt.R'

    original_data_to_xw[, year_mid := (year_start + year_end)/2]
    ## Prep the original data for predicting. We need to scale the covariates, make SE, log-transform / logit-transform the SE.
    to_adjust <- adj_mr_brt(original_data_to_xw, nid=F, original_data = T, fix_zeros = F) ## Same scaling function. Not creating an ID variable as you would for prepping MR-BRT.
    to_adjust[, id := 1:nrow(to_adjust)]
    to_adjust[, se := standard_error]

    ## Take the original standard error into log- or logit-space using the delta method
    if(logit_transform) {

      to_adjust$logit_se <- sapply(1:nrow(to_adjust), function(i) {
        mean_i <-  to_adjust[i, mean]
        se_i <-  to_adjust[i, se]
        deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
      })

      matches[logit_diff_se == 0, logit_diff_se := .001] # check dataset

    } else {

      to_adjust$log_se <- sapply(1:nrow(to_adjust), function(i){
        mean_i <- to_adjust[i, "mean"]
        se_i <- to_adjust[i, "se"]
        deltamethod(~log(x1), mean_i, se_i^2)
      })

      matches[ratio_se_log == 0, ratio_se_log := .001]
    }

    matches[, age_scaled := (age_mid_ref - mean(age_mid_ref))/sd(age_mid_ref)]

    ## Figure out what covariates go into MR-BRT
    # drop the alt defs that didn't get matched
    alt.defs <- list()
    for (n in 1:length(alternate_defs)) {
      if (alternate_defs[n] %in% names(matches)) {
        temp <- alternate_defs[n]
        alt.defs <- append(alt.defs, temp)
      }
    }
    
    alt.defs <- as.character(alt.defs)
    
    # seems like some pairs of alt defs for occ_inj are perfectly colinear... need to get rid of them so mr-brt can run
    x_covs_prelim <- copy(alt.defs)
    
    if (length(alt.defs) == 8) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr1_4 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[4]))]); print(c("corr1_4", alt.defs[1], alt.defs[4], corr1_4))
      corr1_5 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[5]))]); print(c("corr1_5", alt.defs[1], alt.defs[5], corr1_5))
      corr1_6 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[6]))]); print(c("corr1_6", alt.defs[1], alt.defs[6], corr1_6))
      corr1_7 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[7]))]); print(c("corr1_7", alt.defs[1], alt.defs[7], corr1_7))
      corr1_8 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[8]))]); print(c("corr1_8", alt.defs[1], alt.defs[8], corr1_8))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      corr2_4 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[4]))]); print(c("corr2_4", alt.defs[2], alt.defs[4], corr2_4))
      corr2_5 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[5]))]); print(c("corr2_5", alt.defs[2], alt.defs[5], corr2_5))
      corr2_6 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[6]))]); print(c("corr2_6", alt.defs[2], alt.defs[6], corr2_6))
      corr2_7 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[7]))]); print(c("corr2_7", alt.defs[2], alt.defs[7], corr2_7))
      corr2_8 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[8]))]); print(c("corr2_8", alt.defs[2], alt.defs[8], corr2_8))
      corr3_4 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[4]))]); print(c("corr3_4", alt.defs[3], alt.defs[4], corr3_4))
      corr3_5 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[5]))]); print(c("corr3_5", alt.defs[3], alt.defs[5], corr3_5))
      corr3_6 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[6]))]); print(c("corr3_6", alt.defs[3], alt.defs[6], corr3_6))
      corr3_7 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[7]))]); print(c("corr3_7", alt.defs[3], alt.defs[7], corr3_7))
      corr3_8 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[8]))]); print(c("corr3_8", alt.defs[3], alt.defs[8], corr3_8))
      corr4_5 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[5]))]); print(c("corr4_5", alt.defs[4], alt.defs[5], corr4_5))
      corr4_6 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[6]))]); print(c("corr4_6", alt.defs[4], alt.defs[6], corr4_6))
      corr4_7 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[7]))]); print(c("corr4_7", alt.defs[4], alt.defs[7], corr4_7))
      corr4_8 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[8]))]); print(c("corr4_8", alt.defs[4], alt.defs[8], corr4_8))
      corr5_6 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[6]))]); print(c("corr5_6", alt.defs[5], alt.defs[6], corr5_6))
      corr5_7 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[7]))]); print(c("corr5_7", alt.defs[5], alt.defs[7], corr5_7))
      corr5_8 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[8]))]); print(c("corr5_8", alt.defs[5], alt.defs[8], corr5_8))
      corr6_7 <- corr(matches[, .(get(alt.defs[6]), get(alt.defs[7]))]); print(c("corr6_7", alt.defs[6], alt.defs[7], corr6_7))
      corr6_8 <- corr(matches[, .(get(alt.defs[6]), get(alt.defs[8]))]); print(c("corr6_8", alt.defs[6], alt.defs[8], corr6_8))
      corr7_8 <- corr(matches[, .(get(alt.defs[7]), get(alt.defs[8]))]); print(c("corr7_8", alt.defs[7], alt.defs[8], corr7_8))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3, "corr1_4" = corr1_4, "corr1_5" = corr1_5, "corr1_6" = corr1_6, "corr1_7" = corr1_7, "corr1_8" = corr1_8,
                              "corr2_3" = corr2_3, "corr2_4" = corr2_4, "corr2_5" = corr2_5, "corr2_6" = corr2_6, "corr2_7" = corr2_7, "corr2_8" = corr2_8,
                              "corr3_4" = corr3_4, "corr3_5" = corr3_5, "corr3_6" = corr3_6, "corr3_7" = corr3_7, "corr3_8" = corr3_8,
                              "corr4_5" = corr4_5, "corr4_6" = corr4_6, "corr4_7" = corr4_7, "corr4_8" = corr4_8,
                              "corr5_6" = corr5_6, "corr5_7" = corr5_7, "corr5_8" = corr5_8,
                              "corr6_7" = corr6_7, "corr6_8" = corr6_8,
                              "corr7_8" = corr7_8)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 7) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr1_4 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[4]))]); print(c("corr1_4", alt.defs[1], alt.defs[4], corr1_4))
      corr1_5 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[5]))]); print(c("corr1_5", alt.defs[1], alt.defs[5], corr1_5))
      corr1_6 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[6]))]); print(c("corr1_6", alt.defs[1], alt.defs[6], corr1_6))
      corr1_7 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[7]))]); print(c("corr1_7", alt.defs[1], alt.defs[7], corr1_7))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      corr2_4 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[4]))]); print(c("corr2_4", alt.defs[2], alt.defs[4], corr2_4))
      corr2_5 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[5]))]); print(c("corr2_5", alt.defs[2], alt.defs[5], corr2_5))
      corr2_6 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[6]))]); print(c("corr2_6", alt.defs[2], alt.defs[6], corr2_6))
      corr2_7 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[7]))]); print(c("corr2_7", alt.defs[2], alt.defs[7], corr2_7))
      corr3_4 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[4]))]); print(c("corr3_4", alt.defs[3], alt.defs[4], corr3_4))
      corr3_5 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[5]))]); print(c("corr3_5", alt.defs[3], alt.defs[5], corr3_5))
      corr3_6 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[6]))]); print(c("corr3_6", alt.defs[3], alt.defs[6], corr3_6))
      corr3_7 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[7]))]); print(c("corr3_7", alt.defs[3], alt.defs[7], corr3_7))
      corr4_5 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[5]))]); print(c("corr4_5", alt.defs[4], alt.defs[5], corr4_5))
      corr4_6 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[6]))]); print(c("corr4_6", alt.defs[4], alt.defs[6], corr4_6))
      corr4_7 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[7]))]); print(c("corr4_7", alt.defs[4], alt.defs[7], corr4_7))
      corr5_6 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[6]))]); print(c("corr5_6", alt.defs[5], alt.defs[6], corr5_6))
      corr5_7 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[7]))]); print(c("corr5_7", alt.defs[5], alt.defs[7], corr5_7))
      corr6_7 <- corr(matches[, .(get(alt.defs[6]), get(alt.defs[7]))]); print(c("corr6_7", alt.defs[6], alt.defs[7], corr6_7))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3, "corr1_4" = corr1_4, "corr1_5" = corr1_5, "corr1_6" = corr1_6, "corr1_7" = corr1_7,
                              "corr2_3" = corr2_3, "corr2_4" = corr2_4, "corr2_5" = corr2_5, "corr2_6" = corr2_6, "corr2_7" = corr2_7,
                              "corr3_4" = corr3_4, "corr3_5" = corr3_5, "corr3_6" = corr3_6, "corr3_7" = corr3_7,
                              "corr4_5" = corr4_5, "corr4_6" = corr4_6, "corr4_7" = corr4_7,
                              "corr5_6" = corr5_6, "corr5_7" = corr5_7,
                              "corr6_7" = corr6_7)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 6) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr1_4 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[4]))]); print(c("corr1_4", alt.defs[1], alt.defs[4], corr1_4))
      corr1_5 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[5]))]); print(c("corr1_5", alt.defs[1], alt.defs[5], corr1_5))
      corr1_6 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[6]))]); print(c("corr1_6", alt.defs[1], alt.defs[6], corr1_6))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      corr2_4 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[4]))]); print(c("corr2_4", alt.defs[2], alt.defs[4], corr2_4))
      corr2_5 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[5]))]); print(c("corr2_5", alt.defs[2], alt.defs[5], corr2_5))
      corr2_6 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[6]))]); print(c("corr2_6", alt.defs[2], alt.defs[6], corr2_6))
      corr3_4 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[4]))]); print(c("corr3_4", alt.defs[3], alt.defs[4], corr3_4))
      corr3_5 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[5]))]); print(c("corr3_5", alt.defs[3], alt.defs[5], corr3_5))
      corr3_6 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[6]))]); print(c("corr3_6", alt.defs[3], alt.defs[6], corr3_6))
      corr4_5 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[5]))]); print(c("corr4_5", alt.defs[4], alt.defs[5], corr4_5))
      corr4_6 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[6]))]); print(c("corr4_6", alt.defs[4], alt.defs[6], corr4_6))
      corr5_6 <- corr(matches[, .(get(alt.defs[5]), get(alt.defs[6]))]); print(c("corr5_6", alt.defs[5], alt.defs[6], corr5_6))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3, "corr1_4" = corr1_4, "corr1_5" = corr1_5, "corr1_6" = corr1_6,
                              "corr2_3" = corr2_3, "corr2_4" = corr2_4, "corr2_5" = corr2_5, "corr2_6" = corr2_6,
                              "corr3_4" = corr3_4, "corr3_5" = corr3_5, "corr3_6" = corr3_6,
                              "corr4_5" = corr4_5, "corr4_6" = corr4_6,
                              "corr5_6" = corr5_6)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 5) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr1_4 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[4]))]); print(c("corr1_4", alt.defs[1], alt.defs[4], corr1_4))
      corr1_5 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[5]))]); print(c("corr1_5", alt.defs[1], alt.defs[5], corr1_5))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      corr2_4 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[4]))]); print(c("corr2_4", alt.defs[2], alt.defs[4], corr2_4))
      corr2_5 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[5]))]); print(c("corr2_5", alt.defs[2], alt.defs[5], corr2_5))
      corr3_4 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[4]))]); print(c("corr3_4", alt.defs[3], alt.defs[4], corr3_4))
      corr3_5 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[5]))]); print(c("corr3_5", alt.defs[3], alt.defs[5], corr3_5))
      corr4_5 <- corr(matches[, .(get(alt.defs[4]), get(alt.defs[5]))]); print(c("corr4_5", alt.defs[4], alt.defs[5], corr4_5))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3, "corr1_4" = corr1_4, "corr1_5" = corr1_5,
                              "corr2_3" = corr2_3, "corr2_4" = corr2_4, "corr2_5" = corr2_5,
                              "corr3_4" = corr3_4, "corr3_5" = corr3_5,
                              "corr4_5" = corr4_5)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 4) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr1_4 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[4]))]); print(c("corr1_4", alt.defs[1], alt.defs[4], corr1_4))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      corr2_4 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[4]))]); print(c("corr2_4", alt.defs[2], alt.defs[4], corr2_4))
      corr3_4 <- corr(matches[, .(get(alt.defs[3]), get(alt.defs[4]))]); print(c("corr3_4", alt.defs[3], alt.defs[4], corr3_4))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3, "corr1_4" = corr1_4,
                              "corr2_3" = corr2_3, "corr2_4" = corr2_4,
                              "corr3_4" = corr3_4)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 3) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      corr1_3 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[3]))]); print(c("corr1_3", alt.defs[1], alt.defs[3], corr1_3))
      corr2_3 <- corr(matches[, .(get(alt.defs[2]), get(alt.defs[3]))]); print(c("corr2_3", alt.defs[2], alt.defs[3], corr2_3))
      
      all.corrs <- data.table("corr1_2" = corr1_2, "corr1_3" = corr1_3,
                              "corr2_3" = corr2_3)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    } else if (length(alt.defs) == 2) {
      corr1_2 <- corr(matches[, .(get(alt.defs[1]), get(alt.defs[2]))]); print(c("corr1_2", alt.defs[1], alt.defs[2], corr1_2))
      
      all.corrs <- data.table("corr1_2" = corr1_2)
      
      if (any(all.corrs == 1 | all.corrs == -1) == FALSE) {
        message("no pairs of alt defs with corrs of 1 or -1")
      } else {
        for (n in 1:ncol(all.corrs)) {
          if (all.corrs[, ..n] == 1 | all.corrs[, ..n] == -1) {
            rm_def_1 <- alt.defs[as.numeric(substr(substr(names(all.corrs[, ..n]), 1, 5), 5, 5))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_1]
            rm_def_2 <- alt.defs[as.numeric(substr(names(all.corrs[, ..n]), 7, 7))]
            x_covs_prelim <- x_covs_prelim[x_covs_prelim != rm_def_2]
          }
        }
      }
    }

    # add any additional x covs
    if (!(is.null(addl_x_covs) | missing(addl_x_covs))) {
      x_covs <- c(addl_x_covs, x_covs_prelim)
    } else {
      x_covs <- as.character(x_covs_prelim)
    }

    ##### Launch the MR-BRT model
    message("Launching MR-BRT...")
    results<-launch_mr_brt(data=matches, response=response, data_se=data_se, id_vars=id_vars,
                           x_covs=x_covs,include_x_intercept=include_x_intercept,
                           use_lasso=use_lasso, spline_covs=spline_covs,
                           opt_method=opt_method, trim_pct=trim_pct,
                           temp_folder=temp_folder, delete_model = F, subfolder_name = "main_xw")
    
    save(results, file = "FILEPATH")

    ## Pull out this object to return later
    covs <- results$model_coefs

    message("Predicting onto non-reference data points.")
    ## Predict on our prepped original data. Take the log-adjusted / logit-adjusted means and SEs.
    new_preds <- predict_mr_brt(results, to_adjust, temp_folder = temp_folder, subfolder_name = "xw_preds")
    save(new_preds, file = "FILEPATH")
    new_predictions <- new_preds$model_summaries

    ## Figure out the standard error of the predictions
    if(logit_transform) {

      new_predictions$prediction_se_logit <- sapply(1:nrow(new_predictions), function(i) {
        mean_i <- new_predictions[i, get("Y_mean")]
        se_i <- new_predictions[i, (Y_mean_hi - Y_mean_lo)/3.92]
        deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
      })

    } else {

      new_predictions[, prediction_se_log := (Y_mean_hi - Y_mean_lo)/3.92]
    }

    new_predictions[, id := 1:nrow(new_predictions)]

    ## Use the predictions to adjust the original data.
    ## If reference def, do nothing (should be no reference def in this dataset but..). If non-reference, adjust both mean & SE.
    adjusted_data <- merge(new_predictions, to_adjust, by="id")

    ## The below comes from the math (see documentation)
    if(logit_transform) {

      adjusted_data[, mean_logit_adjusted := ifelse(get(reference_def) == 1, mean_logit, mean_logit - Y_mean)]
      adjusted_data[, se_logit_adjusted := ifelse(get(reference_def) == 1, logit_se, sqrt(prediction_se_logit^2 + logit_se^2))]
      adjusted_data[, high_logit_adjusted := ifelse(get(reference_def) == 1, logit(upper), mean_logit_adjusted + 1.96*se_logit_adjusted)]
      adjusted_data[, low_logit_adjusted := ifelse(get(reference_def) == 1, logit(lower), mean_logit_adjusted - 1.96*se_logit_adjusted)]


      # Logit back to normal space
      adjusted_data$se_adjusted <- sapply(1:nrow(adjusted_data), function(i) {
          mean_i <- adjusted_data[i, mean_logit_adjusted]
          se_i <- adjusted_data[i, se_logit_adjusted]
          deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
      })
      adjusted_data[, mean_adj := inv.logit(mean_logit_adjusted)]

      ## adjusted se of mean = 0 in normal space, see comment in log-ratio below
      adjusted_data$pred_se_normal <- sapply(1:nrow(adjusted_data), function(i) {
        mean_i <- adjusted_data[i, Y_mean]
        se_i <- adjusted_data[i,prediction_se_logit]
        deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
      })

      adjusted_data[mean==0, se_adjusted := ifelse(get(reference_def) == 1, se_adjusted, sqrt(se^2 + pred_se_normal^2))]
      adjusted_data[mean==0, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]

    } else {

      ## In log-space, the uncertainty is the sum of prediction SE and original SE
      adjusted_data[, mean_log_adjusted := ifelse(get(reference_def) == 1, mean_log, mean_log - Y_mean)]
      adjusted_data[mean != 0, se_log_adjusted := ifelse(get(reference_def) == 1, log_se, sqrt(prediction_se_log^2 + log_se^2))]
      adjusted_data[mean != 0, high_log_adjusted := ifelse(get(reference_def) == 1, log(upper), mean_log_adjusted + 1.96*se_log_adjusted)]
      adjusted_data[mean != 0, low_log_adjusted := ifelse(get(reference_def) == 1, log(lower), mean_log_adjusted - 1.96*se_log_adjusted)]
      adjusted_data[mean != 0, `:=` (upper=exp(high_log_adjusted), lower=exp(low_log_adjusted))]

      adjusted_data[, mean_adj := exp(mean_log_adjusted)]

      ## Take out of log space
      adjusted_data$se_adjusted <- sapply(1:nrow(adjusted_data), function(i) {
        ratio_i <- adjusted_data[i, "mean_log_adjusted"]
        ratio_se_i <- adjusted_data[i, "se_log_adjusted"]
        deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
      })

      ## However, if mean is 0, we need to adjust the uncertainty in normal space, not log-space

      adjusted_data$pred_se_normal <- sapply(1:nrow(adjusted_data), function(i) {
        ratio_i <- adjusted_data[i, "Y_mean"]
        ratio_se_i <- adjusted_data[i,"prediction_se_log"]
        deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
      })

      adjusted_data[mean==0, se_adjusted := ifelse(get(reference_def) == 1, se_adjusted, sqrt(se^2 + pred_se_normal^2))]
      adjusted_data[mean==0, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]

    }


    data_for_comparison <- copy(adjusted_data)
    data_for_comparison <- rbind(data_for_comparison, original_data_no_xw, fill=T)

    ## Collapse back to our preferred data structure
    full_dt <- copy(adjusted_data)

    if(logit_transform) {

      full_dt[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = inv.logit(high_logit_adjusted), lower = inv.logit(low_logit_adjusted),
                      cases = "", sample_size = "", uncertainty_type_value = "",
                      note_modeler = paste0(note_modeler, " | crosswalked with logit difference: ", round(Y_mean, 2), " (",
                                            round(prediction_se_logit), ")"))]

    } else {

      full_dt[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = upper, lower = lower,
                      cases = "", sample_size = "", uncertainty_type_value = "",
                      note_modeler = paste0(note_modeler, " | crosswalked with log(ratio): ", round(Y_mean, 2), " (",
                                            round(prediction_se_log), ")"))]

    }

    extra_cols <- setdiff(names(full_dt), names(original_data_no_xw))
    full_dt[, c(extra_cols) := NULL]

    final_dt <- rbind(original_data_no_xw, full_dt, fill=T)

    message("Done crosswalking.")

  }

  if (sex_split_only) final_dt <- copy(sex_specific_data)

  ## save crosswalk results
  # adjust data to pass epiDataUploader scrutiny
  final_dt_upload <- copy(final_dt)

  final_dt_upload$underlying_nid <- NA
  final_dt_upload$crosswalk_parent_seq <- final_dt_upload$seq
  final_dt_upload$seq <- NA

  final_dt_upload$group_review <- NA
  final_dt_upload$specificity <- NA
  final_dt_upload$group <- NA

  final_dt_upload$upper <- NA
  final_dt_upload$lower <- NA

  final_dt_upload$sampling_type <- NA

  final_dt_upload[, grep("cv_*", names(final_dt_upload)) := NULL]

  # clean up for crosswalk version upload
  final_dt_upload[, unit_value_as_published := 1]
  setnames(final_dt_upload, "mean", "val")
  
  write.xlsx(final_dt_upload, save_crosswalk_result_file, sheetName="extraction")

  message(paste("Crosswalk results saved to", save_crosswalk_result_file))

  if (upload_crosswalk_version) {

    if (is.null(bundle_version_id) | missing(bundle_version_id)) {

      stop("You need to set bundle_version_id to upload data.")

    } else {

      description <- "MR-BRT network meta-regression"
      save_crosswalk_version_result <- save_crosswalk_version(
        bundle_version_id=bundle_version_id,
        data_filepath=save_crosswalk_result_file,
        description=description)

      print(sprintf('Request status: %s', save_crosswalk_version_result$request_status))
      print(sprintf('Request ID: %s', save_crosswalk_version_result$request_id))
      print(sprintf('Crosswalk version ID: %s', save_crosswalk_version_result$crosswalk_version_id))

      message("crosswalk version uploaded.")
    }

  }



  if (num_both_sex > 0) {
    if (sex_split_only) return(list(sex_results=sex_results,
                                    sex_counts=matches_result,
                                    sex_covs=sex_covariates,
                                    sex_ratios=sex_matches,
                                    sex_specific_data=sex_specific_data,
                                    ratio_mean = ratio_mean,
                                    ratio_se = ratio_se))
    else return (list(data=final_dt,
                      xw_results=results,
                      xw_counts=matches_count,
                      xw_covs=covs,
                      xw_ratios=matches,
                      data_for_comp=data_for_comparison,
                      sex_results=sex_results,
                      sex_counts=matches_result,
                      sex_covs=sex_covariates,
                      sex_ratios=sex_matches,
                      sex_specific_data=sex_specific_data,
                      ratio_mean = ratio_mean,
                      ratio_se = ratio_se))
  } else {
    return(list(data=final_dt, xw_results=results, xw_counts=matches_count, xw_covs=covs, xw_ratios=matches, data_for_comp=data_for_comparison))
  }

}