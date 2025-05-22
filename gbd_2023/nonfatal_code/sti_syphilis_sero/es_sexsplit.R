#################################################################################################
#' Name: SEX-SPLIT EARLY SYPHILIS
#' 5 crosswalks total (sex, diagnostics (2), populations(2))
################################################################################################


#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())

#LOAD PACKAGES
pacman::p_load(data.table, openxlsx, ggplot2, dplyr)
library(crosswalk)
library(mortdb)
library(Hmisc) 
library(msm)
library(stringr)
library(magrittr)
library(assertthat)
library(metafor)
library(boot)

#SOURCE FUNCTIONS
source_shared_functions(functions = c("get_bundle_version", "get_population", "get_location_metadata",
                                      "get_age_metadata", "get_ids"))

#ARGS & DIRS
cause <- "early_syphilis"
xwalk_dir <- paste0("FILEPATH")
sex_dir <- paste0(xwalk_dir, "sex_split/")
draws <- paste0("draw_", 0:999)
es_bvid <- OBJECT


#CODE -------------------------------------------------------------------------------------------------------------------

#GET BUNDLE VERSION
full_bv <- get_bundle_version(bundle_version_id = es_bvid, fetch = "all", export = FALSE)
dim(full_bv)

## FORMAT BUNDLE VERSION
format_data <- function(raw_dt){

  #get cases/sample size
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]

  #calculate std error based on uploader formulas
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]

  #calculate cases from se
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]

  #finato
  return(dt)
}

formatted_bv <- format_data(raw_dt = full_bv)

#GET SEX MATCHES-------------------------------------------------------
#this will give you matched male and female pairs from the same study.
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end")
  sex_dt[, match_n := .N, by = match_vars]

  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean","standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean, na.rm = TRUE)
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}

matched_dt <- find_sex_match(dt = formatted_bv)
names(matched_dt)

head(matched_dt)
write.csv(x = matched_dt, file = paste0("FILEPATH"), row.names = FALSE)


##LOG RATIO PREPARE--------------------------------------------------------------------------------------------------------
log_preparation <- function(dt){
  demo_cols <- dt[ ,c("nid","age_start", "age_end", "location_id", "measure", "year_start", "year_end", "id")]

  #create ratio & se
  dt[ ,ratio := mean_Female/mean_Male]
  dt[ ,ratio_se := sqrt(standard_error_Female^2 + standard_error_Male^2)]

  #calculate log ratio & se
  log_dt <- data.table(delta_transform(mean = dt$ratio, sd = dt$ratio_se, transformation = "linear_to_log"))
  setnames(log_dt, c("mean_log", "sd_log"), c("log_ratio", "log_ratio_se"))

  #combine the dataset
  full_log_dt <- cbind(demo_cols, log_dt)

  #add in appropriate columns
  full_log_dt[ ,`:=` (dorm_alt = "sex_female", dorm_ref = "sex_male")]
  full_log_dt[ ,mid_age := (age_start + age_end)/2]
  full_log_dt[ ,id2 := as.integer(as.factor(id))]
  full_log_dt[ ,transformation := "log_ratio"]

  return(full_log_dt)
}

log_trans_dt <- data.table(log_preparation(dt = matched_dt))
names(log_trans_dt)
head(log_trans_dt)

#PLOT INPUT DATA------------------------------------------------------------------------------------------------------------
plot_sex_by_age <- function(dt) {
  trans_type <- unique(dt$transformation)
  print(trans_type)

  pdf(paste0(xwalk_dir, "sex_split/", trans_type, "_sex_by_age.pdf"), width = 15)

  age_start <- ggplot(dt, aes(x=age_start, y=log_ratio)) +geom_point() + labs(title = "Female/Male Log Ratio by Age-Start")
  age_start

  mid_age <- ggplot(dt, aes(x=mid_age, y=log_ratio)) +geom_point() + labs(title = "Female/Male Log Ratio by Mid-Age")
  mid_age
  dev.off()

  return(list(by_start = age_start, by_mid = mid_age))
}
plots <- plot_sex_by_age(dt = log_trans_dt)
plots$by_start
plots$by_mid

#FORMAT DATA FOR THE MODEL
sex_dat1 <- CWData(
  df = log_trans_dt,
  obs = "log_ratio",       # matched ratios in log space
  obs_se = "log_ratio_se", # SE of F/M ratio in log space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",   # var for the reference def/method
  covs = list("mid_age"),       # list of (potential) covariate columns
  study_id = "id2"          # var for random intercepts; i.e. (1|study_id)
)
sex_dat1

sex_fit1_pct <- 0.8
sex_fit1 <- CWModel(
  cwdata = sex_dat1,           # result of CWData() function call
  obs_type = "diff_log", # must be "diff_logit" or "diff_log"
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept"),
    CovModel(cov_name = "mid_age", spline = XSpline(knots = c(12,30,60,80), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "decreasing")),
  gold_dorm = "sex_male", # level of 'ref_dorms' that's the gold standard
  inlier_pct = sex_fit1_pct #1-pct(outlier)
)
sex_fit1

print(data.frame(
  beta_true = unique(log_trans_dt$dorm_alt),
  beta_mean = sex_fit1$beta,
  beta_se = sex_fit1$beta_sd,
  gamma = sex_fit1$gamma
))

#PLOT
repl_python()
plots <- import("crosswalk.plots")

plots$dose_response_curve(
  dose_variable = "mid_age",
  obs_method = 'sex_female',
  continuous_variables = list(),
  cwdata = sex_dat1,
  cwmodel = sex_fit1,
  plot_note = paste0("Female/Male Log Ratio by Age Midpoint, ", sex_fit1_pct*100, "%"),
  plots_dir = paste0(sex_dir, "plots/"),
  file_name = paste0("spline_4pts_pct", sex_fit1_pct),
  write_file = TRUE
)


#FORMAT BOTH-SEX DT-------------------------------------------------------------------------------------------------------------------------------------
dim(full_bv)
prep_for_sexsplit <- function(dt){

  leave <- dt[measure != "prevalence" | (measure == "prevalence" & sex %in% c("Male", "Female"))]

  #get the both sex dt
  both_all <- dt[measure == "prevalence" & sex == "Both"]
  both_all[ ,mid_age := (age_start +age_end)/2]

  #inflate se of this data
  inflate <- both_all[mean == 0]
  #adjust mean and se of this data
  adjust <- both_all[mean != 0]

  if (nrow(leave) + nrow(inflate) + nrow(adjust) == nrow(dt)){
    "returning 3 data tables."
    return(list(leave_dt = leave, inflate_dt = inflate, adjust_dt = adjust))
  }else{
    print("Error: leave, inflate, and adjust are not mutually exclusive.")
  }
}

formatted_dts <- prep_for_sexsplit(dt = copy(full_bv))
leave_dt <- formatted_dts$leave_dt
inflate_dt <- formatted_dts$inflate_dt
adjust_dt <- formatted_dts$adjust_dt

#prep adjust for predictions
ssplit_dt <- copy(adjust_dt)
ssplit_dt[ ,sex_dummy := "sex_female"]
ssplit_dt$row_id <- paste0("row", 1:nrow(ssplit_dt))
nrow(ssplit_dt)

#GET PREDICTIONS
sex_preds <- adjust_orig_vals(
  fit_object = sex_fit1, # object returned by `CWModel()`
  df = ssplit_dt,
  orig_dorms = "sex_dummy",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error",
  data_id = "row_id"   # optional argument to add a user-defined ID to the predictions;
)

head(sex_preds)

#merge on the age value
mid_age_row <- ssplit_dt[row_id %in% sex_preds$data_id, c("mid_age", "row_id")]
setnames(mid_age_row, "row_id", "data_id")
head(mid_age_row)

sex_preds_full <- merge(sex_preds, mid_age_row, by = "data_id", all.x = TRUE)
head(sex_preds_full)
setorder(sex_preds_full, ... = mid_age)
write.csv(x = sex_preds_full, file = paste0(sex_dir, "bv", es_bvid, "_sexpreds_", name_of_model, ".csv"), row.names = FALSE)

#SEX SPLIT -------------------------------------------------------------------------------------
split_both_sex <- function(both_sex, both_zero_dt) {

  #message
  print(paste0("There are ", nrow(both_sex), " both-sex, non-zero rows to be split."))

  #add vars
  both_sex[, year_mid := (year_start + year_end)/2]
  both_sex[, year_floor := floor(year_mid)]

  both_sex[ ,mid_age := (age_start+age_end)/2]
  print(sort(unique(both_sex$mid_age)))
  both_sex[, age_floor:= floor(mid_age)]

  ## Pull out population data. We need age- sex- location-specific population.
  message("Getting population")
  pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = "step3", year_id = unique(floor(both_sex$year_mid)),
                        location_id=unique(both_sex$location_id), single_year_age = T, gbd_round_id = gbd_round)
  ids <- get_ids("age_group") ## age group IDs
  pop_all <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)

  pop_all$age_group_name <- as.numeric(pop_all$age_group_name)
  unique(pop_all$age_group_name)
  pop_all <- pop_all[!(is.na(age_group_name))]
  pop_all$age_group_id <- NULL

  ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
  both_sex <- merge(both_sex, pop_all[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(both_sex, "population", "population_both")
  both_sex <- merge(both_sex, pop_all[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(both_sex, "population", "population_male")
  both_sex <- merge(both_sex, pop_all[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(both_sex, "population", "population_female")

  sex_preds_dt <- data.table(sex_preds_full)
  sex_preds_dt[, id := 1:nrow(sex_preds_dt)]
  both_sex[, id := 1:nrow(both_sex)]
  if (nrow(both_sex[is.na(population_both)]) > 0) dropped_nids <- unique(both_sex[is.na(population_both), nid])
  both_sex <- both_sex[!is.na(population_both)]
  ids <- unique(both_sex$id)

  sex_preds_dt <- sex_preds_dt[id %in% ids,]
  setnames(sex_preds_dt, "data_id", "row_id")

  ## Merge adjustments with unadjusted data
  orig_merged <- merge(both_sex, sex_preds_dt, by="row_id", allow.cartesian = T)

  ## Take mean, SEs into real-space so we can combine and make adjustments
  orig_merged[, c("normal_ratio", "normal_ratio_se") := data.table(delta_transform(mean = pred_diff_mean, sd = pred_diff_sd, transform = "log_to_linear"))]

  ## Make adjustments. See documentation for rationale behind algebra.
  orig_merged[, m_mean := mean * (population_both/(population_male + normal_ratio * population_female))]
  orig_merged[, f_mean := normal_ratio * m_mean]

  ## Get combined standard errors
  orig_merged[, m_standard_error := sqrt(normal_ratio_se^2 + standard_error^2)]
  orig_merged[, f_standard_error := sqrt(normal_ratio_se^2 + standard_error^2)]

  ## Make male- and female-specific dts
  message("Getting male and female specific data tables")
  male_dt <- data.table(copy(orig_merged))
  male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", normal_ratio, " (",
                                        normal_ratio_se, ")"))]
  female_dt <- data.table(copy(orig_merged))
  female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", normal_ratio, " (",
                                          normal_ratio_se, ")"))]

  #drop unneeded columns
  names(male_dt)
  names(female_dt)

  drop_cols <- setdiff(names(male_dt), names(both_sex_dt))
  drop_cols

  male_dt[ ,c("year_floor", "age_floor", "year_mid","row_id","sex_dummy","mid_age.x", "population_both", "population_male", "population_female", "id.x", "ref_vals_mean",
              "ref_vals_sd", "pred_diff_mean", "pred_diff_sd", "mid_age.y", "id.y", "normal_ratio", "normal_ratio_se",
              "m_mean", "f_mean", "m_standard_error", "f_standard_error") := NULL]

  female_dt[ ,c("year_floor", "age_floor", "year_mid","row_id","sex_dummy","mid_age.x", "population_both", "population_male", "population_female", "id.x", "ref_vals_mean",
              "ref_vals_sd", "pred_diff_mean", "pred_diff_sd", "mid_age.y", "id.y", "normal_ratio", "normal_ratio_se",
              "m_mean", "f_mean", "m_standard_error", "f_standard_error") := NULL]

  #inflate SE of both_zero_dt
  message("inflating mean zero points")
  inflate_both_zero <- copy(both_zero_dt)
  age_inflate <- unique(inflate_both_zero$mid_age)

  sex_preds_inflate <- data.table(sex_preds_full[ ,c("pred_diff_sd", "mid_age")])
  sex_preds_inflate <- unique(sex_preds_inflate[mid_age %in% age_inflate])

  inflate_both_zero <- merge(inflate_both_zero, sex_preds_inflate, by = c("mid_age"), all.x = TRUE)
  inflate_both_zero[ ,standard_error := sqrt(standard_error^2 + pred_diff_sd^2)]
  inflate_both_zero[ ,c("mid_age", "pred_diff_sd") := NULL]

  male_zero <- copy(inflate_both_zero)
  male_zero[ ,sex := "Male"]
  female_zero <- copy(inflate_both_zero)
  female_zero[ ,sex := "Female"]

  both_sex_split <- rbind(male_dt, male_zero, female_dt, female_zero)
  both_sex_split$crosswalk_parent_seq <- both_sex_split$seq
  both_sex_split$seq <- NA
  message("Finished sex splitting.")

  return(list(final = both_sex_split, graph = orig_merged))
}
sex_split_dt <- split_both_sex(both_sex = ssplit_dt, both_zero_dt = inflate_dt)

sex_specific_dt <- sex_split_dt$final
graph_ssplit <- sex_split_dt$graph
full_ssplit_bv <- rbind(sex_specific_dt, leave_dt, fill = TRUE)

write.xlsx(x = sex_specific_dt)
write.xlsx(x = full_ssplit_bv)


graph_sex_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, m_mean, m_standard_error, f_mean, f_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_mean", "f_mean"))
  graph_dt_means[variable == "f_mean", variable := "Female"][variable == "m_mean", variable := "Male"]
  graph_dt_means[, id := .I]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_standard_error", "f_standard_error"))
  graph_dt_error[variable == "f_standard_error", variable := "Female"][variable == "m_standard_error", variable := "Male"]
  graph_dt_error[, id := .I]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable", "id"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]

  graph_dt[,mid_age := (age_start + age_end)/2]
  graph_dt[mid_age >=10 & mid_age <19.99, age_group := "10-19 years"]
  graph_dt[mid_age >=20 & mid_age <29.99, age_group := "20-29 years"]
  graph_dt[mid_age >=30 & mid_age <39.99, age_group := "30-39 years"]
  graph_dt[mid_age >=40 & mid_age <49.99, age_group := "40-49 years"]
  graph_dt[mid_age >=50 & mid_age <59.99, age_group := "50-59 years"]

  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    #geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "red")) +
    theme_classic() +
    facet_wrap(~age_group)
  gg_sex
  return(gg_sex)
}

sex_split_plot <- graph_sex_predictions(dt = graph_ssplit)
sex_split_plot

pdf(paste0("FILEPATH"), width = 15)
sex_split_plot
dev.off()


