################################################################################################################
################################################################################################################
## This script is used for data processing for the IBD models; this script is run separately for UC and CD
################################################################################################################
################################################################################################################

rm(list=ls())


## Source functions and packages
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(metafor, lib.loc=FILEPATH)
library(Hmisc, lib.loc = FILEPATH)
library(msm)
library(mortdb, lib = FILEPATH)
library(crosswalk, lib.loc = FILEPATH)

base_dir <- FILEPATH
temp_dir <- FILEPATH
functions <- c("get_bundle_data", "upload_bundle_data","get_bundle_version", "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
               "get_age_metadata", "save_bulk_outlier","get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_population", "get_ids")
lapply(paste0(base_dir, functions, ".R"), source)


mrbrt_helper_dir <- FILEPATH
mrbrt_dir <- FILEPATH
draws <- paste0("draw_", 0:999)
functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}


## Source custom functions to fill out mean/cases/sample size/standard error when missing
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

#########################################################################################
##STEP 1: Set objects 
#########################################################################################
date <- Sys.Date()
date <- gsub("-", "_", date)
bundle <- BUNDLE_ID
model_name <- "UC"
acause <- CAUSE_NAME
gbd_round_id <- GBD_ROUND
decomp_step <- 'iterative'
save_path <- FILEPATH
step2_bundle_version <- BUNDLE_VERSION_ID  

#For crosswalk
mrbrt_cv <- "intercept"
xwalk_type<-"logit"
xwalk_output <- FILEPATH
xwalk_pkl_output<- FILEPATH
output_filepath_agesplit <- FILEPATH


#########################################################################################
##STEP 2: Within-study age-sex split
#########################################################################################

#FIRST, WITHIN-STUDY AGE-SEX SPLIT
dt <- as.data.table(subset(df_all, cv_literature==1 | canada_microdata==1 )) #subset literature data only, excluding custom EMR and CSMR NIDs
dt$note_modeler <- ""

dt <- subset(dt, is_outlier==0)
dt <- subset(dt, crosswalk==0 | is.na(crosswalk))

age_sex_split <- function(dt){
  notsplit <- as.data.table(subset(dt, age_sex_split=="" ))
  dt_split <- as.data.table(subset(dt, age_sex_split!=""))
  dt_split[, note_modeler := as.character(note_modeler)]
  dt_split[, specificity := as.character(specificity)]
  
  #fill in cases and sample size for age-sex splitting
  dt_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
  dt_split[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  dt_split[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt_split[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  dt_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  dt_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  dt_split[is.na(cases), cases := sample_size * mean]
  dt_split <- dt_split[!is.na(cases),]
  
  
  #SEX SPLITTING --------------------------------------------------
  #subset to data needing age-sex splitting
  tosplit_sex <- dt_split[age_sex_split =="tosplit" & sex == "Both"] 
  
  #subset the rows with sex-specific data
  sex <- dt_split[age_sex_split=="sexvalue" & year_specific==""]
  sex_yr <- dt_split[age_sex_split=="sexvalue" & year_specific=="yes"]
  setnames(sex_yr, c("cases", "sample_size"),  c("cases1", "sample_size1"))
  sex_yr <- sex_yr[,  c("nid", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific", "year_start", "year_end")]
  
  #sum female and male-specific data points from the same study but across multiple years to calculate pooled sex ratio
  sex[, cases1:= sum(cases), by = list(nid, sex, measure, location_id)]
  sex[, sample_size1:= sum(sample_size), by = list(nid, sex, measure, location_id)]
  sex <- sex[,  c("nid", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific")]
  sex <- unique(sex)
  
  #calculate proportion of cases male and female
  sex <- as.data.table(sex)
  sex[, cases_total:= sum(cases1), by = list(nid,  measure)]
  sex[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  sex[, ss_total:= sum(sample_size1), by = list(nid, measure)]
  sex[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  sex[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  sex[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  sex[, ratio := round(prop_cases / prop_ss, digits = 3)]
  sex[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  #calculate proportion of cases male and female
  sex_yr <- as.data.table(sex_yr)
  sex_yr[, cases_total:= sum(cases1), by = list(nid,  measure, year_start, year_end)]
  sex_yr[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  sex_yr[, ss_total:= sum(sample_size1), by = list(nid, measure, year_start, year_end)]
  sex_yr[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  sex_yr[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  sex_yr[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  sex_yr[, ratio := round(prop_cases / prop_ss, digits = 3)]
  sex_yr[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  
  #Create age,sex observations 
  tosplit_sex[,specificity := paste(specificity, ",sex")]
  tosplit_sex[, seq := NA]
  male <- copy(tosplit_sex[, sex := "Male"]) #create a copy of data table with sex specified as male
  female <- copy(tosplit_sex[, sex := "Female"]) #create a copy of data table with sex specified as female
  tosplit_sex <- rbind(male, female)
  
  #Merge sex ratios to age,sex observations
  tosplit_sex1 <- merge(tosplit_sex, sex, by = c("nid", "sex", "measure", "location_id"))
  tosplit_sex2 <- merge(tosplit_sex, sex_yr, by = c("nid", "sex", "measure", "location_id", "year_start", "year_end"))
  tosplit_sex <- as.data.table(rbind.fill(tosplit_sex1,tosplit_sex2))
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  tosplit_sex[, standard_error1 := (sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2))]
  tosplit_sex[, mean1 := mean * ratio]
  tosplit_sex[, cases1 := round(cases * prop_cases, digits = 0)]
  tosplit_sex[, sample_size1 := round(sample_size * prop_ss, digits = 0)]
  tosplit_sex[, `:=` (upper = NA, lower = NA, uncertainty_type_value= NA)]
  
  tosplit_sex[,note_modeler := paste(note_modeler, "| sex split using sex ratio", round(ratio, digits = 2))]
  tosplit_sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "sample_size1", "cases_total") := NULL]
  
  
  #clean sex-specific data points that we want to append to final dataset
  sex_append <- dt_split[age_sex_split=="sexvalue"]
  sex_append <- subset(sex_append, group_review==1)
  
  final_sex <- rbind.fill(tosplit_sex, sex_append)
  
  #AGE SPLITTING --------------------------------------------------
  #subset to data needing age splitting
  tosplit_age <- dt_split[age_sex_split =="tosplit"] 
  
  #subset the rows with age-specific data
  age <- as.data.table(dt_split[(age_sex_split=="agevalue") & year_specific==""])
  
  #sum age-specific data points from the same study but across multiple years to calculate pooled sex ratio
  age[, cases1:= sum(cases), by = list(nid, age_start, age_end, sex, measure, location_id)]
  age[, sample_size1:= sum(sample_size), by = list(nid, age_start, age_end, sex, measure, location_id)]
  age <- age[,  c("nid", "age_start", "age_end", "sex", "measure", "age_sex_split", "location_id", "cases1", "sample_size1", "year_specific")]
  age <- unique(age)
  
  
  #calculate proportion of cases for each age group
  age <- as.data.table(age)
  age[, cases_total:= sum(cases1), by = list(nid,  measure, sex)]
  age[, prop_cases := round(cases1 / cases_total, digits = 3)]
  
  #calculate proportion of sample male and female
  age[, ss_total:= sum(sample_size1), by = list(nid, measure, sex)]
  age[, prop_ss := round(sample_size1 / ss_total, digits = 3)]
  
  #calculate standard error of % cases & sample_size M and F
  age[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  age[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  #estimate ratio & standard error of ratio % cases : % sample
  age[, ratio := round(prop_cases / prop_ss, digits = 3)]
  age[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  
  
  #Create age,sex observations 
  age_nid <- unique(age$nid)
  
  tosplit_age <- tosplit_age[nid %in% age_nid]
  tosplit_age[,specificity := paste(specificity, ",age")]
  tosplit_age[, seq := NA]
  tosplit_age[, c("age_start", "age_end")] <- NULL
  
  #Merge sex ratios to age,sex observations
  age_spec <- subset(age, nid==442296)
  age<- subset(age, nid!=442296)
  age$sex <- NULL
  split_age <- merge(tosplit_age, age, by = c("nid", "location_id", "measure"),  allow.cartesian = T)
  split_age_spec <- merge(tosplit_age, age_spec, by = c("nid", "location_id", "measure", "sex"),   allow.cartesian = T)
  
  split_age <- as.data.table(rbind.fill(split_age,split_age_spec))
  
  #calculate age-sex specific mean, standard_error, cases, sample_size
  split_age[, standard_error := (sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2))]
  split_age[, mean := mean * ratio]
  split_age[, cases := round(cases * prop_cases, digits = 0)]
  split_age[, sample_size := round(sample_size * prop_ss, digits = 0)]
  split_age[, `:=` (upper = NA, lower = NA)]
  
  split_age[,note_modeler := paste(note_modeler, "| age split using age ratio", round(ratio, digits = 2))]
  split_age[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "sample_size1", "cases_total", "year_specific.y","year_start.x", "year_start.y", "year_end.x", "year_end.y", "age_sex_split", "year_specific", "year_specific.x") := NULL]
  
  
  test <- split_age[, c("nid", "sex", "location_name", "age_start", "age_end", "year_start", "year_end", "mean", "standard_error", "cases", "sample_size")]
  
  #clean sex-specific data points that we want to append to final dataset
  age_append <- dt_split[age_sex_split=="agevalue" | age_sex_split == "agesexvalue"]
  age_append <- subset(age_append, group_review==1)
  
  within_split <- as.data.table(rbind.fill(final_sex, split_age, age_append))
  
  within_split[,c("ratio", "se_ratio", "prop_cases", "prop_ss","age_sex_split.x", "age_sex_split.y", "ss_total", "se_cases", "se_ss", "cases1", "sample_size1", "cases_total", "year_specific.y","year_start.x", "year_start.y", "year_end.x", "year_end.y", "age_sex_split", "year_specific", "year_specific.x") := NULL]
  
  within_split  <- subset(within_split, standard_error != "NaN")
  notsplit  <- subset(notsplit, standard_error != "NaN")
  
  
  
  ##return the prepped data
  return(list(within_split = within_split, notsplit_original_data = notsplit))
  
}

split <- age_sex_split(dt)
within_split <- split$within_split
notsplit_original_data <- split$notsplit_original_data

within_split <- subset(within_split, is.na(group_review) | group_review==1)
notsplit_original_data<- as.data.table(subset(notsplit_original_data, is.na(group_review) | group_review==1))


########################################################################################################     
#Step 3: Sex-split both sex data points using the pooled ratio from sex-specific literature data points
########################################################################################################     
cv_drop <- c("cv_self_report")

dt_sex <- get_cases_sample_size(dt)
dt_sex <- calculate_cases_fromse(dt_sex)

sex_prev <-  copy(dt_sex[measure=="prevalence"])
sex_inc <-  copy(dt_sex[measure=="incidence"])
sex_specific <- copy(notsplit_original_data[sex != "Both" ])

## Prevalence first -------------------------------------------------------------------------------------
#subset to both sex
both_sex <- copy(notsplit_original_data[sex == "Both" & measure=="prevalence"])
both_sex[, id := 1:nrow(both_sex)]
both_sex[, sex_dummy := "Female"]

both_zero <- copy(both_sex)
both_zero <- both_zero[mean == 0, ]
nrow(both_zero)

both_sex <- both_sex[mean != 0]

#find sex matches--------------------------------------------------------------------------------------
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c( "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                   names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars] #finding female-male pairs within each study, matched on measure, year, age, location_name
  sex_dt <- sex_dt[match_n >1] 
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt <- dplyr::select(sex_dt, keep_vars) #keep only the variables in "keep_var" vector
  sex_dt<-data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean) #reshape long to wide, to match male to female, rename mean and se with "x_Female" and "x_<ale"
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0] #drop where sex-specific mean is zero -> not informative 
  sex_dt[, id := .GRP, by = c("nid", "location_id")] #re-number based on nid and location id
  sex_dt$dorm_alt <- "Female"
  sex_dt$dorm_ref <- "Male"
  return(sex_dt)
}

sex_matches <- find_sex_match(sex_prev)
sex_matches

#calculate logit difference between sex-specific mean estimates: female alternative; male reference-----
#1. logit transform mean
model <- paste0("sex_split_prevalence_", model_name, "_", date)

sex_diff <- as.data.frame(cbind(
                                delta_transform(
                                  mean = sex_matches$mean_Female, 
                                  sd = sex_matches$standard_error_Female,
                                  transformation = "linear_to_log" ),
                                delta_transform(
                                  mean = sex_matches$mean_Male, 
                                  sd = sex_matches$standard_error_Male,
                                  transformation = "linear_to_log")
                              ))
names(sex_diff) <- c("mean_alt_Female", "mean_se_alt_Female", "mean_ref_Male", "mean_se_ref_Male")


#2. calculate logit(prev_alt) - logit(prev_ref)
sex_matches[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = sex_diff, 
  alt_mean = "mean_alt_Female", alt_sd = "mean_se_alt_Female",
  ref_mean = "mean_ref_Male", ref_sd = "mean_se_ref_Male" )

#3. run MRBRT
sex_model <- run_mr_brt(
  output_dir = FILEPATH,
  model_label = paste0(model_name, "_sex_split_prevalence_", date),
  data = sex_matches,
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  mean_var = "log_diff",
  se_var = "log_diff_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10)

#4. predict out
sex_model$model_coefs

#5. set up data for sex-splitting
sex_plit_data <- both_sex
sex_plit_data$midyear <- (sex_plit_data$year_end + sex_plit_data$year_start)/2

#6. call sex-split functions
get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}
split_data <- function(dt, model){
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  location_ids <- tosplit_dt[, unique(location_id)]
  location_ids<-location_ids[!location_ids %in% NA]
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids =location_ids)
  
  
  pops[age_group_years_end == 125, age_group_years_end := 99]
  
  
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
  
  
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA, 
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(sex_plit_data))
  female_dt <- copy(split_dt)
  
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(sex_plit_data))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt$N[graph_dt$N=="NaN"] <-NA
  graph_dt <-subset(graph_dt, N!="")
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    #  facet_wrap(~age_group) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

#7. run sex-split functions
predict_sex <- split_data(sex_plit_data, sex_model)
final_dt_prev <- copy(predict_sex$final)
final_dt_prev <- rbind.fill(final_dt_prev, within_split,sex_specific )
graph_dt_prev <- copy(predict_sex$graph)

#8. create visualization 
sex_graph <- graph_predictions(graph_dt_prev)
sex_graph
ggsave(filename = paste0(save_path,  "sex_graph_prevalence_", date,".pdf"), plot = sex_graph, width = 6, height = 6)


## Incidence next ---------------------------------------------------------------------------------------
#subset to both sex
both_sex <- copy(notsplit_original_data[sex == "Both" & measure=="incidence"])
both_sex[, id := 1:nrow(both_sex)]
both_sex[, sex_dummy := "Female"]

both_zero <- copy(both_sex)
both_zero <- both_zero[mean == 0, ]
nrow(both_zero)

both_sex <- both_sex[mean != 0]


#find sex matches--------------------------------------------------------------------------------------
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c( "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                   names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars] #finding female-male pairs within each study, matched on measure, year, age, location_name
  sex_dt <- sex_dt[match_n >1] 
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt <- dplyr::select(sex_dt, keep_vars) #keep only the variables in "keep_var" vector
  sex_dt<-data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean) #reshape long to wide, to match male to female, rename mean and se with "x_Female" and "x_<ale"
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0] #drop where sex-specific mean is zero -> not informative 
  sex_dt[, id := .GRP, by = c("nid", "location_id")] #re-number based on nid and location id
  sex_dt$dorm_alt <- "Female"
  sex_dt$dorm_ref <- "Male"
  return(sex_dt)
}

sex_matches <- find_sex_match(sex_inc)
sex_matches

#calculate logit difference between sex-specific mean estimates: female alternative; male refernce-----
#1. logit transform mean
model <- paste0("sex_split_incidence_", model_name, "_", date)

sex_diff <- as.data.frame(cbind(
  delta_transform(
    mean = sex_matches$mean_Female, 
    sd = sex_matches$standard_error_Female,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = sex_matches$mean_Male, 
    sd = sex_matches$standard_error_Male,
    transformation = "linear_to_log")
))
names(sex_diff) <- c("mean_alt_Female", "mean_se_alt_Female", "mean_ref_Male", "mean_se_ref_Male")


#2. calculate logit(prev_alt) - logit(prev_ref)
sex_matches[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = sex_diff, 
  alt_mean = "mean_alt_Female", alt_sd = "mean_se_alt_Female",
  ref_mean = "mean_ref_Male", ref_sd = "mean_se_ref_Male" )


sex_model <- run_mr_brt(
  output_dir = FILEPATH,
  model_label = paste0(model_name, "_sex_split_incidence_", date),
  data = sex_matches,
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  mean_var = "log_diff",
  se_var = "log_diff_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10
)

sex_model$model_coefs
sex_plit_data <- both_sex
sex_plit_data$midyear <- (sex_plit_data$year_end + sex_plit_data$year_start)/2
split_data <- function(dt, model){
  tosplit_dt <- copy(sex_plit_data)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  location_ids <- tosplit_dt[, unique(location_id)]
  location_ids<-location_ids[!location_ids %in% NA]
  preds <- predict_mr_brt(sex_model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids =location_ids)
  
  
  pops[age_group_years_end == 125, age_group_years_end := 99]
  
  
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
  
  
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA, 
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(sex_plit_data))
  female_dt <- copy(split_dt)
  
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(sex_plit_data))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "incidence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt$N[graph_dt$N=="NaN"] <-NA
  graph_dt <-subset(graph_dt, N!="")
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    #  facet_wrap(~age_group) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

predict_sex <- split_data(sex_plit_data, sex_model)
final_dt_inc <- copy(predict_sex$final)
graph_dt_inc <- copy(predict_sex$graph)


sex_graph <- graph_predictions(graph_dt_inc)
sex_graph
ggsave(filename = paste0(save_path,  "sex_graph_incidence_", date,".pdf"), plot = sex_graph, width = 6, height = 6)

#combine prev and inc sex-split data
final_dt <- rbind.fill(final_dt_prev, final_dt_inc )
write.csv(final_dt, paste0(save_path, "sex_split_final_",date,".csv"), row.names = FALSE)


#########################################################################################
##STEP 4: GBD2020 CROSSWALK WITH UPDATED MRBRT FUNCTIONS
##########################################################################################
#calculate xwalk coefficient from IBD/UC/CD combined for within study
xwalk <-as.data.table(read_excel(FILEPATH))
xwalk$mean <- as.numeric(xwalk$mean)
xwalk$standard_error <- as.numeric(xwalk$standard_error)
xwalk$cases <- as.numeric(xwalk$cases)
xwalk$sample_size <- as.numeric(xwalk$sample_size)

xwalk <- get_cases_sample_size(xwalk)
xwalk <-calculate_cases_fromse(xwalk)
xwalk <-get_se(xwalk)


#Subset crosswalk datasets by measure; need to calculate xwalk coefficient separately by measure
xwalk_prev <- subset(xwalk, measure=="prevalence" )

#PREVALENCE FIRST------------------------------------------------------------
ref <- subset(xwalk_prev, cv_chart_review==1)
ref$cv_diagn_admin_data <- NULL
alt <- subset(xwalk_prev, cv_diagn_admin_data==1)
alt <- alt[, c("nid", "location_id", "sex", "age_start", "age_end", "measure", "mean", "standard_error", "cv_diagn_admin_data", "case_name")]
setnames(ref, c("mean", "standard_error"), c("prev_ref", "prev_se_ref"))
setnames(alt, c("mean", "standard_error"), c("prev_alt", "prev_se_alt"))

#Create a variable identifying different case definitions
alt$dorm_alt<- "ICD_based"
ref$dorm_ref<- "Stringent"

#match based on NID, location, sex, age, and measure
df_matched <- merge(ref, alt, by = c("nid", "location_id", "sex", "age_start", "age_end", "measure", "case_name"))
df_matched$obs_method <- "ICD_based"

#logit transform mean
library(crosswalk, lib.loc = FILEPATH)
dat_diff <- as.data.frame(cbind(
                                delta_transform(
                                  mean = df_matched$prev_alt, 
                                  sd = df_matched$prev_se_alt,
                                  transformation = "linear_to_logit" ),
                                delta_transform(
                                  mean = df_matched$prev_ref, 
                                  sd = df_matched$prev_se_ref,
                                  transformation = "linear_to_logit")
                              ))
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")


#calculate logit(prev_alt) - logit(prev_ref)
df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
                                                                  df = dat_diff, 
                                                                  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
                                                                  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
df_matched[, id := .GRP, by = c("nid")]

#CWData() formats meta-regression data
dat_prev <- CWData(df = df_matched, 
                   obs = "logit_diff",                         #matched differences in logit space
                   obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                   alt_dorms = "dorm_alt",                     #var for the alternative def/method
                   ref_dorms = "dorm_ref",                     #var for the reference def/method
                   study_id = "nid" )                         #var for random intercepts; i.e. (study_id)

#CWModel() runs mrbrt model
results_pre <- CWModel(
  cwdata = dat_prev,                              #result of CWData() function  call
  obs_type = "diff_logit",                        #must be "diff_logit" or "diff_log"
  cov_models = list(CovModel("intercept")),       #specify covariate details
  inlier_pct = 0.9,
  gold_dorm = "Stringent")                        #level of "dorm_ref" that is the gold standard


results_pre$fixed_vars

#save model output
df_result<- results_pre$create_result_df()
write.xlsx(df_result, xwalk_output) 
py_save_object(object = results_pre, filename = xwalk_pkl_output, pickle = "dill")



#forest plots
options(scipen=999)
preds <- results_pre$fixed_vars
preds <- as.data.frame(preds)
preds$Stringent <- NULL
preds$dorm_alt <- "ICD_based"

beta_sd <- results_pre$beta_sd
beta_sd <- as.data.frame(beta_sd) 
beta_sd <- subset(beta_sd, beta_sd!=0)
preds <- merge(preds, beta_sd)
setnames(preds, "ICD_based", "beta")
df_plot <- df_matched[, c("nid", "location_id", "location_name", "sex", "age_start", "age_end", "measure", "year_start", "year_end",  "dorm_alt", "prev_alt", "prev_se_alt", "dorm_ref", "prev_ref", "prev_se_ref","logit_diff", "logit_diff_se" )]
df_plot <- merge(df_plot, preds, by = "dorm_alt")
df_plot[, `:=` (prev_ref = round(prev_ref, digits = 5), prev_alt = round(prev_alt, digits = 5))]
df_plot[, `:=` (prev_ref = signif(prev_ref, digits = 4), prev_alt = signif(prev_alt, digits = 4))]
mod <- rma(yi=logit_diff, sei=logit_diff_se, data=df_plot, measure="RR")



## Decrease margins so the full space is used
par(mar=c(5,4,5,4))  #margin size for bottom, left, top, right

## Convert sect from factor to character for use in forest plot
df_plot$sex <- as.character(df_plot$sex)

## Make forest plot of ratios
op <- par(cex=0.8, font=1)
forest(mod,
       atransf =transf.ilogit,
       alim = c(-1, 1),
       xlim=c(-8,4),
       ilab=cbind(df_plot$nid, df_plot$location_name, df_plot$sex, df_plot$age_start, "-", df_plot$age_end, df_plot$year_start,"-", df_plot$year_end, df_plot$prev_alt, df_plot$prev_ref),
       ilab.xpos=c(-6.8, -5.7, -4.7,-4.27,-4.13, -4,-3.5,-3.25,-3, -2.2, -1.2), 
       xlab="Logit difference: (Alternative) - (Reference)", mlab="",
       order=order(df_plot$location_name),
       #rows=c(3:4,9:15,20:70),
       psize=1,
       cex.axis	=1,
       cex=1, font=1
)

op <- par(cex=0.8, font=1)
text(c(-6.8, -5.7, -4.7,-4.1, -3.25, -2.2, -1.2, 3), 13.3, c("Study NID", "Location", "Sex", "Age", "Year", "Alternative", "Reference","Logit difference [95% CI]"))
op <- par(cex=1, font=1)
text(c(-1.7), 13.7, c("Prevalence"))
op <- par(cex=1.5, font=1)
text(c(-1.5), 14.3, c("IBD/CD/UC: Within-Study Pairs (Prevalence)"))


######################################################################################################  
# Step 5. Apply crosswalk coefficients found in step 4
######################################################################################################  
#adjust_orig_vals() adjusts the original data 
df_original <- subset(final_dt, crosswalk==0  | is.na(crosswalk))
df_original <- subset(df_original, is_outlier==0  | is.na(is_outlier))
df_original <- subset(df_original, group_review==1  | is.na(group_review))

df_original$obs_methods[df_original$cv_diagn_admin_data==1] <- "ICD_based"
df_original$obs_methods[df_original$cv_chart_review==1] <- "Stringent"
df_original$obs_methods[df_original$canada_microdata==1] <- "Stringent"
df_original$obs_methods[df_original$cv_diag_exam==1] <- "Stringent"
df_original$obs_methods[df_original$confirmed_before_entered_in_database==1] <- "Stringent"

test <- subset(df_original, is.na(obs_methods))
df_original <- subset(df_original, obs_methods!="")

#set aside data rows with mean = 0
zero <- subset(df_original, mean==0)
unique(zero$obs_methods)
non_zero <- subset(df_original, mean!=0)


# apply crosswalk coefficients to original data
setnames(non_zero, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
non_zero[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
                                                                                            fit_object = results_pre,             # result of CWModel()
                                                                                            df = non_zero,                        # original data with obs to be adjusted
                                                                                            orig_dorms = "obs_methods",           # name of column with (all) def/method levels
                                                                                            orig_vals_mean = "orig_mean",         # original mean
                                                                                            orig_vals_se = "orig_standard_error"  # standard error of original mean
                                                                                          )


#check if crosswalk was done properly
test <- subset(non_zero, obs_methods=="ICD_based" & measure=="prevalence")
test %>% 
  select(obs_methods, location_name, age_start, age_end,  mean, standard_error , orig_mean, orig_standard_error, diff, diff_se) %>%
  head(10)


final <- as.data.table(rbind.fill(non_zero, zero))

final$dup[duplicated(final$seq)] <-1
final$dup[is.na(final$dup)] <-0

final$seq[final$dup==1] <-NA
final$unit_value_as_published[is.na(final$unit_value_as_published)] <- 1
final[(is.na(lower)), `:=` (uncertainty_type_value=NA)]

write.csv(final,paste0(save_path,"xwalked_lit_data_", date,".csv" ))


#########################################################################################
## STEP 6 UPLOAD TO AGE-SPLIT MODEL
#########################################################################################  
agesplit <- as.data.table(copy(final))
agesplit$midage <- agesplit$age_end - agesplit$age_start
agesplit <- subset(agesplit, midage <26)
agesplit[(obs_methods=="ICD_based"), crosswalk_parent_seq := seq]

write.xlsx(agesplit, output_filepath_agesplit, sheetName = "extraction", col.names=TRUE)

description <- DESCRIPTION 
result <- save_crosswalk_version(step2_bundle_version, output_filepath_agesplit, description=description)

#########################################################################################
#STEP 7: AGE SPLITTING: DONE SEPARATELY FOR UC AND CD
#########################################################################################
# GET OBJECTS -------------------------------------------------------------
b_id <- BUNDLE_ID
a_cause <- CAUSE_ID
name <- "UC"
bundle_version_id <- step2_bundle_version
id <-    MEID    
measure  <- 5 #5 is prevalence 6 is incidence
region_pattern <- T


final[obs_methods=="ICD_based", `:=` (crosswalk_parent_seq=seq)]
dt <- copy(final)

dt_inc <- subset(dt, measure=="incidence")
dt_prev <- subset(dt, measure=="prevalence")

repo_dir <- paste0(h_root, "Repos/shared-code/")
date <- gsub("-", "_", date)
draws <- paste0("draw_", 0:999)

# GET FUNCTIONS -----------------------------------------------------------
## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## GET CASES IF THEY ARE MISSING
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("proportion", "prevalence", "incidence"),] 
  dt <- dt[!group_review==0 | is.na(group_review),] ##don't use group_review 0
  dt <- dt[is_outlier==0,] ##don't age split outliered data
  dt <- dt[(age_end-age_start)>25 & cv_literature==1 ,] #for prevelance, incidence, proportion
  dt <- dt[(!mean == 0 & !cases == 0) |(!mean == 0 & is.na(cases))  , ] 
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "proportion", measure_id := 18]
  dt[measure == "prevalence", measure_id := 5]
  dt[measure == "incidence", measure_id := 6]
  
  dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
  return(dt)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)
  
  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 99, age_end := 99]
  
  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  dt[, drop := cases/n.age] #
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  return(split)
}

## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                           measure_id = measure, location_id = locs, source = "epi", ##Measure ID 5= prev, 6=incidence, 18=proportion
                           version_id = id,  sex_id = c(1,2), gbd_round_id =7, decomp_step = "iterative", #can replace version_id with status = "best" or "latest"
                           age_group_id = age_groups, year_id = 2010) ##imposing age pattern
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, gbd_round_id =7, decomp_step = "iterative")
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]
  
  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
  age_pattern <- rbind(age_pattern, age_1)
  
  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 18, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]
  
  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id == 18, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)
  
  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years,decomp_step = "step2",
                                sex_id = c(1, 2, 3), age_group_id = age_groups)
  age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)  ##add age group id 1 back on
  return(populations)
}

## ACTUALLY SPLIT THE DATA
split_data <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[, total_pop := sum(population), by = "id"]
  dt[, sample_size := (population / total_pop) * sample_size]
  dt[, cases_dis := sample_size * rate_dis]
  dt[, total_cases_dis := sum(cases_dis), by = "id"]
  dt[, total_sample_size := sum(sample_size), by = "id"]
  dt[, all_age_rate := total_cases_dis/total_sample_size]
  dt[, ratio := mean / all_age_rate]
  dt[, mean := ratio * rate_dis ]
  dt <- dt[mean < 1, ]
  dt[, cases := mean * sample_size]
  return(dt)
}

## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age,sex"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  # dt <- col_order(dt)
  if (region == T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F]
  return(dt)
}

# RUN THESE CALLS ---------------------------------------------------------------------------
ages <- get_age_metadata(19, gbd_round_id = 7)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[age_start >= 5, age_group_id]

df <- as.data.table(copy(dt_prev))
df <- as.data.table(copy(dt_inc))

age <- age_groups
gbd_id <- id

location_pattern_id <- 1

# AGE SPLIT FUNCTION -----------------------------------------------------------------------
age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id){
  
  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  super_region_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]
  
  
  ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
  original <- copy(df)
  original[, id := 1:.N]
  
  ## FORMAT DATA
  dt <- format_data(original, sex_dt = sex_names)
  dt <- get_cases_sample_size(dt)
  dt <- get_se(dt)
  dt <- calculate_cases_fromse(dt)
  
  ## EXPAND AGE
  split_dt <- expand_age(dt, age_dt = ages)
  
  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify
  
  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_locs<-pop_locs[!pop_locs %in% NA]
  
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  
  
}

# Run functions
final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                 original_dt = original)

#final_dt_prev <- copy(final_dt)

# clean datasets before saving
append <- rbind.fill(final_dt, final_dt_prev)
append$dup[duplicated(append$seq)] <-1
append$dup[is.na(append$dup)] <-0
append$seq[append$dup==1] <-NA
append$crosswalk_parent_seq[append$dup==1] <-NA
append$cv_stringent <- NULL
append$standard_error[append$standard_error>1] <-1
append <- subset(append, location_id!=95 & location_id !=4620 & location_id !=4625)

output_filepathlit <- FILEPATH

write.xlsx(append, output_filepathlit, sheetName = "extraction", col.names=TRUE)

########################################################################################
#STEP 8: UPLOAD PROCESSED DATA
########################################################################################
path_to_data2 <- output_filepathlit
description2 <- DESCRIPTION 
result <- save_crosswalk_version( step2_bundle_version, path_to_data2, description=description2)

print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
print(sprintf('Crosswalk version ID from decomp 2/3 best model: %s', result$previous_step_crosswalk_version_id))

