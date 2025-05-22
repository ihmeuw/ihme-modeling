##########################################################################
### Purpose: Data processing - sex splitting, crosswalking, cirrhosis
##########################################################################

source(FILEPATH)
source(FILEPATH)

# ARGS
gbd_round_id <- 7
decomp_step <- "iterative"

## We need the population for each data point. Instead of pooling the population (for ex if you were age 3-10) for all years, we just take the midpoint (3-10 would become 6)
split_both_sex <- function(full_dt, sex_results) {

# The offset does not affect the estimated value of pred_diff_se or pred_diff_mean but is needed to run through adjust_orig_vals
offset <- 0.0000001
  
full_dt <- get_cases_sample_size(full_dt)
full_dt <- get_se(full_dt)
full_dt <- calculate_cases_fromse(full_dt)
n <- names(full_dt)

both_sex <- full_dt[sex == "Both" & measure == measures]
both_sex[, sex_dummy := "Female"]
both_zero_rows <- nrow(both_sex[mean == 0, ]) #Both sex, zero mean data 
message(paste0("There are ", both_zero_rows, " data points that will be offset by ", offset))
both_sex[mean == 0, mean := offset]  # apply offset to mean zero data points 
sex_specific <- copy(full_dt[sex != "Both"]) # sex specific data before sex splitting 

both_sex[, c("ref_vals_mean", "ref_vals_sd", "pred_diff_mean", "pred_diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = sex_results,       # result of CWModel()
  df = both_sex,            # original data with obs to be adjusted
  orig_dorms = "sex_dummy", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error",  # standard error of original mean
)

# the value of pred_diff_mean and pred_diff_se is not impacted by adding an offset 

log_ratio_mean <- both_sex$pred_diff_mean
log_ratio_se <- sqrt(both_sex$pred_diff_se^2 + as.numeric(sex_results$gamma))

adjust_both <- copy(both_sex)

## To return later
adjust_both[, year_mid := (year_start + year_end)/2]
adjust_both[, year_floor := floor(year_mid)]
adjust_both[, age_mid := (age_start + age_end)/2]
adjust_both[, age_floor:= floor(age_mid)]

## Pull out population data. We need age- sex- location-specific population.
message("Getting population")
pop <- get_population(age_group_id = "all", sex_id = "all", decomp_step = decomp_step, year_id = unique(floor(adjust_both$year_mid)),
                      location_id=unique(adjust_both$location_id), single_year_age = T, gbd_round_id = gbd_round_id) 
ids <- get_ids("age_group") ## age group IDs
pop <- merge(pop, ids, by="age_group_id", all.x=T, all.y=F)
# Age group names are sometimes characters... I only really need the ones that are numeric on their own.
pop$age_group_name <- as.numeric(pop$age_group_name)
pop <- pop[!(is.na(age_group_name))]
pop$age_group_id <- NULL

## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
adjust_both <- merge(adjust_both, pop[sex_id==3,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(adjust_both, "population", "population_both")
adjust_both <- merge(adjust_both, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(adjust_both, "population", "population_male")
adjust_both <- merge(adjust_both, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("year_floor", "age_floor", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
setnames(adjust_both, "population", "population_female")

## Take mean, SEs into real-space so we can combine and make adjustments
adjust_both[, se_val  := unique(sqrt(both_sex$pred_diff_se^2 + as.vector(sex_results$gamma)))]
adjust_both[, c("real_pred_mean", "real_pred_se") := data.table(delta_transform(mean = pred_diff_mean, sd = se_val, transform = "log_to_linear"))]
# convert pred_diff_mean because we want to the ratio of female/male in linear space to apply to populations 

# Subtract the offset before making any adjustments
adjust_both[mean == paste0(offset), mean := 0 ] 

## Make adjustments
adjust_both[, m_mean := mean * (population_both/(population_male + real_pred_mean * population_female))]
adjust_both[, f_mean := real_pred_mean * m_mean]

## Get combined standard errors
adjust_both[, m_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]
adjust_both[, f_standard_error := sqrt((real_pred_se^2 * standard_error^2) + (real_pred_se^2 * mean^2) + (standard_error^2 * real_pred_mean^2))]

## Adjust the standard error of mean 0 data points 
adjust_both[mean == 0, `:=` (m_standard_error = sqrt(2)*standard_error, f_standard_error = sqrt(2)*standard_error)]

## Make male- and female-specific dts
message("Getting male and female specific data tables")
male_dt <- copy(adjust_both)
male_dt[, `:=` (mean = m_mean, standard_error = m_standard_error, upper = NA, lower = NA,
                cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                      real_pred_se, ")"))]
male_dt <- dplyr::select(male_dt, all_of(n))
female_dt <- copy(adjust_both)
female_dt[, `:=` (mean = f_mean, standard_error = f_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", real_pred_mean, " (",
                                        real_pred_se, ")"))]
female_dt <- dplyr::select(female_dt, all_of(n))
dt_all <- rbindlist(list(sex_specific, female_dt, male_dt))

sex_specific_data <- copy(dt_all)
drop_nids <- setdiff(full_dt[sex == "Both" & measure == measures, unique(nid)], adjust_both$nid)
message("Dropped nids:", list(drop_nids))

message("Finished sex splitting.")
return(list(final = sex_specific_data, graph = adjust_both, missing_nids = drop_nids))
}

graph_sex_predictions <- function(dt){
  graph_dt <- copy(dt[measure == measures, .(age_start, age_end, mean, m_mean, m_standard_error, f_mean, f_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_mean", "f_mean"))
  graph_dt_means[variable == "f_mean", variable := "Female"][variable == "m_mean", variable := "Male"]
  graph_dt_means[, id := .I]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("m_standard_error", "f_standard_error"))
  graph_dt_error[variable == "f_standard_error", variable := "Female"][variable == "m_standard_error", variable := "Male"]
  graph_dt_error[, id := .I]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable", "id"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  # wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  # graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    # geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}
