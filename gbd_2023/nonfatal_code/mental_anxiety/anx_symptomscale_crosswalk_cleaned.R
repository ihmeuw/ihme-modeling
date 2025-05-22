
library(data.table)
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_draws.R")
library(reticulate)
reticulate::use_python("/FILEPATH/python")
mr <- import("mrtool")
rlogit <- function(x){exp(x)/(1+exp(x))}
logit <- function(x){log(x/(1-x))}
get_beta_vcov <- function(model){
  model_specs <- mr$core$other_sampling$extract_simple_lme_specs(model)
  beta_hessian <- mr$core$other_sampling$extract_simple_lme_hessian(model_specs)
  solve(beta_hessian)
}
get_beta_sd <- function(model){
  beta_sd <- sqrt(diag(get_beta_vcov(model)))
  names(beta_sd) <- model$cov_names
  return(beta_sd)
}

# Create dataset ----------------------------------------------------------

row_1 <- data.table(author = "Spitzer et al (a and b)", year_id = "2004, 2005", country = "USA", location_id = 102, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32", sens = 0.89, sens_lower = 0.73, sens_upper = 0.98, spec = 0.82, spec_lower = 0.79, spec_upper = 0.84)
row_2 <- data.table(author = "kroenke et al", year_id = "2004, 2005", country = "USA", location_id = 102, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30", sens = 0.68, sens_lower = 0.60, sens_upper = 0.74, spec = 0.88, spec_lower = 0.85, spec_upper = 0.9)
row_3 <- data.table(author = "Vrublevska et al", year_id = "2014, 2015, 2016, 2017", country = "Latvia", location_id = 59, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235", sens = 0.37, sens_lower = 0.27, sens_upper = 0.47, spec = 0.917, spec_lower = 0.902, spec_upper = 0.932)
row_4 <- data.table(author = "Ahn et al", year_id = "2015, 2016, 2017, 2018", country = "Republic of Korea", location_id = 68, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30", sens = 0.722, sens_lower = 0.625, sens_upper = 0.804, spec = 0.893, spec_lower = 0.885, spec_upper = 0.900)
row_5 <- data.table(author = "Konkan et al", year_id = "2010, 2011, 2012", country = "Turkey", location_id = 155, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17", sens = 0.37, sens_lower = 0.28, sens_upper = 0.460, spec = 0.845, spec_lower = 0.778, spec_upper = 0.912)
row_6 <- data.table(author = "Christensen", year_id = "2009", country = "Australia", location_id = 71, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17", sens = 0.606, sens_lower = 0.420, sens_upper = 0.770, spec = 0.876, spec_lower = 0.830, spec_upper = 0.910)
row_7 <- data.table(author = "Garcia-Campayo et al", year_id = "2007, 2008, 2009", country = "Spain", location_id = 92, sex_id = 3, age_ids = "8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30", sens = 0.868, sens_lower = 0.804, sens_upper = 0.932, spec = 0.929, spec_lower = 0.880, spec_upper = 0.978)
row_8 <- data.table(author = "Marlow et al", year_id = "2021", country = "South Africa", location_id = 196, sex_id = 3, age_ids = "7, 8", sens = 0.40, sens_lower = 0.257, sens_upper = 0.543, spec = 0.91, spec_lower = 0.875, spec_upper = 0.945)
row_9 <- data.table(author = "LP et al", year_id = "2019, 2020, 2021, 2022", country = "Hong Kong", location_id = 354, sex_id = 3, age_ids = "8, 9", sens = 0.627, sens_lower = 0.496, sens_upper = 0.758, spec = 0.883, spec_lower = 0.872, spec_upper = 0.894)

dataset <- rbind(row_1, row_2, row_3, row_4, row_5, row_6, row_7, row_8, row_9)

dataset[, row_id := seq_len(.N)]

# Load in GBD 2021 prevalence and associated meta-data ---------------------------------------------
age_metadata <- get_age_metadata(age_group_set_id = 24, release_id = 9)[,.(age_group_id, age_start = age_group_years_start, age_end = age_group_years_end-1, most_detailed)]
ages <- as.numeric(unlist(strsplit(dataset$age_ids, ", ")))
years <- as.numeric(unlist(strsplit(dataset$year_id, ", ")))
anxiety_prev <- get_draws(gbd_id_type = "cause_id", year_id = years, gbd_id = 571, sex_id = c(1, 2, 3), source = "como", age_group_id = ages, measure_id = 5, location_id = unique(dataset$location_id), metric_id = 3, status = 'best', release_id = 9)
anxiety_prev <- melt.data.table(anxiety_prev, id.vars = names(anxiety_prev)[!(names(anxiety_prev) %like% "draw")], value.name="anx_prev", variable.name="draw")
population <- get_population(location_id = unique(dataset$location_id), release_id = 9, age_group_id = ages, sex_id = c(1, 2, 3), year_id = years)

set.seed(4389)

for(i in dataset$row_id){
  sensitivity_draws <- data.table(draw = paste0("draw_", 0:499), sens = rnorm(500, mean = dataset[row_id == i, sens], sd = dataset[row_id == i, (sens_upper - sens_lower)/(qnorm(0.975, 0, 1)*2)]))
  specificity_draws <- data.table(draw = paste0("draw_", 0:499), spec = rnorm(500, mean = dataset[row_id == i, spec], sd = dataset[row_id == i, (spec_upper - spec_lower)/(qnorm(0.975, 0, 1)*2)]))
  
  years <- as.numeric(unlist(strsplit(dataset[row_id == i, year_id], ", ")))
  ages <- as.numeric(unlist(strsplit(dataset[row_id == i, age_ids], ", ")))
  sex <- as.numeric(dataset[row_id == i, sex_id])
  location <- as.numeric(dataset[row_id == i, location_id])
  anx_prev_draws <- anxiety_prev[(age_group_id %in% ages) & location_id == location & sex_id == sex & (year_id %in% years),]
  anx_prev_draws <- merge(anx_prev_draws, population, by = c('year_id', 'sex_id', 'age_group_id', 'location_id'), all.x = T)
  anx_prev_draws[, cases := anx_prev * population]
  anx_prev_draws[, `:=` (cases = sum(cases), population = sum(population)), by = c('draw')]
  anx_prev_draws[, `:=` (anx_prev = cases / population)]
  anx_prev_draws <- unique(anx_prev_draws[,.(draw, anx_prev)])
  anx_prev_draws <- merge(anx_prev_draws, sensitivity_draws, by = 'draw')
  anx_prev_draws <- merge(anx_prev_draws, specificity_draws, by = 'draw')
  anx_prev_draws[, gad7_prev := anx_prev * sens + (1-anx_prev)*(1-spec)]
  anx_prev_draws[, `:=` (anx_logit = logit(anx_prev), gad7_logit = logit(gad7_prev))]
  anx_prev_draws[, `:=` (logit_dif = gad7_logit - anx_logit)]
  dataset[row_id == i, `:=` (xwalk = mean(anx_prev_draws$logit_dif), xwalk_se = sd(anx_prev_draws$logit_dif))]
}

trimming <- 1

mr_dataset <- mr$MRData()
mr_dataset$load_df(
  data = dataset,
  col_obs = "xwalk", col_obs_se = "xwalk_se",
  col_covs = as.list(c("row_id")), col_study_id = "author" )

## Step 1
cov_list <- list(mr$LinearCovModel('intercept', use_re = T))
model <- mr$MRBRT(data = mr_dataset, cov_models =cov_list, inlier_pct =trimming)
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L, inner_acceptable_tol=1e-3)
betas <- data.table(cov = model$cov_names, coef = as.numeric(model$beta_soln), se = get_beta_sd(model))
betas[, `:=` (lower = coef-(qnorm(0.975)*se), upper = coef+(qnorm(0.975)*se), z = abs(coef/se), p = (1 - pnorm(abs(coef/se)))*2)]

write.csv(betas, "/FILEPATH/gad7.csv", row.names = F)

