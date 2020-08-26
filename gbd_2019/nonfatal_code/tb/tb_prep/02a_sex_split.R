## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS
library(msm)
library(ggplot2)
library(readxl)

source(paste0(ADDRESS, "FILEPATH/get_age_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_population.R"))

## SOURCE MR-BRT
repo_dir <- paste0(ADDRESS, "FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
date         <- "2020_01_22"
decomp_step  <- "iterative"
input_dir    <- paste0(ADDRESS, "FILEPATH")
adj_data_dir <- paste0(ADDRESS, "FILEPATH")
save_dir     <- paste0(ADDRESS, "FILEPATH")
brt_out_dir  <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)
ifelse(!dir.exists(save_dir), dir.create(save_dir), FALSE)
ifelse(!dir.exists(input_dir), dir.create(input_dir), FALSE)

## HELPER FUNCTION
draw_summaries <- function(x, new_col, cols_sum, se = F){

  x[, (paste0(new_col, "_lower")) := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_mean"))  := rowMeans(.SD), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_upper")) := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (cols_sum)]
  if (se == T) x[, paste0(new_col, "_se") := (get(paste0(new_col, "_upper"))-get(paste0(new_col, "_lower")))/3.92]
  if (se == T) x[get(paste0(new_col, "_se")) == 0, paste0(new_col, "_se") := apply(.SD, 1, sd, na.rm=T), .SDcols = (cols_sum)]
  x <- x[, !cols_sum, with = F]
  return(x)
}

#############################################################################################
###                               RUN MR-BRT FOR SEX RATIO                                ###
#############################################################################################

## GET DATA
dt <- fread(paste0(input_dir, "sex_crosswalk_input.csv"))
dt <- dt[complete.cases(dt)]
dt <- dt[!male_mean == 0][!female_mean == 0]
dt[, age := (age_start + age_end)/2]

## COMPUTE RATIO AND SE
dt[, ratio := male_mean / female_mean]
dt[, ratio_se := sqrt(ratio*((male_se^2/male_mean^2)+(female_se^2/female_mean^2)))]

## LOG TRANSFORMATIONS
dt[, ratio_log := log(ratio)]

dt$ratio_se_log <- sapply(1:nrow(dt), function(i) {
  ratio_i    <- dt[i, "ratio"]
  ratio_se_i <- dt[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## CREATE COV MATRIX
covariates <- c("age")
covs1      <- list()
for (nm in covariates) covs1 <- append(covs1, list(cov_info(nm, "X")))

covs1[[1]]$degree    <- 2
covs1[[1]]$n_i_knots <- 2

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "sex_10p",
  data        = dt,
  covs        = covs1,
  mean_var    = "ratio_log",
  se_var      = "ratio_se_log",
  method      = "trim_maxL",
  study_id    = "nid",
  trim_pct    = 0.15,
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

#############################################################################################
###                                 BEGIN PREDICTIONS                                     ###
#############################################################################################

## LOAD DATA FOR PREDICTIONS
unadj_dt <- fread(paste0(adj_data_dir, decomp_step, "_ep_adj_age_split_", date, ".csv"))
unadj_dt <- unadj_dt[measure == "prevalence"]

## SUBSET TO DATA THAT NEEDS ADJUSTMENTS
sex_adj  <- unadj_dt[sex=="Both" & (group_review == 1 | is.na(group_review))]
unadj_dt[sex=="Both" & (group_review == 1 | is.na(group_review)), sex_split := 1][is.na(sex_split), sex_split := 0]

## CREATE AGE VAR FOR PREDICTIONS
sex_adj[, age := (age_start+age_end)/2]

## START PREDICTION
pred1 <- predict_mr_brt(fit1, newdata = sex_adj, write_draws = T)

## CHECK FOR PREDICTIONS
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

## GET PREDICTION DRAWS
preds <- as.data.table(pred_object$model_summaries)
draws <- as.data.table(pred_object$model_draws)

## COMPUTE MEAN AND CI OF PREDICTION DRAWS
pred_summaries <- copy(draws)
pred_summaries <- draw_summaries(pred_summaries, "pred",  paste0("draw_", 0:999), T)

## MERGE PREDICTIONS TO DATA THAT NEEDS TO BE SPLIT
setnames(draws, paste0("draw_", 0:999), paste0("ratio_", 0:999))
draws   <- draws[, .SD, .SDcols = paste0("ratio_", 0:999)]
sex_adj <- cbind(sex_adj, draws)

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "age", dose_vars = "age")

#############################################################################################
###                                 PREP FOR SEX SPLIT                                    ###
#############################################################################################

## GET AGE GROUPS
ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
ages <- ages[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

## GET POPULATIONS
pops <- get_population(location_id  = unique(sex_adj$location_id),
                       year_id      = min(sex_adj$year_start):max(sex_adj$year_end),
                       age_group_id = ages$age_group_id,
                       sex_id       = 1:3,
                       decomp_step  = "step1",
                       gbd_round_id = 6)

pops <- reshape(pops, idvar = c("age_group_id", "location_id", "year_id"), timevar = "sex_id", direction = "wide", drop = "run_id")
pops <- merge(pops, ages, by = "age_group_id")
setnames(pops, c("population.1", "population.2", "population.3"), c("male_pop", "female_pop", "both_pop"))

## CREATE CUSTOM AGE GROUPS FOR POPULATION
for (my_row in 1:nrow(sex_adj)){
  # establish age groups we are aggregating
  tmp       <- sex_adj[my_row]
  my_start  <- tmp[, age_start]
  my_end    <- tmp[, age_end]
  round_end <- 5*round(my_end/5)
  if (round_end == 100) round_end <- 125
  # prep population
  tmp_pop <- pops[location_id == tmp$location_id & year_id == tmp$year_start]
  tmp_pop <- tmp_pop[(age_start >= my_start) & (age_end <= round_end)]
  # aggregate
  tmp_pop <- tmp_pop[, .(age_start = my_start, age_end = my_end, male_pop = sum(male_pop), female_pop = sum(female_pop), both_pop = sum(both_pop)),
                     by = .(location_id, year_id)]
  # append aggregated populations
  if (my_row == 1) new_pops <- copy(tmp_pop)
  if (my_row != 1) new_pops <- rbind(new_pops, tmp_pop)
}

## MERGE POPULATIONS AND CLEAN
sex_adj[, year_id := year_start]
sex_adj <- merge(sex_adj, new_pops, by = c("age_start", "age_end", "location_id", "year_id"), all.x=T)
sex_adj[, year_id := NULL]

#############################################################################################
###                                  BEGIN SEX SPLIT                                      ###
#############################################################################################

## FIX ROWS WITHOUT SAMPLE SIZE
sex_adj[is.na(sample_size), new_se := (upper-lower)/3.92]
sex_adj[is.na(sample_size), sample_size := (mean(1-mean))/new_se^2]
sex_adj[, new_se := NULL]

## EXPONENTIATE PREDICTION DRAWS
sex_adj[, paste0("ratio_", 0:999) := lapply(0:999, function(x) exp(get(paste0("ratio_", x))))]

## GET 1,000 DRAWS OF PREVALENCE
sex_adj[, paste0("prev_", 0:999) := lapply(0:999, function(x){
  rbinom(n = nrow(sex_adj), size = round(sample_size), prob = mean) / round(sample_size)
})]

## SEX SPLIT AT 1,000 DRAW LEVEL
sex_adj[, paste0("prev_female_", 0:999) := lapply(0:999, function(x) get(paste0("prev_", x))*((both_pop)/(female_pop+get(paste0("ratio_", x))*male_pop)) )]
sex_adj[, paste0("prev_male_", 0:999)   := lapply(0:999, function(x) get(paste0("ratio_", x))*get(paste0("prev_female_", x)))]

## COMPUTE MEAN AND UI
sex_adj <- draw_summaries(sex_adj, "male",   paste0("prev_male_", 0:999), T)
sex_adj <- draw_summaries(sex_adj, "female", paste0("prev_female_", 0:999), T)

#############################################################################################
###                                         FORMAT                                        ###
#############################################################################################

## CLEAN POST SEX-SPLIT TABLE
cols_rem <- names(sex_adj)[names(sex_adj) %like% "ratio_|prev_|_pop"]
sex_adj  <- sex_adj[, .SD, .SDcols = -cols_rem]

## CREATE A MALE TABLE
male <- copy(sex_adj)
male <- male[, .SD, .SDcols = -names(sex_adj)[names(sex_adj) %like% "female"]]

male[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
male[, sex := "Male"]
setnames(male, c("male_lower", "male_mean", "male_upper", "male_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A FEMALE TABLE
female <- copy(sex_adj)

female[, `:=` (male_lower = NULL, male_mean = NULL, male_upper = NULL, male_se = NULL)]
female[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
female[, sex := "Female"]
setnames(female, c("female_lower", "female_mean", "female_upper", "female_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A BOTH SEX TABLE
both <- copy(sex_adj)
both <- both[, .SD, .SDcols = -names(sex_adj)[names(sex_adj) %like% "male"]]

## SEX-SPEC TABLE
sex_spec <- rbind(male, female)
sex_spec[, sex_split := 1]
sex_spec[, uncertainty_type := "Standard error"]
sex_spec[, age := NULL][, cases := NA][, sample_size := NA]

## FORMATE
sex_spec[note_modeler == "" | note_modeler == ".", note_modeler := "Sex split using ratio from MR-BRT"]
sex_spec[note_modeler %like% "extraction", note_modeler := paste0(note_modeler, "; sex split using ratio from MR-BRT")]

## COMPUTE UI
sex_spec[, lower := mean-1.96*standard_error][, upper := mean+1.96*standard_error]
sex_spec[lower < 0, lower := 0]

## APPEND THE SEX SPECIFC ESTIMATES
sex_adj_dt <- copy(unadj_dt)
sex_adj_dt <- sex_adj_dt[sex_split == 0]
sex_adj_dt <- rbind(sex_adj_dt, sex_spec)
sex_adj_dt <- sex_adj_dt[order(seq)]

## SAVE
write.csv(sex_adj_dt, paste0(save_dir, "sex_split_data.csv"), row.names = F, na = "")

#############################################################################################
###                                      DONE                                             ###
#############################################################################################

## PREP FOR PLOTTING
setnames(both, c("lower", "mean", "upper", "standard_error"), c("both_lower", "both_mean", "both_upper", "both_se"))
both[, sex := NULL]
test1 <- merge(male, both, by = names(both)[!names(both) %like% "both"])
test2 <- merge(female, both, by = names(both)[!names(both) %like% "both"])
test3 <- rbind(test1,test2)
test3[, lower := mean-1.96*standard_error][, upper := mean+1.96*standard_error]
test3[lower < 0, lower := 0]

## PLOT DATA
ggplot(data = test3, aes(x=both_mean,y=mean, colour = sex)) +
  geom_point(size = 2.5, alpha = 0.75) +
  geom_errorbar(aes(ymin = lower, ymax=upper), show.legend = F, size = 0.5, alpha = 0.45) +
  geom_abline(slope = 1, intercept = 0) +
  labs(x="Both sex prevalence", y="Sex specific prevalence") +
  scale_color_manual(values = c("indianred3", "darkslategray4")) +
  theme_bw()


