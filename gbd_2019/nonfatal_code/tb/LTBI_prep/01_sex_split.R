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
library(writexl)

source(paste0(ADDRESS, "FILEPATH/get_age_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_population.R"))
source(paste0(ADDRESS, "FILEPATH/get_bundle_data.R"))
source(paste0(ADDRESS, "FILEPATH/save_bundle_version.R"))
source(paste0(ADDRESS, "FILEPATH/get_bundle_version.R"))

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
date         <- "2019_09_22"
decomp_step  <- "step3"
input_dir    <- paste0(ADDRESS, "FILEPATH")
adj_data_dir <- paste0(ADDRESS, "FILEPATH")
save_dir     <- paste0(ADDRESS, "FILEPATH")
brt_out_dir  <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)
ifelse(!dir.exists(save_dir), dir.create(save_dir), FALSE)
ifelse(!dir.exists(input_dir), dir.create(input_dir), FALSE)

## HELPER FUNCTION
draw_summaries <- function(x, new_col, cols_sum, se = F) {

  x[, (paste0(new_col, "_lower")) := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_mean"))  := rowMeans(.SD), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_upper")) := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (cols_sum)]
  if (se == T) x[, paste0(new_col, "_se") := (get(paste0(new_col, "_upper"))-get(paste0(new_col, "_lower")))/3.92]
  if (se == T) x[get(paste0(new_col, "_se")) == 0, paste0(new_col, "_se") := apply(.SD, 1, sd, na.rm=T), .SDcols = (cols_sum)]
  x <- x[, !cols_sum, with = F]
  return(x)
}

age_dummies <- function(x) {

  x[, age := (age_start + age_end)/2]
  x[(age > 0) & (age < 5), age0to4 := 1][is.na(age0to4), age0to4 := 0]
  x[(age >= 5) & (age < 10), age5to9 := 1][is.na(age5to9), age5to9 := 0]
  x[(age >= 10) & (age < 20), age10to19 := 1][is.na(age10to19), age10to19 := 0]
  x[(age >= 20) & (age < 60), age20to59 := 1][is.na(age20to59), age20to59 := 0]
  x[(age >= 60) & (age <= 100), age60to100 := 1][is.na(age60to100), age60to100 := 0]
  x[, age := NULL]
  return(x)
}

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

## GET BUNDLE DATA AND VERSION
dt <- get_bundle_data(bundle_id = 711, decomp_step = decomp_step)
bundle_metadata <- save_bundle_version(bundle_id = 711, decomp_step = decomp_step)

dt <- get_bundle_version(bundle_version_id = bundle_metadata$bundle_version_id)
fwrite(bundle_metadata, file = paste0(input_dir, decomp_step, "_bundle_metadata.csv"), row.names = F)
fwrite(dt, file = paste0(adj_data_dir, decomp_step, "_", date, "_download.csv"), row.names = F)

## COLS TO KEEP
my_cols     <- c("nid", "location_id", "location_name", "sex", "year_start", "year_end", "age_start",
                 "age_end", "mean", "standard_error")

## LOAD AND CLEAN DATA
dt <- fread(paste0(adj_data_dir, decomp_step, "_", date, "_download.csv"))
dt <- dt[cv_bcg_scar == 0 & cv_bcg_mixed == 0]
dt <- dt[, my_cols, with = F]

## DIFFERENT DATA SETS BY SEX
male   <- dt[sex == "Male"]
female <- dt[sex == "Female"]

## PREP FOR MERGE
setnames(male, c("mean", "standard_error"), c("male_mean", "male_se"))
setnames(female, c("mean", "standard_error"), c("female_mean", "female_se"))

male[, sex := NULL]
female[, sex := NULL]

## MERGE TO CREATE A SINGLE WIDE DATA SET
dt <- merge(male, female, by = names(male)[!names(male) %like% "male"], all.x = T, all.y = T)
fwrite(dt, paste0(input_dir, "sex_crosswalk_input.csv"), row.names = F)

#############################################################################################
###                               RUN MR-BRT FOR SEX RATIO                                ###
#############################################################################################

## GET DATA
dt <- fread(paste0(input_dir, "sex_crosswalk_input.csv"))
dt <- dt[complete.cases(dt)]
dt <- dt[male_mean != 0][female_mean != 0]

## AGE DUMMY
dt <- age_dummies(dt)

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
covariates <- c("age5to9", "age10to19", "age20to59", "age60to100")
covs1      <- list()
for (nm in covariates) covs1 <- append(covs1, list(cov_info(nm, "X")))

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
  trim_pct    = 0.10,
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

#############################################################################################
###                                 BEGIN PREDICTIONS                                     ###
#############################################################################################

## GET DATA TO PREDICT ON
orig_dt  <- fread(paste0(adj_data_dir, decomp_step, "_", date, "_download.csv"))
orig_dt[, crosswalk_parent_seq := seq]

unadj_dt <- orig_dt[sex == "Both" & group_review == 1]
orig_dt[sex == "Both" & group_review == 1, sex_split := 1]
orig_dt[is.na(sex_split), sex_split := 0]

## CREATE DUMMIES FOR PREDICTIONS
unadj_dt <- age_dummies(unadj_dt)

## START PREDICTION
pred1 <- predict_mr_brt(fit1, newdata = unadj_dt, write_draws = T)

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
draws    <- draws[, .SD, .SDcols = paste0("ratio_", 0:999)]
unadj_dt <- cbind(unadj_dt, draws)

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "age60to100", dose_vars = "age60to100")

#############################################################################################
###                                 PREP FOR SEX SPLIT                                    ###
#############################################################################################

## GET AGE GROUPS
ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
ages <- ages[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

## GET POPULATIONS
pops <- get_population(location_id  = unique(unadj_dt$location_id),
                       year_id      = min(unadj_dt$year_start):max(unadj_dt$year_end),
                       age_group_id = ages$age_group_id,
                       sex_id       = 1:3)

pops <- reshape(pops, idvar = c("age_group_id", "location_id", "year_id"), timevar = "sex_id", direction = "wide", drop = "run_id")
pops <- merge(pops, ages, by = "age_group_id")
setnames(pops, c("population.1", "population.2", "population.3"), c("male_pop", "female_pop", "both_pop"))

## CREATE CUSTOM AGE GROUPS FOR POPULATION
for (my_row in 1:nrow(unadj_dt)){
  # establish age groups we are aggregating
  tmp       <- unadj_dt[my_row]
  my_start  <- tmp[, age_start]
  my_end    <- tmp[, age_end]
  round_start <- 5*round(my_start/5)
  round_end   <- 5*round(my_end/5)
  if (round_end == 100) round_end <- 125
  if (round_start == round_end) round_end <- 5*round((my_end+3)/5)
  if (round_start == round_end) round_start <- 5*round((my_start-3)/5)
  # prep population
  tmp_pop <- pops[location_id == tmp$location_id & year_id == tmp$year_start]
  tmp_pop <- tmp_pop[(age_start >= round_start) & (age_end <= round_end)]
  # aggregate
  tmp_pop <- tmp_pop[, .(age_start = my_start, age_end = my_end, male_pop = sum(male_pop), female_pop = sum(female_pop), both_pop = sum(both_pop)),
                     by = .(location_id, year_id)]
  # append aggregated populations
  if (my_row == 1) new_pops <- copy(tmp_pop)
  if (my_row != 1) new_pops <- rbind(new_pops, tmp_pop)
}

## MERGE POPULATIONS AND CLEAN
unadj_dt[, year_id := year_start]
unadj_dt <- merge(unadj_dt, new_pops, by = c("age_start", "age_end", "location_id", "year_id"), all.x=T)
unadj_dt[, year_id := NULL]

#############################################################################################
###                                  BEGIN SEX SPLIT                                      ###
#############################################################################################

## EXPONENTIATE PREDICTION DRAWS
unadj_dt[, paste0("ratio_", 0:999) := lapply(0:999, function(x) exp(get(paste0("ratio_", x))))]

## GET 1,000 DRAWS OF PREVALENCE
unadj_dt[, log_mean := log(mean)]

unadj_dt$log_se <- sapply(1:nrow(unadj_dt), function(i) {
  mean_i <- unadj_dt[i, mean]
  se_i   <- unadj_dt[i, standard_error]
  deltamethod(~log(x1), mean_i, se_i^2)
})

unadj_dt[, paste0("both_prev_", 0:999) := lapply(0:999, function(x){
  rnorm(n = nrow(unadj_dt), mean = log_mean, sd = log_se)
})]

unadj_dt[, paste0("both_prev_", 0:999) := lapply(0:999, function(x) exp(get(paste0("both_prev_", x))) * 0.995)]
unadj_dt[, log_mean := NULL][, log_se := NULL]

## SEX SPLIT AT 1,000 DRAW LEVEL
unadj_dt[, paste0("prev_female_", 0:999) := lapply(0:999, function(x) get(paste0("both_prev_", x))*((both_pop)/(female_pop+get(paste0("ratio_", x))*male_pop)) )]
unadj_dt[, paste0("prev_male_", 0:999)   := lapply(0:999, function(x) get(paste0("ratio_", x))*get(paste0("prev_female_", x)))]

## COMPUTE MEAN AND UI
unadj_dt <- draw_summaries(unadj_dt, "male",   paste0("prev_male_", 0:999), T)
unadj_dt <- draw_summaries(unadj_dt, "female", paste0("prev_female_", 0:999), T)

#############################################################################################
###                                         FORMAT                                        ###
#############################################################################################

## CLEAN POST SEX-SPLIT TABLE
cols_rem <- names(unadj_dt)[names(unadj_dt) %like% "ratio_|prev_|_pop"]
unadj_dt  <- unadj_dt[, .SD, .SDcols = -cols_rem]

## CREATE A MALE TABLE
male <- copy(unadj_dt)
male <- male[, .SD, .SDcols = -names(unadj_dt)[names(unadj_dt) %like% "female"]]

male[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
male[, sex := "Male"]
setnames(male, c("male_lower", "male_mean", "male_upper", "male_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A FEMALE TABLE
female <- copy(unadj_dt)

female[, `:=` (male_lower = NULL, male_mean = NULL, male_upper = NULL, male_se = NULL)]
female[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
female[, sex := "Female"]
setnames(female, c("female_lower", "female_mean", "female_upper", "female_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A BOTH SEX TABLE
both <- copy(unadj_dt)
both <- both[, .SD, .SDcols = -names(unadj_dt)[names(unadj_dt) %like% "male"]]

## SEX-SPEC TABLE
sex_spec <- rbind(male, female)
sex_spec[, sex_split := 1]
sex_spec[, uncertainty_type := "Standard error"]
sex_spec[, `:=` (age0to4  = NULL, age5to9 = NULL, age10to19 = NULL, age20to59 = NULL, age60to100 = NULL)]
sex_spec[, cases := NA][, sample_size := NA]

## FORMAT
sex_spec[, specificity := paste0(specificity, "; Sex split using ratio from MR-BRT")]

## COMPUTE UI
sex_spec[, lower := mean-1.96*standard_error][, upper := mean+1.96*standard_error]
sex_spec[lower < 0, lower := 0]

## APPEND THE SEX SPECIFC ESTIMATES
sex_adj_dt <- copy(orig_dt)
sex_adj_dt <- sex_adj_dt[sex_split == 0]
sex_adj_dt <- rbind(sex_adj_dt, sex_spec)
sex_adj_dt <- sex_adj_dt[order(nid, ihme_loc_id, year_start, sex, age_start)]

## SAVE
writexl::write_xlsx(list(extraction = sex_adj_dt), path = paste0(save_dir, decomp_step, "_sex_split_data.xlsx"))

#############################################################################################
###                                      DONE                                             ###
#############################################################################################

## PLOTTING
plot_tp <- copy(both)
setnames(plot_tp, c("lower", "mean", "upper", "standard_error"), c("b_lower", "b_mean", "b_upper", "b_standard_error"))
plot_tp[, sex := NULL]

plot_dt1 <- merge(male, plot_tp, by = names(plot_tp)[!names(plot_tp) %like% "b_"])
plot_dt2 <- merge(female, plot_tp, by = names(plot_tp)[!names(plot_tp) %like% "b_"])
plot_dt3 <- rbind(plot_dt1, plot_dt2)

ggplot(data = plot_dt3, aes(x=b_mean,y=mean, colour = sex)) +
  geom_point(size = 3.25, alpha = 0.55) +
  geom_errorbar(aes(ymin = lower, ymax=upper), show.legend = F, size = 0.5, alpha = 0.30) +
  geom_abline(slope = 1, intercept = 0) +
  labs(x="Both sex prevalence", y="Sex specific prevalence", colour = "Sex") +
  scale_color_manual(values = c("indianred3", "darkslategray4")) + theme_bw() +
  theme(legend.position = "bottom", legend.text=element_text(size=12), strip.text = element_text(size=13),
        axis.title = element_text(size=14), axis.text = element_text(size = 11), legend.title = element_text(size=12))

