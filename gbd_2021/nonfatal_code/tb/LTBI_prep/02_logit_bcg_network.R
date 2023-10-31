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
source(paste0(ADDRESS, "FILEPATH/save_crosswalk_version.R"))
library(splitstackshape)
library(ggplot2)
library(gtools)
library(readxl)
library(writexl)

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
brt_out_dir  <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)
ifelse(!dir.exists(adj_data_dir), dir.create(save_dir), FALSE)
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

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

## COLS TO KEEP
my_cols     <- c("nid", "location_id", "location_name", "sex", "year_start", "year_end", "age_start",
                 "age_end", "mean", "standard_error", "cv_bcg_scar", "cv_bcg_mixed")

## LOAD AND CLEAN DATA
dt <- as.data.table(read_excel(paste0(adj_data_dir, decomp_step, "_sex_split_data.xlsx")))
dt <- dt[, my_cols, with = F]
dt <- dt[sex != "Both"]
dt <- getanID(dt, names(dt)[!names(dt) %like% "mean|err"])
setnames(dt, ".id", "ID")

## DIFFERENT DATA SETS FOR EACH BCG STATUS
ref <- dt[(cv_bcg_scar == 0) & (cv_bcg_mixed == 0)]
mix <- dt[cv_bcg_mixed == 1]
yes <- dt[cv_bcg_scar == 1]

## PREP FOR MERGE
setnames(ref, c("mean", "standard_error"), c("ref_mean", "ref_se"))
setnames(mix, c("mean", "standard_error"), c("mix_mean", "mix_se"))
setnames(yes, c("mean", "standard_error"), c("yes_mean", "yes_se"))

# REMOVE INDICATOR COLUMNS
ref[, `:=` (cv_bcg_scar = NULL, cv_bcg_mixed = NULL)]
mix[, `:=` (cv_bcg_scar = NULL, cv_bcg_mixed = NULL)]
yes[, `:=` (cv_bcg_scar = NULL, cv_bcg_mixed = NULL)]

## MERGE
yes_ref <- merge(yes, ref, by = names(ref)[!names(ref) %like% "ref"])
mix_yes <- merge(mix, yes, by = names(yes)[!names(yes) %like% "yes"])
mix_ref <- merge(mix, ref, by = names(ref)[!names(ref) %like% "ref"])

## LOGIT TRANSFOMRATION OF SE
yes_ref[, `:=` (yes_se = sqrt(yes_se^2*(1/(yes_mean*(1-yes_mean)))^2), ref_se = sqrt(ref_se^2*(1/(ref_mean*(1-ref_mean)))^2))]
mix_ref[, `:=` (mix_se = sqrt(mix_se^2*(1/(mix_mean*(1-mix_mean)))^2), ref_se = sqrt(ref_se^2*(1/(ref_mean*(1-ref_mean)))^2))]
mix_yes[, `:=` (mix_se = sqrt(mix_se^2*(1/(mix_mean*(1-mix_mean)))^2), yes_se = sqrt(yes_se^2*(1/(yes_mean*(1-yes_mean)))^2))]

## LOGIT TRANSFORMATION OF MEAN
yes_ref[, `:=` (yes_mean = logit(yes_mean), ref_mean = logit(ref_mean))]
mix_ref[, `:=` (mix_mean = logit(mix_mean), ref_mean = logit(ref_mean))]
mix_yes[, `:=` (mix_mean = logit(mix_mean), yes_mean = logit(yes_mean))]

## COMPUTE DIFFERENCE
yes_ref[, diff := yes_mean - ref_mean][, se_diff := sqrt(yes_se^2 + ref_se^2)]
mix_ref[, diff := mix_mean - ref_mean][, se_diff := sqrt(mix_se^2 + ref_se^2)]
mix_yes[, diff := mix_mean - yes_mean][, se_diff := sqrt(mix_se^2 + yes_se^2)]

## PREP TO APPEND
yes_ref <- yes_ref[, .SD, .SDcols = names(yes_ref)[!names(yes_ref) %like% "ID|_mean|_se"]]
mix_ref <- mix_ref[, .SD, .SDcols = names(yes_ref)[!names(yes_ref) %like% "ID|_mean|_se"]]
mix_yes <- mix_yes[, .SD, .SDcols = names(yes_ref)[!names(yes_ref) %like% "ID|_mean|_se"]]

## CREATE NETWORK DUMMY
yes_ref[, yes_ref := 1][, mix_ref := 0]
mix_ref[, yes_ref := 0][, mix_ref := 1]
mix_yes[, yes_ref :=-1][, mix_ref := 1]

## APPEND
dt <- do.call("rbind", list(yes_ref, mix_ref, mix_yes))
dt <- dt[order(nid, year_start, location_name, sex, age_start)]
fwrite(dt, paste0(input_dir, "network_crosswalk_input.csv"), row.names = F)

#############################################################################################
###                             RUN NETWORK ANALYSIS IN MR-BRT                            ###
#############################################################################################

## GET DATA
dt <- fread(paste0(input_dir, "network_crosswalk_input.csv"))

## CREATE COV MATRIX
covariates <- c("yes_ref", "mix_ref")
covs1      <- list()
for (nm in covariates) covs1 <- append(covs1, list(cov_info(nm, "X")))

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "bcg_network_logit_test",
  data        = dt,
  mean_var    = "diff",
  se_var      = "se_diff",
  covs        = covs1,
  method      = "trim_maxL",
  #study_id    = "nid",
  trim_pct    = 0.10,
  remove_x_intercept = TRUE,
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

#############################################################################################
###                                 BEGIN PREDICTIONS                                     ###
#############################################################################################

## GET DATA THAT WILL BE USED FOR PREDICTIONS
orig_dt  <- as.data.table(read_excel(paste0(adj_data_dir, decomp_step, "_sex_split_data.xlsx")))
unadj_dt <- orig_dt[((cv_bcg_scar == 1) | (cv_bcg_mixed == 1)) & (group_review == 1)]

## CREATE DUMMIES FOR PREDICTIONS
unadj_dt[cv_bcg_scar  == 1, c("yes_ref", "mix_ref") := .(1, 0)]
unadj_dt[cv_bcg_mixed == 1, c("yes_ref", "mix_ref") := .(0, 1)]

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

## MERGE PREDICTIONS TO DATA
pred_summaries <- pred_summaries[, .(pred_mean, pred_se)]
unadj_dt       <- cbind(unadj_dt, pred_summaries)

#############################################################################################
###                                     ADJUST DATA                                       ###
#############################################################################################

## DO COMPUTATIONS IN LOG SPACE
unadj_dt[is.na(standard_error), standard_error := ((upper - lower) / 3.92)]
unadj_dt[, logit_se   := standard_error^2*(1/(mean*(1-mean)))^2]
unadj_dt[, logit_mean := logit(mean)]

## MAKE ADJUSTMENT
unadj_dt[, adj_logit_mean := logit_mean - pred_mean]
unadj_dt[, adj_logit_se   := sqrt(pred_se^2 + logit_se^2)]
unadj_dt[, adj_mean     := inv.logit(adj_logit_mean)]

unadj_dt$adj_se <- sapply(1:nrow(unadj_dt), function(i) {
  ratio_i    <- unadj_dt[i, adj_logit_mean]
  ratio_se_i <- unadj_dt[i, adj_logit_se]
  deltamethod(~exp(x1)/(1 + exp(x1)), ratio_i, ratio_se_i^2)
})

## OVERWRITE VALUES WITH ADJUSTED VALUES FOR SMEAR POS
unadj_dt[, mean := adj_mean]
unadj_dt[, standard_error := adj_se]
unadj_dt[, lower := mean - 1.96*standard_error]
unadj_dt[, upper := mean + 1.96*standard_error]
unadj_dt[lower < 0, lower := 0][upper > 1, upper := 1]

## PLOT ADJUSTED VALUES
plot_dt <- copy(unadj_dt)
plot_dt[cv_bcg_scar == 1, bcg_status := "BCG Positive"]
plot_dt[cv_bcg_mixed== 1, bcg_status := "BCG Mixed"]
ggplot(data=plot_dt, aes(x=inv.logit(logit_mean), y=mean, colour=bcg_status))+
  geom_point(size = 2.50, alpha = 0.30) +
  geom_abline(intercept = 0, slope = 1) + theme_bw() +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence", colour = "BCG Status") +
  geom_errorbar(aes(ymin = lower, ymax=upper), show.legend = F, size = 0.5, alpha = 0.45) +
  theme(legend.position = "bottom", legend.text=element_text(size=11),
        axis.title = element_text(size=14), axis.text = element_text(size = 11))

#############################################################################################
###                                       FORMAT                                          ###
#############################################################################################

## FORMAT
adj_dt <- copy(unadj_dt)
adj_dt[, uncertainty_type := "Standard error"]
adj_dt[, cases := NA][, sample_size := NA][, orig_mean := inv.logit(logit_mean)]
adj_dt[, `:=` (pred_mean = NULL, pred_se = NULL, logit_mean = NULL, yes_ref = NULL, mix_ref = NULL)]
adj_dt[, `:=` (logit_se = NULL, adj_logit_mean = NULL, adj_logit_se = NULL, adj_mean = NULL, adj_se = NULL)]

## LEAVE NOTE
adj_dt[, specificity := paste0(specificity, "; Adjusted to level of BCG negative using ratio from MR-BRT")]

## APPEND ADJUSTED DATA
new <- orig_dt[!(((cv_bcg_scar == 1) | (cv_bcg_mixed == 1)) & (group_review == 1))]
new[, orig_mean := mean]
new <- rbind(new, adj_dt)
new <- new[order(nid, ihme_loc_id, year_start, sex, age_start)]

## SAVE
writexl::write_xlsx(list(extraction = new), path = paste0(adj_data_dir, decomp_step, "_bcg_adj_data_logit.xlsx"))

#############################################################################################
###                                  REMOVE DUPLICATES                                    ###
#############################################################################################

## GET DATA
new <- as.data.table(read_excel(paste0(adj_data_dir, decomp_step, "_bcg_adj_data_logit.xlsx")))
dup <- unique(new[, .(nid, location_id, year_start, year_end, age_start, age_end, sex, cv_bcg_scar )])
dup <- dup[order(nid, location_id, year_start, cv_bcg_scar , sex, age_start)]

## CREATE REF AND ALT TABLES
dup_smear   <- dup[cv_bcg_scar == 1]
dup_nosmear <- dup[cv_bcg_scar == 0]
setnames(dup_nosmear, "cv_bcg_scar", "no_smear")

## IDENTIFY WHERE REF AND ALT ARE PRESENT
dup <- merge(dup_smear, dup_nosmear)
dup[, duplicate := 1]
dup[, cv_bcg_scar := NULL][, no_smear := NULL]

## CREATE DUPLICATE INDICATOR VARIABLE
new <- merge(new, dup, all.x = T, by = names(dup)[names(dup) != "duplicate"])
new[(duplicate == 1) & (cv_bcg_scar == 1), row_rem := 1]
new[is.na(row_rem), row_rem := 0]

## REMOVE DUPLICATE AND CLEAN
new <- new[row_rem == 0]
new[, `:=` (duplicate = NULL, row_rem = NULL)]
writexl::write_xlsx(list(extraction = new), path = paste0(adj_data_dir, decomp_step, "_bcg_adj_data_logit_rm_dups.xlsx"))

#############################################################################################
###                                       DONE                                            ###
#############################################################################################
