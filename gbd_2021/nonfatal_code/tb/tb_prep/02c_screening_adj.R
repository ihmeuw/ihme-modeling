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
library(data.table)
library(ggplot2)

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
brt_out_dir  <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)
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
###                           RUN MR-BRT FOR SMEAR / BACT POS                             ###
#############################################################################################

## GET DATA FOR REFERENCE V SMEAR
data <- fread(paste0(input_dir, "screening_input.csv"))

## COMPUTE RATIO AND SE
data[, ratio := alt_mean / ref_mean]
data[, ratio_se := sqrt(ratio*((ref_se^2/ref_mean^2)+(alt_se^2/alt_mean^2)))]

## APPEND GUJURAT RATIO
gujurat <- data.table(location_id = 4851, ihme_loc_id = "IND_4851", location_name = "Gujarat",
                      sex = "Both", year_start = 2011, year_end = 2012, age_start = 0, age_end = 100,
                      ratio = (1/1.31))

data <- rbind(data, gujurat, fill = T)

## LOG TRANSFORMATIONS
data[, ratio_log := log(ratio)]

data$ratio_se_log <- sapply(1:nrow(data), function(i) {
  ratio_i    <- data[i, ratio]
  ratio_se_i <- data[i, ratio_se]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "screening",
  data        = data,
  mean_var    = "ratio_log",
  se_var      = "ratio_se_log",
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

#############################################################################################
###                                 BEGIN PREDICTIONS                                     ###
#############################################################################################

## GET DATA THAT WILL BE USED FOR PREDICTIONS
orig_dt  <- fread(paste0(adj_data_dir, "smear_adj_data.csv"))
unadj_dt <- orig_dt[cv_screening == 1]

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

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "intercept", dose_vars = "intercept")

#############################################################################################
###                                     ADJUST DATA                                       ###
#############################################################################################

## DO COMPUTATIONS IN LOG SPACE
unadj_dt[is.na(standard_error), standard_error := ((upper - lower) / 3.92)]
unadj_dt[, log_mean := log(mean)]

unadj_dt$log_se <- sapply(1:nrow(unadj_dt), function(i) {
  ratio_i    <- unadj_dt[i, mean]
  ratio_se_i <- unadj_dt[i, standard_error]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## MAKE ADJUSTMENT
unadj_dt[, adj_log_mean := log_mean - pred_mean]
unadj_dt[, adj_log_se   := sqrt(pred_se^2 + log_se^2)]
unadj_dt[, adj_mean     := exp(adj_log_mean)]

unadj_dt$adj_se <- sapply(1:nrow(unadj_dt), function(i) {
  ratio_i    <- unadj_dt[i, adj_log_mean]
  ratio_se_i <- unadj_dt[i, adj_log_se]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

## OVERWRITE VALUES WITH ADJUSTED VALUES FOR SMEAR POS
unadj_dt[, mean := adj_mean]
unadj_dt[, standard_error := adj_se]
unadj_dt[, lower := mean - 1.96*standard_error]
unadj_dt[, upper := mean + 1.96*standard_error]
unadj_dt[lower < 0, lower := 0][upper > 1, upper := 1]

## PLOT ADJUSTED VALUES
ggplot(data=unadj_dt, aes(x=exp(log_mean), y=mean))+
  geom_point(size = 2.50, alpha = 0.30) +
  geom_abline(intercept = 0, slope = 1) + theme_bw() +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence") +
  geom_errorbar(aes(ymin = lower, ymax=upper), show.legend = F, size = 0.5, alpha = 0.45) +
  theme(legend.position = "bottom", legend.text=element_text(size=11),
        axis.title = element_text(size=14), axis.text = element_text(size = 11))

#############################################################################################
###                                       FORMAT                                          ###
#############################################################################################

## FORMAT
adj_dt <- copy(unadj_dt)
adj_dt[, uncertainty_type := "Standard error"]
adj_dt[, cases := NA][, sample_size := NA]
adj_dt[, `:=` (pred_mean = NULL, pred_se = NULL, log_mean = NULL)]
adj_dt[, `:=` (log_se = NULL, adj_log_mean = NULL, adj_log_se = NULL, adj_mean = NULL, adj_se = NULL)]

## LEAVE NOTE
adj_dt[, note_modeler := paste0(note_modeler, "; Adjusted to level of both screening methods using ratio from MR-BRT")]
adj_dt[substr(note_modeler, start = 1, stop = 1) == ";", note_modeler := gsub("; ", "", note_modeler)]

## APPEND ADJUSTED DATA
new <- orig_dt[cv_screening == 0]
new <- rbind(new, adj_dt)
new <- new[order(seq)]

## SAVE
write.csv(new, paste0(adj_data_dir, "screening_adj_data.csv"), row.names = F)

#############################################################################################
###                                      DONE                                             ###
#############################################################################################

## PLOT ADJUSTED VALUES
new[cv_diag_smear == 0 & cv_screening == 0, adj := "no adjustment"]
new[cv_diag_smear == 0 & cv_screening == 1, adj := "screening adjustment only"]
new[cv_diag_smear == 1 & cv_screening == 0, adj := "smear adjustment only"]
new[cv_diag_smear == 1 & cv_screening == 1, adj := "smear and screening adjustment"]

ggplot(data=new[standard_error < 0.1], aes(x=orig_mean, y=mean, colour=factor(adj)))+
  geom_point(size = 2.50, alpha = 0.20) +
  geom_abline(intercept = 0, slope = 1) + theme_bw() +
  geom_errorbar(aes(ymin = lower, ymax=upper), show.legend = F, size = 0.5, alpha = 0.45) +
  labs(x = "Unadjusted prevalence", y = "Adjusted prevalence", colour = "Number of adjustments") +
  theme(legend.position = "bottom", legend.text=element_text(size=11),
        axis.title = element_text(size=14), axis.text = element_text(size = 11))

