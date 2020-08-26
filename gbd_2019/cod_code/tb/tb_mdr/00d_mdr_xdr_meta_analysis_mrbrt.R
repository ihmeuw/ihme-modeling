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
  ADDRESS <-paste0("ADDRESS", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS
library(msm)
library(data.table)
library(readxl)
source(paste0(ADDRESS, "FILEPATH/get_outputs.R"))

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
input_dir    <- paste0(ADDRESS, "FILEPATH/input/")
output_dir   <- paste0(ADDRESS, "FILEPATH/temp/")
brt_out_dir  <- paste0(ADDRESS, "FILEPATH/mr_brt_outputs/")
save_dir     <- "FILEPATH"

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
###                                 PREP ODDS RATIO DATA                                  ###
#############################################################################################

## GET DATA
data  <- data.table(read_excel(paste0(ADDRESS, "FILEPATH/MDR-TB_rr_extraction/MDR_RR_extraction_document_2018_05_15.xlsx")))
gbd19 <- as.data.table(read_excel(paste0(ADDRESS, "FILEPATH/MDR_RR_extraction_document_joyma_0816_complete.xlsx")))
gbd19 <- gbd19[file_name != "Zhang_2018"]
gbd19[file_name %like% "Gallo", year_end := 2016]
gbd19[file_name %like% "Viana", file_name := "Viana_2018"]

## CLEAN DATA
data[file_name %like% "Kim", risk := "xdr_tb"]
data  <- rbind(data, gbd19, fill = T)
data <- data[, .(file_name, location_id, HIV_negative_sample, risk, tb_category, sex, year_start, year_end, age_start, age_end, RR_mean,	RR_lower,	RR_upper,	
                 odds_ratio,	OR_lower,	OR_upper,	hazard_ratio,	HR_lower,	HR_upper,	sample_per_cat,	deaths_per_cat)]

## HANDLE ODDS RATIO
or <- data[!is.na(odds_ratio)]

## GET TB RESULTS FOR SPLITTING SAMPLE SIZE OF MARKS STUDY
tb <- get_outputs(topic = "cause", measure_id = 5, location_id = 102, sex_id = 3, year_id = 1997:2005, 
                  cause_id = c(934, 946), age_group_id = 22, metric_id = 1, gbd_round_id = 5)
tb <- tb[, .(sample_per_cat = sum(val)), by = .(cause_id)]
tbsum <- sum(tb$sample_per_cat)

## SPLIT RESULTS
ref <- or[odds_ratio == 1]
ref <- ref[, .(file_name, HIV_negative_sample, late, risk, tb_category, sample_per_cat, deaths_per_cat)]
setnames(ref, old = c("tb_category", "sample_per_cat", "deaths_per_cat"), new = c("ref_cat", "ref_sample", "ref_deaths"))

## MERGE REFERENCE
or <- merge(or, ref)
or <- or[odds_ratio != 1]
or[, ref_prop := ref_deaths/ref_sample]
or[, RR_mean := (odds_ratio) / ((1-ref_prop) + (ref_prop*odds_ratio))]

## UNCERTAINTY
or[, se := sqrt((1/ref_deaths)+(1/deaths_per_cat)-(1/ref_sample)-(1/sample_per_cat))]
or[is.infinite(se), se := sqrt((1/ref_deaths)+1-(1/ref_sample)-(1/sample_per_cat))]
or[, `:=` (RR_lower=RR_mean-1.96*se, RR_upper=RR_mean+1.96*se)]
or[, RR_lower := (OR_lower) / ((1-ref_prop) + (ref_prop*OR_lower))]
or[, RR_upper := (OR_upper) / ((1-ref_prop) + (ref_prop*OR_upper))]

#############################################################################################
###                                 PREP HAZARD RATIO DATA                                ###
#############################################################################################

## HANDLE HAZARD RATIO
hr1 <- data[(!is.na(hazard_ratio)) & (!is.na(deaths_per_cat))]
ref <- hr1[hazard_ratio == 1]
ref <- ref[, .(file_name, risk, tb_category, sex, year_start, year_end, age_start, age_end, sample_per_cat, deaths_per_cat)]
setnames(ref, old = c("tb_category", "sample_per_cat", "deaths_per_cat"), new = c("ref_cat", "ref_sample", "ref_deaths"))

## MERGE REFERENCE 
hr1 <- merge(hr1, ref)
hr1 <- hr1[hazard_ratio != 1]
hr1[, ref_prop := ref_deaths/ref_sample]
hr1[, RR_mean := (1-exp(hazard_ratio*log(1-ref_prop)))/ref_prop]

## UNCERTAINTY
hr1[, se := sqrt((1/ref_deaths)+(1/deaths_per_cat)-(1/ref_sample)-(1/sample_per_cat))]
hr1[, `:=` (RR_lower=RR_mean-1.96*se, RR_upper=RR_mean+1.96*se)]
hr1[, RR_lower := (1-exp(HR_lower*log(1-ref_prop)))/ref_prop]
hr1[, RR_upper := (1-exp(HR_upper*log(1-ref_prop)))/ref_prop]

## HANDLE HAZARD RATIO
hr2 <- data[(!is.na(hazard_ratio)) & (!file_name %in% hr1$file_name)]
hr2 <- hr2[hazard_ratio != 1]

hr2[, RR_mean := (1-exp(hazard_ratio*log(1-ref_prop)))/ref_prop]
hr2[, se := (HR_upper - hazard_ratio)/3.92]
hr2[, `:=` (RR_lower = RR_mean-1.96*se, RR_upper = RR_mean+1.96*se)]

#############################################################################################
###                                     GATHER INPUTS                                     ###
#############################################################################################

## GET STUDIES ALREADY WITH RR
rr <- data[!is.na(RR_mean)]
rr <- rr[RR_mean != 1]
rr[, `:=` (sample_per_cat=NULL, deaths_per_cat=NULL)]

## PREP TO APPEND EVERYTHING
or[, `:=` (sample_per_cat=NULL, deaths_per_cat=NULL, ref_cat=NULL, ref_sample=NULL, ref_deaths=NULL, ref_prop=NULL, se=NULL, late=NULL)]
hr1[, `:=`(sample_per_cat=NULL, deaths_per_cat=NULL, ref_cat=NULL, ref_sample=NULL, ref_deaths=NULL, ref_prop=NULL, se=NULL)]
hr2[, `:=`(sample_per_cat=NULL, deaths_per_cat=NULL, ref_prop=NULL, se=NULL)]

## APPEND AND CLEAN
data <- do.call("rbind", list(rr, or, hr1, hr2))
data <- data[order(file_name)]
data <- data[!file_name %like% "Hicks"]

#############################################################################################
###                                 PREP FOR MDR META-ANALYSIS                            ###
#############################################################################################

## SUBSET TO MDR
mdr <- data[risk == "mdr_tb"]
mdr <- mdr[, .SD, .SDcols = -names(mdr)[names(mdr) %like% "odd|OR|hazard|HR"]]

## LOG TRANSFORM MEAN
mdr[, rr := log(RR_mean)]
mdr[, RR_se := (RR_upper-RR_lower)/3.92]

## LOG TRANSFORM SE
mdr$rr_se<- sapply(1:nrow(mdr), function(i) {
  rr_i    <- mdr[i, RR_mean]
  rr_se_i <- mdr[i, RR_se]
  deltamethod(~log(x1), rr_i, rr_se_i^2)
})

## SAVE INPUT
write.csv(mdr, "FILEPATH/mrbrt_input.csv", row.names = F, na = "")

#############################################################################################
###                           RUN MDR META-ANALYSIS IN MR-BRT                             ###
#############################################################################################

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "mdr_v_ds_step4",
  data        = mdr,
  mean_var    = "rr",
  se_var      = "rr_se",
  method      = "trim_maxL",
  study_id    = "file_name",
  trim_pct    = 0.15,
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

## START PREDICTIONS
preds <- data.table(acause = "tb_drug")

## START PREDICTION
pred1 <- predict_mr_brt(fit1, newdata = preds, write_draws = T)

## CHECK FOR PREDICTIONS
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

## GET PREDICTION DRAWS
draws <- unique(as.data.table(pred_object$model_draws))
draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) exp(get(paste0("draw_", x))))]

## COMPUTE MEAN AND CI OF PREDICTION DRAWS
pred_summaries <- copy(draws)
pred_summaries <- draw_summaries(pred_summaries, "pred",  paste0("draw_", 0:999), T)
print(unique(pred_summaries))

## FORMAT DRAWS
preds <- cbind(preds, draws)
preds[, `:=` (X_intercept = NULL, Z_intercept = NULL)]
setnames(preds, old = paste0("draw_", 0:999), new = paste0("rr_", 0:999))

## SAVE DRAWS
fwrite(preds, file = paste0(save_dir, "MDR_pooled_RR_draws_step4.csv"), row.names = F)

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "intercept", dose_vars = "intercept")

#############################################################################################
###                                    FOREST PLOT                                        ###
#############################################################################################

## LOAD IN DATA
mod_data <- as.data.table(fit1$train_data)
mod_data[, file_name := tstrsplit(file_name, ".pdf")[[1]]]

## FIX STUDY NUMBERS
mod_data[, count := .N, by = "file_name"]
mod_data[count == 2 & sex == 1, file_name := paste0(file_name, " male")][count == 2 & sex == 2, file_name := paste0(file_name, " female")]
mod_data[count == 2 & HIV_negative_sample == 1, file_name := paste0(file_name, " no HIV")]
mod_data[count == 2 & file_name == "Jones-Lopez_2011", file_name := paste0(file_name, " with HIV")]
mod_data[file_name == "Lefebvre_2008_European surveillance data" & tb_category %like% "secondary", file_name := paste0(file_name, " secondary")]
mod_data[file_name == "Lefebvre_2008_European surveillance data" & tb_category %like% "primary", file_name := paste0(file_name, " primary")]
mod_data[file_name == "Yen_2013_Taiwan" & tb_category %like% "early",file_name :=  paste0(file_name, " early")]
mod_data[file_name == "Yen_2013_Taiwan" & tb_category %like% "late", file_name := paste0(file_name, " late")]
mod_data[w < 1, file_name := paste0(file_name, ": TRIMMED")]

## MAKE THE PLOT
f <- ggplot(mod_data, aes(ymax = RR_upper, ymin = RR_lower)) + 
  geom_point(aes(y = RR_mean, x = file_name)) + geom_errorbar(aes(x = file_name), width=0) +
  theme_bw() + labs(x = "", y = "Relative Risk") + coord_flip() + ylim(-1,50)+
  ggtitle(paste0("MR-BeRT Meta Analysis - RR: ", round(pred_summaries$pred_mean, 4), " (", round(pred_summaries$pred_lower, 4), " to ", round(pred_summaries$pred_upper, 4), ")")) +
  geom_hline(yintercept = 0) + geom_hline(yintercept = pred_summaries$pred_mean, col = "purple") +
  geom_rect(data = pred_summaries, aes(ymin = pred_lower, ymax = pred_upper, xmin = 0, xmax = length(mod_data$file_name)+1), alpha=0.2, fill="purple")

## PRINT THE FOREST PLOT
print(f)

#############################################################################################
###                                 PREP FOR XDR META-ANALYSIS                            ###
#############################################################################################

## SUBSET TO MDR
xdr <- data[risk == "xdr_tb"]
xdr <- xdr[, .SD, .SDcols = -names(xdr)[names(xdr) %like% "odd|OR|hazard|HR"]]

## LOG TRANSFORM MEAN
xdr[, rr := log(RR_mean)]
xdr[, RR_se := (RR_upper-RR_lower)/3.92]

## LOG TRANSFORM SE
xdr$rr_se<- sapply(1:nrow(xdr), function(i) {
  rr_i    <- xdr[i, RR_mean]
  rr_se_i <- xdr[i, RR_se]
  deltamethod(~log(x1), rr_i, rr_se_i^2)
})

## SAVE INPUT
write.csv(xdr, "FILEPATH/mrbrt_input.csv", row.names = F, na = "")

#############################################################################################
###                           RUN XDR META-ANALYSIS IN MR-BRT                             ###
#############################################################################################

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "xdr_v_mdr_step4",
  data        = xdr,
  mean_var    = "rr",
  se_var      = "rr_se",
  study_id    = "file_name",
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

## START PREDICTIONS
preds <- data.table(acause = "tb_xdr")

## START PREDICTION
pred1 <- predict_mr_brt(fit1, newdata = preds, write_draws = T)

## CHECK FOR PREDICTIONS
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

## GET PREDICTION DRAWS
draws <- as.data.table(pred_object$model_draws)
draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) exp(get(paste0("draw_", x))))]

## COMPUTE MEAN AND CI OF PREDICTION DRAWS
pred_summaries <- copy(draws)
pred_summaries <- draw_summaries(pred_summaries, "pred",  paste0("draw_", 0:999), T)
print(pred_summaries)

## FORMAT DRAWS
preds <- cbind(preds, draws)
preds[, `:=` (X_intercept = NULL, Z_intercept = NULL)]
setnames(preds, old = paste0("draw_", 0:999), new = paste0("rr_", 0:999))

## SAVE DRAWS
fwrite(preds, file = paste0(save_dir, "XDR_pooled_RR_draws_step4.csv"), row.names = F)

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "intercept", dose_vars = "intercept")

#############################################################################################
###                                    FOREST PLOT                                        ###
#############################################################################################

## LOAD IN DATA
mod_data <- as.data.table(fit1$train_data)
mod_data[, file_name := tstrsplit(file_name, ".pdf")[[1]]]
mod_data[w < 1, file_name := paste0(file_name, ": TRIMMED")]

## MAKE THE PLOT
f <- ggplot(mod_data, aes(ymax = RR_upper, ymin = RR_lower)) + 
  geom_point(aes(y = RR_mean, x = file_name)) + geom_errorbar(aes(x = file_name), width=0) +
  theme_bw() + labs(x = "", y = "Relative Risk") + coord_flip() +
  ggtitle(paste0("MR-BeRT Meta Analysis - RR: ", round(pred_summaries$pred_mean, 4), " (", round(pred_summaries$pred_lower, 4), " to ", round(pred_summaries$pred_upper, 4), ")")) +
  geom_hline(yintercept = 0) + geom_hline(yintercept = pred_summaries$pred_mean, col = "purple") +
  geom_rect(data = pred_summaries, aes(ymin = pred_lower, ymax = pred_upper, xmin = 0, xmax = length(mod_data$file_name)+1), alpha=0.2, fill="purple")

## PRINT THE FOREST PLOT
print(f)


