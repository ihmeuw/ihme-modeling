##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: CROSSWALKING GBD 2019
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}


pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = paste0(j_root, "FILEPATH"))
library(Hmisc, lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS ---------------------------------------------------------------------------
cause_path <- "FILEPATH"
cause_path2 <- "FILEPATH"
cause_name <- "DVB_ENV_"
cause_name2 <- "SEV_ENV_"
covariate_name <- "severity"
bundle_version <- "sex"

dem_dir <- paste0("FILEPATH")
dem_dir2 <- paste0("FILEPATH")
functions_dir <- "FILEPATH"
mrbrt_helper_dir <- paste0("FILEPATH")
mrbrt_dir <- paste0("FILEPATH")
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id=6)
age_dt <- get_age_metadata(12, gbd_round_id = 6)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]


## PULL IN DATA TO DETERMINE SPLIT
get_dem_data <- function(){
  dt1 <- fread(paste0("FILEPATH"))
  dt2 <- fread(paste0("FILEPATH"))
  dt <- rbind.fill(dt1, dt2)
  dt <- as.data.table(dt)
  dt <- dt[!is_outlier == 1 & measure %in% c("prevalence", "incidence")]
  dt <- dt[group_review==1 | is.na(group_review), ]
  return(dt)
}

## PULL IN DATA TO APPLY SPLIT
get_split_data <- function(){
  dt <- fread(paste0(dem_dir, cause_name, bundle_version, ".csv"))
  dt <- dt[group_review==1 | is.na(group_review), ]
  return(dt)
}


# Severity matches - within study ratios -----------------------------------------------------

find_sev_match <- function(dt){
  sev_dt <- copy(dt)
  sev_dt <- sev_dt[sev_code %in% c("SEV", "DVB") & measure %in% c("prevalence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end", "sex",
                  names(sev_dt)[grepl("^cv_", names(sev_dt))])
  sev_dt[, match_n := .N, by = match_vars]
  sev_dt <- sev_dt[match_n == 2]
  keep_vars <- c(match_vars, "sev_code", "mean", "standard_error", "corr_code")
  sev_dt[, id := .GRP, by = match_vars]
  sev_dt <- dplyr::select(sev_dt, keep_vars)
  sev_dt <- data.table::dcast(sev_dt, ... ~ sev_code, value.var = c("mean", "standard_error"), fun.aggregate=mean)
  sev_dt <- sev_dt[!mean_SEV == 0 & !mean_DVB == 0]
  sev_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sev_dt)
}

calc_sev_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, midage := (age_start + age_end)/2]
  ratio_dt[, `:=` (ratio = mean_DVB/mean_SEV,
                   ratio_se = sqrt((mean_DVB^2 / mean_SEV^2) * (standard_error_DVB^2/mean_DVB^2 + standard_error_SEV^2/mean_SEV^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(ratio_dt)
}


graph_mrbrt_spline <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_summaries)
  shapes <- c(15, 16, 18)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg <- ggplot() +
    geom_point(data = data_dt, aes(x = midage, y = log_ratio, color = as.factor(excluded)), size = 3) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    labs(x = "Age", y = "Log Ratio") +
    ggtitle(paste0("Meta-Analysis Results")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_smooth(data = model_dt, aes(x = X_midage, y = Y_mean), color = "black") +
    geom_ribbon(data = model_dt, aes(x = X_midage, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
    scale_color_manual(name = "", values = c("blue", "red"),
                       labels = c("Included", "Trimmed"))
  return(gg)
}


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
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

calculate_cases_fromes <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


# RUN SEVERITY SPLIT -----------------------------------------------------------

dem_sev_dt <- get_dem_data()
dem_split_dt <- get_split_data()

dem_sev_dt <- get_cases_sample_size(dem_sev_dt)
dem_sev_dt <- get_se(dem_sev_dt)
dem_sev_matches <- find_sev_match(dem_sev_dt)

#Remove NaNs and infs
dem_sev_matches <- subset(dem_sev_matches, standard_error_SEV > 0)


# Run MR-BRT
mrbrt_sev_dt <- calc_sev_ratios(dem_sev_matches)

model_name <- paste0(cause_name, "_sevsplit", date)

sev_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  list(cov_info("midage", "X", degree = 3,
                i_knots = paste(mrbrt_sev_dt[, quantile(midage, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
                r_linear = T, l_linear = T)),
  model_label = model_name,
  data = mrbrt_sev_dt,
  overwrite = TRUE,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)


sev_predictions <- predict_mr_brt(sev_model, newdata = data.table(midage = seq(0, 110, by = 5)), write_draws = T)

dt <- dt[, midage := (age_start + age_end)/2]
dt$midage <- round(dt$midage, digits = -1)

#Remove any best corrected rows that also have presenting data rows
nid_bc_p <- dt[,.N,by="nid,diagcode"][,.N,by="nid"][N == 2, "nid"]
dt <- dt[!(nid %in% nid_bc_p$nid & diagcode == "ENV-SEV_DVB-b"),]

model <- sev_model
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sev_code %in% c("SEV", "DVB")]
  tosplit_dt <- tosplit_dt[sev_code == "SEV_DVB"]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  preds <- predict_mr_brt(sev_model, newdata = data.table(midage= seq(0, 100, by = 10), X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  setnames(pred_draws, "X_midage", "midage")
  split_dt <- merge(tosplit_dt, pred_draws, by = "midage", allow.cartesian = T)
  split_dt[, paste0("DVB_", 0:999) := lapply(0:999, function(x) mean*get(paste0("draw_", x))/(get(paste0("draw_", x))+1))]
  split_dt[, paste0("SEV_", 0:999) := lapply(0:999, function(x) mean/(get(paste0("draw_", x))+1))]
  split_dt[, sev_mean := rowMeans(.SD), .SDcols = paste0("SEV_", 0:999)]
  split_dt[, dvb_mean := rowMeans(.SD), .SDcols = paste0("DVB_", 0:999)]
  split_dt[, dvb_standard_error := apply(.SD, 1, sd), .SDcols = paste0("DVB_", 0:999)]
  split_dt[, sev_standard_error := apply(.SD, 1, sd), .SDcols = paste0("SEV_", 0:999)]
  split_dt[, c(draws, paste0("SEV_", 0:999), paste0("DVB_", 0:999)) := NULL]
  sev_dt <- copy(split_dt)
  sev_dt[, `:=` (mean = sev_mean, standard_error = sev_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sev_code = "SEV",
                  note_modeler = paste0(note_modeler, " | sev split with DVB/SEV ratio for SEV envelope: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  sev_dt <- dplyr::select(sev_dt, names(dt))
  sev_dt[mean<0, mean:= 0]
  dvb_dt <- copy(split_dt)
  dvb_dt[, `:=` (mean = dvb_mean, standard_error = dvb_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = NA, uncertainty_type_value = NA, sev_code = "DVB",
                    note_modeler = paste0(note_modeler, " | sev split with DVB/SEV ratio for DVB envelope: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  dvb_dt <- dplyr::select(dvb_dt, names(dt))
  total_dt <- rbindlist(list(nosplit_dt, dvb_dt, sev_dt))

predict_sev <- list(final = total_dt, graph = split_dt)


graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, sev_mean, sev_standard_error, dvb_mean, dvb_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("sev_mean", "dvb_mean"))
  graph_dt_means[variable == "dvb_mean", variable := "DVB"][variable == "sev_mean", variable := "SEV"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("sev_standard_error", "dvb_standard_error"))
  graph_dt_error[variable == "dvb_standard_error", variable := "DVB"][variable == "sev_standard_error", variable := "SEV"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <- subset(graph_dt, value > 0)
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := round(age_end + age_start)/2]
  graph_dt$midage <- floor(graph_dt$midage/5)*5
  ages <- c(10,30,50,70,90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    facet_wrap(~midage) +
    labs(x = "Both Severity Mean", y = " Severity Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Severity Split Means Compared to Both Severity Mean") +
    scale_color_manual(name = "Severity", values = c("SEV" = "midnightblue", "DVB" = "purple")) +
    theme_classic()
  return(gg_sex)
}

pdf(paste0("FILEPATH"))
graph_predictions(predict_sev$graph)
dev.off()

write.csv(total_dt, paste0("FILEPATH"), row.names = F)

