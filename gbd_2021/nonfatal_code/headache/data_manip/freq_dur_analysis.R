###########################################################
### Project: Frequency Duration Headache Analysis
### Purpose: GBD 2019 Nonfatal Analysis
###########################################################

## SET-UP
rm(list=ls())

pacman::p_load(data.table, plyr)
library("openxlsx", lib.loc = paste0("FILEPATH"))
library("metafor", lib.loc = paste0("FILEPATH")

## SET OBJECTS
mig_acause <- "neuro_migraine"
tth_bundle <- "neuro_tensache"
mig_bundle <- 1385
tth_bundle <- 1352
draws <- paste0("draw_", 0:999)
freq_draws <- paste0("draw_", 0:999, "_Frequency")
dur_draws <- paste0("draw_", 0:999, "_Duration")
date <- Sys.Date()
date <- gsub("-", "_", date)
step <- "step2"

## SOURCE FUNCTIONS
source(paste0("FILEPATH", "get_bundle_data.R"))

## USER FUNCTIONS
scale_inputs <- function(dt){
  by_vars <- c("nid", "acause", "parameter", "sex", "type")
  dt[measure == "proportion" & is.na(cases), cases := value_prop * sample_size / 100] 
  missing_map <- dt[parameter_scale == "missing", .(nid, acause, parameter, sex, type, cases, sample_size)]
  missing_map[, new_sample_size := sample_size - cases]
  missing_map[, c("cases", "sample_size") := NULL]
  dt <- dt[!parameter_scale == "missing"]
  dt <- merge(dt, missing_map, by = by_vars, all.x = T)
  dt[!is.na(new_sample_size), sample_size := new_sample_size]
  dt[, new_sample_size := NULL]
  ## GET CASES TO ADD UP
  dt[measure == "proportion", case_total := sum(cases), by = by_vars]
  dt[measure == "proportion" & !is.na(case_total), case_mult := sample_size/case_total]
  dt[measure == "proportion" & !is.na(case_total), cases := cases * case_mult]
  dt[, c("value_prop", "case_total", "case_mult") := NULL]
  dt[measure == "continuous", standard_error := standard_dev / sqrt(sample_size)]
  return(dt)
}

beta_dis <- function(dt){
  by_vars <- c("nid", "acause", "parameter", "sex", "type")
  dt[measure == "proportion", cat_mean := rowMeans(.SD), .SDcols = c("cat_start", "cat_end")]
  dt[, non_cases := sample_size - cases]
  beta_rows <- copy(dt)
  beta_rows <- beta_rows[measure == "proportion"]
  beta_rows[, row_count := 1:.N]
  beta_cols <- data.table()
  for (x in unique(beta_rows$row_count)){
    row <- beta_rows[row_count == x]
    beta_draws <- as.data.table(rbeta(n = 1000, shape1 = row$cases, shape2 = row$non_cases, ncp = 0))
    beta_draws[, row_count := x]
    beta_draws[, draw := paste0("draw_", 0:999)]
    beta_draws <- dcast(beta_draws, row_count ~ draw, value.var = "V1")
    beta_cols <- rbind(beta_cols, beta_draws)
  }
  beta_rows <- merge(beta_rows, beta_cols, by = "row_count")
  beta_rows[, (draws) := lapply(.SD, function(x) x * cat_mean), .SDcols = draws]
  beta_rows[, (draws) := lapply(.SD, function(x) sum(x)), by = by_vars, .SDcols = draws]
  beta_rows[, c("row_count", "non_cases") := NULL]
  return(beta_rows)
}

normal_dis <- function(dt){
  contin_rows <- copy(dt)
  contin_rows <- contin_rows[measure == "continuous"]
  contin_rows[, row_count := 1:.N]
  contin_cols <- data.table()
  for (x in unique(contin_rows$row_count)){
    row <- copy(contin_rows)
    row <- contin_rows[row_count == x]
    contin_draws <- as.data.table(rnorm(n = 1000, mean = row$value_mean, sd = row$standard_error))
    contin_draws[, row_count := x]
    contin_draws[, draw := paste0("draw_", 0:999)]
    contin_draws <- dcast(contin_draws, row_count ~ draw, value.var = "V1")
    contin_cols <- rbind(contin_cols, contin_draws)
  }
  contin_rows <- merge(contin_rows, contin_cols, by = "row_count")
  contin_rows[, c("row_count", "non_cases") := NULL]
  return(contin_rows)
}

reformat <- function(dt){
  by_vars <- c("nid", "acause", "type", "sex", "parameter")
  sum_vars <- c("nid", "acause", "type", "sex")
  unique_data <- unique(dt, by = by_vars)
  long_dt <- melt(unique_data, measure.vars = draws, variable.name = "draw")
  long_dt <- dcast(long_dt, draw + nid + acause + type + sex + kids + assume ~ parameter, value.var = "value")
  dt <- copy(long_dt)
  dt <- as.data.table(dt)
  dt[, mean_dur := mean(Duration), by = sum_vars]
  dt[, sd_dur := sd(Duration), by = sum_vars]
  dt[, mean_freq := mean(Frequency), by = sum_vars]
  dt[, sd_freq := sd(Frequency), by = sum_vars]
  dt <- unique(dt, by = sum_vars)
  dt <- dt[, c("Frequency", "Duration", "draw") := NULL]
  return(dt)
}

meta_analysis <- function(name){
  dt <- get(name)
  freq_data <- copy(dt[!is.na(mean_freq)])
  dur_data <- copy(dt[!is.na(mean_dur)])
  if (nrow(freq_data)>0 & nrow(dur_data)>0){
    meta_freq <- rma(yi = freq_data$mean_freq, sei = freq_data$sd_freq)
    meta_dur <- rma(yi = dur_data$mean_dur, sei = dur_data$sd_dur)
    meta_freq <- rma(yi = freq_data$mean_freq, sei = freq_data$sd_freq)
    meta_dur <- rma(yi = dur_data$mean_dur, sei = dur_data$sd_dur)
    pdf(paste0("FILEPATH", name, ".pdf"))
    forest(meta_freq, slab = paste0(freq_data$nid, " ", freq_data$sex), showweights = T, xlab = "Frequency")
    forest(meta_dur, slab = paste0(dur_data$nid, " ", dur_data$sex), showweights = T, xlab = "Duration")
    dev.off()
    results <- data.table(type = name, freq_mean = as.numeric(meta_freq$beta),
                          freq_se = as.numeric(meta_freq$se), freq_n = nrow(freq_data), 
                          dur_mean = as.numeric(meta_dur$beta), dur_se = as.numeric(meta_dur$se),
                          dur_n = nrow(dur_data))
  } else {
    results <- data.table(type = name, freq_mean = NA, freq_se = NA, freq_n = nrow(freq_data),
                          dur_mean = NA, dur_se = NA, dur_n = nrow(dur_data))
  }
  return(results)
}

calculate_time <- function(dt){
  results <- copy(dt)
  results[!is.na(freq_mean), time_ictal := (freq_mean * dur_mean/24)/365.25]
  results[!is.na(freq_mean), dur_se2 := (dur_se/24)/365.25]
  results[!is.na(freq_mean), dur_mean2 := (dur_mean/24)/365.25]
  results[!is.na(freq_mean), time_ictal_se := sqrt(freq_se^2 * dur_se2^2 + freq_se^2 * dur_mean2^2 + freq_mean^2 * dur_se2^2)]
  results[, c("dur_mean2", "dur_se2") := NULL]
  return(results)
}

## SET UP ANALYSIS
in_data <- get_bundle_data(bundle_id = 1385, decomp_step = step, export = F)
in_data <- as.data.table(read_xlsx(data_in_path))
in_data[parameter == "Duration ", parameter := "Duration"]
dt <- scale_inputs(in_data)
dt <- dt[!is_outlier == 1] ## exclude outliered studies #outlier_type_id
prop_draws <- beta_dis(dt)
contin_draws <- normal_dis(dt)
all_draws <- rbind(prop_draws, contin_draws)
formatted <- reformat(all_draws)

## DEFINE SUBGROUPS
no_kids <- copy(formatted)
all_migraine <- no_kids[acause == "neuro_migraine"]
all_tth <- no_kids[acause == "neuro_tensache"]
migraine_f <- all_migraine[sex == "Female"]
migraine_m <- all_migraine[sex == "Male"]
tth_f <- all_tth[sex == "Female"]
tth_m <- all_tth[sex == "Male"]
both_migraine <- all_migraine[type == "definite,probable"]
definite_migraine <- all_migraine[type == "definite"]
probable_migraine <- all_migraine[type == "probable"]
both_tth <- all_tth[type == "definite,probable"]
definite_tth <- all_tth[type == "definite"]
probable_tth <- all_tth[type == "probable"]
moh <- no_kids[acause == "neuro_medache"]
groups <- c("all_migraine", "all_tth", "migraine_f", "migraine_m", "tth_f", "tth_m", "both_migraine", 
            "definite_migraine", "probable_migraine", "both_tth", "definite_tth", "probable_tth", "moh")

## RUN META ANALYSIS AND CALCULATE
results <- rbindlist(lapply(groups, meta_analysis))
results <- calculate_time(results)
write.csv(results, "FILEPATH", row.names = F)

############################# DECOMP 2 ONWARD
#############################
mr_brt_dir <- "FILEPATH"
source(paste0(mr_brt_dir, "cov_info_function.R"))
source(paste0(mr_brt_dir, "run_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_outputs_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_outputs_function.R"))
source(paste0(mr_brt_dir, "predict_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_preds_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_preds_function.R"))
source(paste0(mr_brt_dir, "plot_mr_brt_function.R"))

data <- read_xlsx(paste0("FILEPATH")
data <- as.data.table(data)
for (i in unique(data$type)) {
  data[, paste(i) := ifelse(type == paste(i), 1, 0)]
}
varyingcomps <- c(unique(data$type))
cov_list <- lapply(varyingcomps, function(x) cov_info(x, "X"))
fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "sympt_analysis",
  data = data,
  mean_var = "time_ictal",
  se_var = "time_ictal_se",
  covs = cov_list,
  remove_x_intercept = T,
  overwrite_previous = TRUE
)

results1 <- load_mr_brt_outputs(fit1)
results <- as.data.table(results1$model_coefs)
setnames(results, c("x_cov", "beta_soln"), c("type", "time_ictal"))
results[, time_ictal_se := sqrt(beta_var)]
save <- results[, c("type", "time_ictal", "time_ictal_se")]
write.csv(save, "FILEPATH", row.names = F)

tmp_orig <- copy(data)
tmp_orig <- as.data.frame(tmp_orig)
df_pred <- as.data.frame(tmp_orig[, varyingcomps]) 
pred1 <- predict_mr_brt(fit1, newdata = df_pred, write_draws = T)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

tmp_preds <- preds %>%
  dplyr::mutate(
    pred = Y_mean,
    pred_se = (Y_mean_hi - Y_mean_lo) / 3.92  ) %>%
  select(pred, pred_se)


summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

graph_combos <- function(model, predictions){
  data <- as.data.table(model$train_data)
  data[, headache_type := type]
  data[, `:=` (time_l = time_ictal - 1.96*time_ictal_se, time_u = time_ictal + 1.96*time_ictal_se)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("headache_type", paste(varyingcomps)), with = F]), by = varyingcomps, allow.cartesian = T)
  fit_graph <- function(n){
    ratio_title <- unique(preds$headache_type)[n]
    ratio_mean <- unique(preds[headache_type == ratio_title, mean])
    ratio_lower <- unique(preds[headache_type == ratio_title, lower])
    ratio_upper <- unique(preds[headache_type == ratio_title, upper])
    graph_dt <- copy(data[headache_type == (ratio_title)])
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = ratio_lower, xmax = ratio_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = time_ictal, y = country, color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(x = time_ictal, y = country, xmin = time_l, xmax = time_u, color = as.factor(w))) +
      geom_vline(xintercept = ratio_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Log Effect Size", y = "") +
      xlim(-0.02, 0.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      ggtitle(paste0("Model fit for ratio ", ratio_title)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(headache_type)]), fit_graph)
  return(fit_graphs)
}

graphs_modelfit <- graph_combos(model = fit1, predictions = pred1)

pdf("FILEPATH", width = 12))
graphs_modelfit
dev.off()
