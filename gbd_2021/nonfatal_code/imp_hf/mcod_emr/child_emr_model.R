
##
## Author: USER
## Date: 
## 
## Purpose:
##

os <- .Platform$OS.type

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, survival, ggrepel, msm, stats, boot)

library(mrbrt002, lib.loc = "FILEPATH")

###### Paths, args
#################################################################################

central <- "FILEPATH"

args <-commandArgs(trailingOnly = TRUE)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")))

etiology_path <- args[1]
outputDir <- args[2]

etiology_path <- fread(etiology_path)
et <- etiology_path[V1==task_id, V2]

data_path <- args[3]
df <- fread(data_path)

plot <- T

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))

## Function to graph the MRBRT fit
graph_mrbrt_spline <- function(model, title_append="") {
  trimmed_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
  trimmed_data <- as.data.table(trimmed_data)
  trimmed_data[w == 0, excluded := "Trimmed"][w > 0, excluded := "Not trimmed"]      
  p <- ggplot() +
    geom_point(data = trimmed_data, aes(x = age_group_years_start, y = obs, color = as.factor(excluded))) +
    scale_color_manual(values = c("purple", "red")) +
    labs(x = "Age", y = "EMR, Logit", color = "") +
    ggtitle(paste0("Meta-Analysis Results: ", title_append)) +
    theme_bw() +
    theme(text = element_text(size = 17, color = "black")) +
    geom_smooth(data = new_data, aes(x = age_group_years_start, y = logit_value), color = "black", se = F, fullrange = T) 
  print(p)
}


predictions <- data.table()
if (plot) pdf(paste0(outputDir, "modeled_emr_", date, "_", et, ".pdf"), height=8, width=12)


mod <- copy(df[etiology == et,])
mod[, c("deaths", "person_years", "small") := NULL]
mod <- unique(mod)
mod <- melt(mod, measure.vars = c("hazard", "standard_error"))
mod <- data.table::dcast(mod, formula = age_group_years_start + age_group_years_end + etiology ~ variable + demographics )
mod <- mod[!(is.na(hazard_heart_failure)) & !(is.na(hazard_never_heart_failure))]
mod[, id_var := .GRP, by=names(mod)]

## EMR = hazard, HF - hazard, background. Add SEs.
mod[, excess_mortality := hazard_heart_failure - hazard_never_heart_failure]
mod <- mod[excess_mortality >0,]
mod[, se_emr := sqrt(standard_error_heart_failure^2 + standard_error_never_heart_failure^2)]

mod[, logit_emr := log(excess_mortality/(1-excess_mortality))]
mod[, logit_se := sqrt((1/(excess_mortality - excess_mortality^2))^2 * se_emr^2)]


initialized_data <- MRData()
initialized_data$load_df(
  data = mod,  col_obs = "logit_emr", col_obs_se = "logit_se",
  col_covs = list("age_group_years_start"), col_study_id = "id_var" )

model <- MRBRT(
  data = initialized_data,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel(alt_cov = "age_group_years_start",
                   use_spline = TRUE,
                   spline_knots_type = "domain",
                   spline_degree = 4L, 
                   spline_r_linear = TRUE, 
                   spline_l_linear = TRUE)),
  inlier_pct = 0.9)

model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

## Predict
ages <- get_age_metadata(age_group_set_id=VALUE, gbd_round_id = VALUE)
new_data <- data.table(age_group_years_start = ages[age_group_years_start >= 40, age_group_years_start])

prediction_df_initalized <- MRData()

prediction_df_initalized$load_df(
  data = new_data, 
  col_covs=list('age_group_years_start')
)

n_samples <- 1000L
samples <- model$sample_soln(sample_size = n_samples)

draws <- model$create_draws(
  data = prediction_df_initalized,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE)

new_data$logit_value <- model$predict(prediction_df_initalized)
new_data$logit_value_low <- apply(draws, 1, function(x) quantile(x, 0.025))    
new_data$logit_value_high <- apply(draws, 1, function(x) quantile(x, 0.975))
new_data[, etiology := et]

if (plot) graph_mrbrt_spline(model = model, title_append = et)

new_data[, estimate := inv.logit(logit_value)]
new_data[, pred_se_logit := (logit_value_high - logit_value_low)/(2*1.96)]
new_data$pred_se <- sapply(1:nrow(new_data), function(i) {
  ratio_i <- new_data[i, logit_value]
  ratio_se_i <- new_data[i, pred_se_logit]
  sqrt((exp(ratio_i)/ (1 + exp(ratio_i))^2)^2 * ratio_se_i^2)
})


predictions <- rbind(predictions, new_data, fill=T)
  
write.csv(new_data, paste0(outputDir, "predictions_", et, "_", date, ".csv"))


if (plot) dev.off()
