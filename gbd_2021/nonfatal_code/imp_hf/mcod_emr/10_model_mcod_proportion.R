
##
## Author: USER
## Date: DATE
## Purpose: Model the MCOD proportion by age, sex. 
##          This is the proportion of deaths due to an etiology that have heart failure coded with them.
##          Data prep done in 'FILEPATH'
##
## GBD 2020 - updated to GBD 2020 sequela list
##
## source('FILEPATH')

rm(list=ls())
os <- .Platform$OS.type

datetime<-gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, doBy, msm, boot, RMySQL)
library(mrbrt002, lib.loc = "FILEPATH")

###### Paths, args
############################################################################################################

## Central functions
central <- "FILEPATH"

## GBD 2020 etiology list for HF
etiologies <-  c("cvd_ihd", "cvd_rhd", "cvd_htn", "cvd_cmp_myocarditis", "cvd_cmp_other", "cvd_endo", "cvd_other",
                 "resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other",
                 "resp_interstitial", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo_other", "cong_heart",
                 "cvd_cmp_alcoholic", "cvd_valvu_other", "cvd_pah", 
                 "mental_drug_cocaine", 
                 "mental_drug_amphet",
                 "cvd_afib",
                 "ckd",
                 "cirrhosis",
                 "cvd_stroke_cerhem", "cvd_stroke_isch", "cvd_stroke_subhem", "cvd_stroke",
                 "cvd_ihd_chronic", 
                 "cvd_ihd_acute",
                 "cvd_ihd_angina",
                 "endo_thyroid", "endo_hypothyroid", "endo_hyperthyroid") 

hund_attribution <- c("cvd_htn", "cvd_cmp_other", "cvd_cmp_alcoholic")

## ICD codes for HF 
hf_codes <- "I50|I11|^428|^402|^425" # HF, HHD, cardiomyopathies

gbd_round_id <- VALUE
decomp_step <- "VALUE"

write_path <- 'FILEPATH'


###### Functions
############################################################################################################

## Age metadata
source(paste0(central, "get_age_metadata.R"))
age_groups <- get_age_metadata(age_group_set_id = VALUE, gbd_round_id = gbd_round_id)

db_con = fread(paste0(j, "FILEPATH"),
               stringsAsFactors = FALSE)

con <- dbConnect(dbDriver("MySQL"), 
                 username = db_con$username, 
                 password = db_con$pass, 
                 host = db_con$host)

all_age_groups <- as.data.table(dbGetQuery(con, "QUERY"))
dbDisconnect(con)

source(paste0(central, "get_covariate_estimates.R"))
source(paste0(central, "get_location_metadata.R"))

locs <- get_location_metadata(release_id = VALUE, location_set_id = VALUE)

## Helper functions written by USER and USER
source('FILEPATH')


###### Pull in data, HAQI
############################################################################################################
date <- 'DATE'

usa <- fread(paste0(write_path, "usa_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
usa[, location_id := VALUE]
usa <- usa[, .(sum_deaths, sum_hf_deaths, cause_yll_cause, cause_yll_cause_name, sex_id, age_group_id, location_id, year_id)]
usa[, country := "USA"]

twn <- fread(paste0(write_path, "twn_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
twn[, location_id := VALUE]
twn[, country := "TWN"]

bra <- fread(paste0(write_path, "bra_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
bra[, country := "BRA"]

mex <- fread(paste0(write_path, "mex_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
mex[, country := "MEX"]

col <- fread(paste0(write_path, "col_mcod_counts", gbd_round_id, decomp_step, "_", date, ".csv"))
col[, location_id := VALUE]
col[, country := "COL"]


data <- do.call("rbind", list(usa, twn, bra, col, mex, fill=T))
data[, year_id := NULL]
data[, location_id := NULL]
data[, `:=` (sum_deaths=sum(sum_deaths), sum_hf_deaths=sum(sum_hf_deaths)), by=c("cause_yll_cause", "sex_id", "age_group_id", "country")]
data <- unique(data)

collapse <- T
if (collapse) {
  data[cause_yll_cause %in% c("mental_drug_cocaine", "mental_drug_amphet"), cause_yll_cause := "mental_drug_cocaine"]
  data[cause_yll_cause %in% c("hemog_g6pd", "hemog_other", "hemog_thalass"), cause_yll_cause := "hemog_other"]
  data[cause_yll_cause %in% c("resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other", "resp_pneum_silico"), cause_yll_cause := "resp_pneum_other"]
  # Collapse code for cvd_ihd, stroke, and thyroid subtypes
  data[cause_yll_cause %in% c("cvd_ihd_chronic", "cvd_ihd_angina", "cvd_ihd_acute"), cause_yll_cause := "cvd_ihd"]
  data[cause_yll_cause %in% c("cvd_stroke_cerhem", "cvd_stroke_isch", "cvd_stroke_subhem"), cause_yll_cause := "cvd_stroke"]
  data[cause_yll_cause %in% c("endo_thyroid", "endo_hypothyroid", "endo_hyperthyroid"), cause_yll_cause := "endo_thyroid"]
  data[, `:=` (sum_deaths=sum(sum_deaths), sum_hf_deaths=sum(sum_hf_deaths)), by=c("cause_yll_cause", "sex_id", "age_group_id", "country")]
  data <- unique(data)
}

data[, ratio := sum_hf_deaths/sum_deaths]
data <- data[ratio > 0 & ratio < 1,]
data[, logit_ratio := log(ratio/(1-ratio))]
data[, se := sqrt(ratio/sum_deaths)]
data$logit_se <- sapply(1:nrow(data), function(i){
  mean_i <- data[i, ratio]
  se_i <- data[i, se]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
}
)

data <- merge(data, all_age_groups[, .(age_group_id,
                                 age_group_years_start,
                                 age_group_years_end)], by="age_group_id")

# Get rid of missing and assign midpoint to keep ambiguous age data
data <- data[age_group_years_start!=999.]
data[, age_midpoint := (age_group_years_start + age_group_years_end)/2]

# Rename midpoint to "start" to try to avoid naming error
data[,age_group_years_start_ := age_group_years_start]
data[,age_group_years_start := age_midpoint]
data <- data[sex_id %in% c(1, 2)]
data[, male := ifelse(sex_id==1, 1, 0)]


data[, id_var := .GRP, by="country"]

## Make dummy data for prediction purposes
#prediction_df <- expand.grid(age_group_years_start=unique(data$age_group_years_start), male=c(1, 0))
age_groups <- age_groups[,age_midpoint:=(age_group_years_start + age_group_years_end)/2]
prediction_df <- expand.grid(age_group_years_start=unique(age_groups$age_midpoint), male=c(1, 0))
predictions <- data.table()

date<-gsub("-", "_", Sys.Date())
plot <- T

if (plot & collapse){
  pdf(file = paste0('FILEPATH',date,'_collapsed.pdf'), width = 12, height = 8)
} else if (plot & collapse==F){
  pdf(file = paste0('FILEPATH',date,'.pdf'), width = 12, height = 8)
}

for (etiology in unique(data[!(cause_yll_cause %in% hund_attribution), cause_yll_cause])) {
  
  print(etiology)
  df <- copy(data[cause_yll_cause==etiology,])
  name <- unique(data[cause_yll_cause==etiology, cause_yll_cause_name])
  new_data <- data.table(copy(prediction_df))
  
  initialized_data <- MRData()
  initialized_data$load_df(
    data = df,  col_obs = "logit_ratio", col_obs_se = "logit_se",
    col_covs = list("age_group_years_start"), col_study_id = "id_var" )
  
  model <- MRBRT(
    data = initialized_data,
    cov_models = list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel(alt_cov = "age_group_years_start",
                     use_spline = TRUE,
                     spline_knots_type = "frequency",
                     spline_degree = 3L, 
                     spline_r_linear = TRUE, 
                     spline_l_linear = TRUE)),
    inlier_pct = 0.9)
  
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
  
  ## Predict
  
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
  new_data[, et := etiology]

  predictions <- rbind(predictions, new_data, fill=T)
  
  if (plot) {
    
    trimmed_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
    trimmed_data <- as.data.table(trimmed_data)
    trimmed_data[w == 0, excluded := "Trimmed"][w > 0, excluded := "Not trimmed"]      
    
    p <- ggplot() +
      geom_point(data = trimmed_data, aes(x = age_group_years_start, y = obs, color = as.factor(excluded))) +
      scale_color_manual(values = c("midnightblue", "purple")) +
      labs(x = "Age", y = "% Deaths with HF, Logit", color = "") +
      ggtitle(paste0("Meta-Analysis Results: ", name)) +
      theme_bw() +
      theme(text = element_text(size = 17, color = "black")) +
      geom_smooth(data = new_data, aes(x = age_group_years_start, y = logit_value), color = "black", se = F, fullrange = T) 
    print(p)
  }
  
}
if (plot) dev.off()


preds <- copy(predictions)
date<-gsub("-", "_", Sys.Date())
if (collapse){
  write.csv(predictions, file=paste0('FILEPATH', date, '_collapsed.csv'))
} else{
  write.csv(predictions, file=paste0('FILEPATH', date, '.csv'))
}

predictions <- fread(paste0('FILEPATH', date, '_collapsed.csv'))

predictions[, mcod_ratio := inv.logit(logit_value)]
predictions[, pred_se_logit := (logit_value_high - logit_value_low)/3.92]

predictions$pred_se <- sapply(1:nrow(predictions), function(i) {
  ratio_i <- predictions[i, logit_value]
  ratio_se_i <- predictions[i, pred_se_logit]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})

predictions[, sex_id := ifelse(male==1, 1, 2)]

for (hund_et in hund_attribution) {
  print(hund_et)
  df <- as.data.table(copy(new_data))
  df[, mcod_ratio := 1]
  df[, et := hund_et]
  df[, sex_id := ifelse(male==1, 1, 2)]
  df[, `:=`(logit_value=0, logit_value_low=0, logit_value_high=0, pred_se=0)]
  predictions <- rbind(predictions, df, fill=T)
}

predictions[,merge_col:=age_group_years_start]
age_groups[,merge_col := round(age_midpoint, 5)]
predictions[,merge_col := round(merge_col, 5)]
predictions$age_group_years_start <- NULL
predictions <- merge(predictions, age_groups[,.(merge_col, age_group_years_start)], by='merge_col')
predictions$merge_col <- NULL

pred <- predictions[!is.na(age_group_years_start)]

predictions[is.na(sex_id), sex_id := ifelse(male==1, 1, 2)]
#
hemog <- copy(predictions[et == "hemog_other"])[, et := "hemog_g6pd"]
hemog_thalass <- copy(predictions[et == "hemog_other"])[, et := "hemog_thalass"]
asbest <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_asbest"]
coal <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_coal"]
silico <- copy(predictions[et == "resp_pneum_other"])[, et := "resp_pneum_silico"]
chronic <- copy(predictions[et == "cvd_ihd"])[, et := "cvd_ihd_chronic"]
acute <- copy(predictions[et == "cvd_ihd"])[, et := "cvd_ihd_acute"]
angina <- copy(predictions[et == "cvd_ihd"])[, et := "cvd_ihd_angina"]
cerhem <- copy(predictions[et == "cvd_stroke"])[, et := "cvd_stroke_cerhem"]
subhem <- copy(predictions[et == "cvd_stroke"])[, et := "cvd_stroke_subhem"]
isch <- copy(predictions[et == "cvd_stroke"])[, et := "cvd_stroke_isch"]
hypo <- copy(predictions[et == "endo_thyroid"])[, et := "endo_hypothyroid"]
hyper <- copy(predictions[et == "endo_thyroid"])[, et := "endo_hyperthyroid"]




predictions <- do.call("rbind", list(predictions, hemog, hemog_thalass, asbest, coal, silico,
                                     chronic, acute, angina, cerhem, subhem, isch, hypo, hyper))

setnames(predictions, "et", "etiology")

predictions <- predictions[, .(age_group_years_start, etiology, mcod_ratio, pred_se, sex_id)]
predictions <- rbind(predictions, copy(predictions)[etiology == "mental_drug_cocaine"][, etiology := "mental_drug_amphet"])

# Write out files
write.csv(predictions, file=paste0('FILEPATH', date, '.csv'))
  


