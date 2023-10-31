#Baseline Previous infection immunity

library(tidyverse)
library(reshape2)
library(data.table)

user <- Sys.info()[["user"]]

pinf <- read.csv("FILEPATH/previous_infection_immunity_adjusted.csv")

#keep only "0=inlcude" of "exclude this source from analysis" variable
ve1 <- subset(pinf, exclude == 0)

ve2 <- subset(ve1, select = c(exclude, study_id, author, location_id, location_name, piv, prim_inf_var, variant, symptom_severity, severity, 
                               sample_size, efficacy_mean, efficacy_lower, efficacy_upper, pinf, age_start, age_end, start_interval, end_interval))

#creating a new varaible to have 3 categories of severity: infection, symptomatic and severe
ve2 <- mutate(ve2, sev_severity = if_else(symptom_severity == "Severe", "severe",
                                          if_else(symptom_severity == "severe", "severe",
                                                  if_else(symptom_severity == "Infection", "infection",
                                                          if_else(symptom_severity == "infection", "infection",
                                                                  if_else(symptom_severity == "asymptomatic, Mild, Moderate, Severe", "symptomatic", 
                                                                          if_else(symptom_severity == "Asymptompatic, Mild, Moderate + Severe", "symptomatic",
                                                                                  if_else(symptom_severity == "Mild, Moderate", "symptomatic",
                                                                                          if_else(symptom_severity == "Mild, Moderate + Severe", "symptomatic",
                                                                                                  if_else(symptom_severity == "Moderate, Severe", "symptomatic", "NA"))))))))))
#rename prim_variant_name primary variant name
ve2 <- mutate(ve2, prim_variant_name = if_else(prim_inf_var == "Ancestral", "Ancestral",
                                       if_else(prim_inf_var == "all variant", "Ancestral",          #original: "all variant", "all variant",
                                       if_else(prim_inf_var == "Ancestral, B.1.1.7", "Ancestral",   #original: "Ancestral, B.1.1.7", "mixed",
                                       if_else(prim_inf_var == "mixed variant", "Ancestral",        #original: "mixed variant", "mixed", 
                                       if_else(prim_inf_var == "mixed", "Ancestral",                #original: "mixed", "mixed",
                                       if_else(prim_inf_var == "B.1.1.7", "Ancestral",              #changed to "Ancestral" for this category be considered in the data availability plot
                                       if_else(prim_inf_var == "B.1.351", "Beta", 
                                       if_else(prim_inf_var == "B.1.617.2", "Ancestral",            #changed to "Ancestral" for this category be considered in the data availability plot
                                       if_else(prim_inf_var == "B.1.1.529", "Omicron",
                                       if_else(prim_inf_var == "B.1.1.529.1", "Omicron",            #changed to "Ancestral" for this category be considered in the data availability plot
                                       if_else(prim_inf_var == "B.1.1.529.2", "Omicron", "NA"))))))))))))     #changed to "Ancestral" for this category be considered in the data availability plot

#rename variant (subsequent variant) variable
ve2 <- mutate(ve2, variant_name = if_else(variant == "Ancestral", "Ancestral",
                                  if_else(variant == "all variant", "Ancestral",           #original: "all variant", "all variant",
                                  if_else(variant == "Ancestral, B.1.1.7", "Ancestral",    #original: "Ancestral, B.1.1.7", "mixed",       	
                                  if_else(variant == "B.1.1.7", "Alpha",
                                  if_else(variant == "mixed", "Ancestral",                 #original: "mixed", "mixed",
                                  if_else(variant == "mixed variant", "Ancestral",         #original: "mixed variant", "mixed",
                                  if_else(variant == "B.1.351", "Beta",
                                  if_else(variant == "B.1.617.2", "Delta",
                                  if_else(variant == "B.1.1.529", "Omicron",
                                  if_else(variant == "B.1.1.529.1", "Omicron",
                                  if_else(variant == "B.1.1.529.2", "Omicron", "NA"))))))))))))


#keep only baseline studies
ve2 <- subset(ve2, pinf == 0)

#number of studies and countries
length(unique(ve2$study_id))
length(unique(ve2$location_name))
table(ve2$location_name)
table(ve2$study_id)

#calculate the OR from efficacy_mean
ve2$efficacy_mean <- as.numeric(as.character(ve2$efficacy_mean))
ve2 <- mutate(ve2, or = (1 - efficacy_mean))

#to convert the effectiveness CI in OR standard error 
ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
ve2 <- mutate(ve2, or_lower = (1 - efficacy_upper))
ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
ve2 <- mutate(ve2, or_upper = (1 - efficacy_lower))
ve2 <- mutate(ve2, se = (or_upper - or_lower)/3.92)

#transforming in log space
ve2 <- mutate(ve2, or_log = log(or))
ve2 <- mutate(ve2, se_log = (log(or_upper) - log(or_lower))/3.92)

#Subset - infection, symptomatic, severe
#infec <- subset(ve2, sev_severity == "infection")
#symptomatic <- subset(ve2, sev_severity == "symptomatic")
severe <- subset(ve2, sev_severity == "severe")

#inf <- subset(infec, variant_name == "Ancestral")
#inf <- subset(infec, variant_name == "Alpha")
#inf <- subset(infec, variant_name == "Delta")
#inf <- subset(infec, prim_variant_name == "Ancestral" & variant_name == "Omicron")
#inf <- subset(infec, prim_variant_name == "Omicron" & variant_name == "Omicron")

#symp <- subset(symptomatic, variant_name == "Ancestral")
#symp <- subset(symptomatic, variant_name == "Alpha")
#symp <- subset(symptomatic, variant_name == "Beta")
#symp <- subset(symptomatic, variant_name == "Delta")
#symp <- subset(symptomatic, variant_name == "Omicron")

#sev <- subset(severe, variant_name == "Alpha")
#sev <- subset(severe, variant_name == "Beta")
#sev <- subset(severe, variant_name == "Delta")
sev <- subset(severe, variant_name == "Omicron")

#Run MR-BRT
library(dplyr)
library(mrbrt002, lib.loc = "FILEPATH")
library(data.table)

dt_combined <- data.table()

#inf
#symp
#sev

#1. Fitting a standard mixed effects model
ve6 <- MRData()
ve6$load_df(
  data = sev,  
  col_obs = 'or_log', 
  col_obs_se = 'se_log',
  #col_covs = as.list(new_covs),
  col_study_id = 'study_id' )

mod1 <- MRBRT(
  data = ve6,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE)
    #LinearCovModel(as.list(new_covs) ) 
  ))

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

pred_data <- as.data.table(expand.grid("intercept"=c(1)))

dat_pred1  <- MRData()
dat_pred1 $load_df(
  data = pred_data,
  col_covs = list('intercept')
)

mod1$beta_soln

#3.2.2 â Uncertainty from fixed effects only (using fit-refit)
n_samples2 <- 1000L 
set.seed(1)

samples2_fitrefit <- mod1$sample_soln(sample_size = n_samples2)

draws2_fitrefit <- mod1$create_draws(
  data = dat_pred1,
  beta_samples = samples2_fitrefit[[1]],
  gamma_samples = samples2_fitrefit[[2]],
  random_study = FALSE )

pred_data$pred2 <- mod1$predict(data = dat_pred1)
pred_data$pred2_lo <- apply(draws2_fitrefit, 1, function(x) quantile(x, 0.025)) #, na.rm = T
pred_data$pred2_hi <- apply(draws2_fitrefit, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#summary(pred_data)
#summary
exp(pred_data$pred2)
exp(pred_data$pred2_lo )
exp(pred_data$pred2_hi)

#3.2.3 â Uncertainty from fixed effects and between-study heterogeneity

n_samples3 <- 1000L
set.seed(1)
samples3 <- mod1$sample_soln(sample_size = n_samples3)

draws3 <- mod1$create_draws(
  data = dat_pred1,
  beta_samples = samples3[[1]],
  gamma_samples = samples3[[2]], 
  random_study = TRUE )

pred_data$pred3 <- mod1$predict(dat_pred1)
pred_data$pred3_lo <- apply(draws3, 1, function(x) quantile(x, 0.025)) #, na.rm = T
pred_data$pred3_hi <- apply(draws3, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#summary(pred_data)
#summary
exp(pred_data$pred3)
exp(pred_data$pred3_lo )
exp(pred_data$pred3_hi)

#spreadsheet 
dtfinal <- copy(pred_data)

dtfinal[, pred2_exp:= exp(pred2)]
dtfinal[, pred2_lo_exp:= exp(pred2_lo)]
dtfinal[, pred2_hi_exp:= exp(pred2_hi)]
dtfinal[, pred3_exp:= exp(pred3)]
dtfinal[, pred3_lo_exp:= exp(pred3_lo)]
dtfinal[, pred3_hi_exp:= exp(pred3_hi)]

dt_combined <- rbind(dt_combined, dtfinal)

#Transforming back to VE

#using pred3_exp UIs estimates Uncertainty from fixed effects and between-study heterogeneity
#calculate the VE from OR
ve3 <- mutate(dt_combined, ve = (1 - pred3_exp))
#convert the OR CI to effectiveness CI 
ve3 <- mutate(ve3, ve_lower = (1 - pred3_hi_exp))
ve3 <- mutate(ve3, ve_upper = (1 - pred3_lo_exp))

#Forest plot previous infection_preventing infection_symptomatic_severe disease

#inf
#symp
#sev

gg1 <- fp_dat <- data.frame(
  mean = c(NA, NA, sev$efficacy_mean, NA, ve3$ve),
  lower = c(NA, NA, sev$efficacy_lower, NA, ve3$ve_lower),
  upper = c(NA, NA, sev$efficacy_upper, NA, ve3$ve_upper)
)

fp_text <- cbind(
  c("", "Author", as.character(sev$author), NA, "Mean estimate"),
  c("", "Country", as.character(sev$location_name), NA, "Mean estimate"),
  #c("", "Variant", as.character(sev$variant), NA, "Mean estimate"),
  c("", "Previous infection effectiveness (95% UI)", paste0(
    format(round(sev$efficacy_mean, digits = 2), nsmall = 2), " (",
    format(round(sev$efficacy_lower, digits = 2), nsmall = 2), "-",
    format(round(sev$efficacy_upper, digits = 2), nsmall = 2), ")"
  ), NA, paste0(
    format(round(ve3[1, "ve"], digits = 2), nsmall = 2), " (",
    format(round(ve3[1, "ve_lower"], digits = 2), nsmall = 2), "-",
    format(round(ve3[1, "ve_upper"], digits = 2), nsmall = 2), ")"
  )))

library(forestplot, lib.loc = "FILEPATH")

forestplot(fp_text, 
           fp_dat,# new_page = TRUE,
           is.summary=c(rep(TRUE,2), rep(FALSE, nrow(sev)),TRUE),
           #clip=c(0, 1.2),
           #xlog=FALSE, 
           col=fpColors(box="royalblue",line="darkblue", summary="royalblue"))

print(gg1)

