#VE Baseline

library(tidyverse)
library(reshape2)
library(data.table)

basve <- read.csv("FILEPATH/data_tracking2.csv")

ve1 <- subset(basve, select = c(exclude, study_id, study_id2, author, location_id, location_name, vaccine_developer, 
                                variant, symptom_severity, Severity, age_start, age_end, age_mean, sample_size, booster, 
                                efficacy_mean, efficacy_lower, efficacy_upper))

ve2 <- mutate(ve1, sev_severity = if_else(symptom_severity == "Severe", "severe",
                                  if_else(symptom_severity == "severe", "severe",
                                  if_else(symptom_severity == "Infection", "infection",
                                  if_else(symptom_severity == "infection", "infection",
                                  if_else(symptom_severity == "asymptomatic, Mild, Moderate, Severe", "symptomatic", 
                                  if_else(symptom_severity == "Asymptompatic, Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate", "symptomatic",
                                  if_else(symptom_severity == "Mild,Moderate", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Moderate, Severe", "symptomatic", "NA")))))))))))

#subset
ve2 <- subset(ve2, exclude == 0)

#subset of studies only booster or not booster
ve2 <- subset(ve2, booster == 0) #booster == 0 or 1
ve2 <- subset(ve2, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
ve2 <- subset(ve2, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")

#subset Alpha B.1.1.7   Beta B.1.351	  Delta B.1.617.2
#ve2 <- subset(ve2, variant == "B.1.617.2")
ve2 <- subset(ve2, variant == "B.1.1.529" | variant == "B.1.1.529.1" | variant == "B.1.1.529.2")

#run the model using log space 
#calculate the OR from efficacy_mean
#ve2$efficacy_mean <- as.numeric(as.character(ve2$efficacy_mean))
#ve2 <- mutate(ve2, or = (1 - efficacy_mean))

#to convert the effectiveness CI in OR standard error 
#ve2 <- mutate(ve2, or_lower = (1 - efficacy_upper))
#ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
#ve2 <- mutate(ve2, or_upper = (1 - efficacy_lower))
#ve2 <- mutate(ve2, se = (or_upper - or_lower)/3.92)

#transforming in log space
#ve2 <- mutate(ve2, or_log = log(or))
#ve2 <- mutate(ve2, se_log = (log(or_upper) - log(or_lower))/3.92)

#run the model using logit space
#excluding values below 0 because <0 did not work in logit space
ve2$efficacy_mean <- as.numeric(as.character(ve2$efficacy_mean))
ve2 <- ve2[!(ve2$efficacy_mean < 0), ]
ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
ve2 <- mutate(ve2, se = (efficacy_upper - efficacy_lower)/3.92)

#transform to logit
library(crosswalk002, lib.loc = "FILEPATH")
logit <- delta_transform(mean = ve2$efficacy_mean, sd = ve2$se, transformation = "linear_to_logit")

names(logit) <- c("mean_logit", "sd_logit")
vacc_logit <- cbind(ve2, logit)
ve2 <- vacc_logit

#Subset - outcome, variant Alpha B.1.1.7   Beta B.1.351	  Delta B.1.617.2   Omicron B.1.1.529
#vaccine_developer == "Pfizer & BioNTech"  "Moderna" "AstraZeneca" "Johnson & Johnson"

inf <- subset(ve2, sev_severity == "infection" & vaccine_developer == "AstraZeneca")
#sym <- subset(ve2, sev_severity == "symptomatic" & vaccine_developer == "Johnson & Johnson")
#sev <- subset(ve2, sev_severity == "severe" & vaccine_developer == "Johnson & Johnson")

#Run MR-BRT
library(dplyr)
library(mrbrt002, lib.loc = "FILEPATH")
library(data.table)

dt_combined <- data.table()

#1. Fitting a standard mixed effects model
ve6 <- MRData()
ve6$load_df(
  data = inf,  
  col_obs = 'mean_logit',      #mean_logit #or_log
  col_obs_se = 'sd_logit',     #sd_logit   #se_log
  #col_covs = as.list(new_covs),
  col_study_id = 'study_id2' )

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

summary(pred_data)
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

summary(pred_data)
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
#ve1 <- mutate(dt_combined, ve = (1 - pred3_exp))
#convert the OR CI to effectiveness CI 
#ve1 <- mutate(ve1, ve_lower = (1 - pred3_hi_exp))
#ve1 <- mutate(ve1, ve_upper = (1 - pred3_lo_exp))

#convert VE logit to VE ve = (plogis(pred5) # + 0.1
ve1 <- mutate(dt_combined, ve = (plogis(pred3))) 
ve1 <- mutate(ve1, ve_lower = (plogis(pred3_lo))) 
ve1 <- mutate(ve1, ve_upper = (plogis(pred3_hi)))

#Forest plot infection, symptomatic, severe disease
#Subset - outcome, variant Alpha B.1.1.7   Beta B.1.351	  Delta B.1.617.2   Omicron B.1.1.529
#vaccine_developer == "Pfizer & BioNTech"  "Moderna" "AstraZeneca" "Johnson & Johnson"

#inf 
#sym
#sev

fp_dat <- data.frame(
  mean = c(NA, NA, inf$efficacy_mean, NA, ve1$ve),
  lower = c(NA, NA, inf$efficacy_lower, NA, ve1$ve_lower),
  upper = c(NA, NA, inf$efficacy_upper, NA, ve1$ve_upper)
)

fp_text <- cbind(
  c("", "Author", as.character(inf$author), NA, "Mean estimate"),
  c("", "Country", as.character(inf$location_name), NA, NA),
  c("", "Vaccine effectiveness (95% UI)", paste0(
    format(round(inf$efficacy_mean, digits = 2), nsmall = 2), " (",
    format(round(inf$efficacy_lower, digits = 2), nsmall = 2), "-",
    format(round(inf$efficacy_upper, digits = 2), nsmall = 2), ")"
  ), NA, paste0(
    format(round(ve1[1, "ve"], digits = 2), nsmall = 2), " (",
    format(round(ve1[1, "ve_lower"], digits = 2), nsmall = 2), "-",
    format(round(ve1[1, "ve_upper"], digits = 2), nsmall = 2), ")"
  )))

library(forestplot, lib.loc = "FILEPATH")

forestplot(fp_text, 
           fp_dat,# new_page = TRUE,
           is.summary=c(rep(TRUE,2), rep(FALSE, nrow(inf)),TRUE),
           #clip=c(0, 1.2),
           #xlog=FALSE, 
           col=fpColors(box="royalblue",line="darkblue", summary="royalblue"))

