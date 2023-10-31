#booster VE

library(tidyverse)
library(reshape2)
library(data.table)

tsvacc <- read.csv("FILEPATH/tsvacc.csv")

#keep only "0=inlcude" of "exclude this source from analysis" variable
ve1 <- subset(tsvacc, exclude == 0)

ve2 <- subset(ve1, select = c(study_id, author, X1st.dose.only., location_id, location_id2, location_name, location_name2, 
                              vac_before_booster, vaccine_developer, variant, symptom_severity, severity,
                              sample_size, age_start, age_end, efficacy_mean, efficacy_lower, efficacy_upper, booster, 
                              vemt, mean_time_since.vaccination, start_interval, end_interval))

ve2 <- mutate(ve2, sev_severity = if_else(symptom_severity == "Severe", "severe",
                                  if_else(symptom_severity == "severe", "severe",
                                  if_else(symptom_severity == "Infection", "infection",
                                  if_else(symptom_severity == "infection", "infection",
                                  if_else(symptom_severity == "asymptomatic, Mild, Moderate, Severe", "symptomatic", 
                                  if_else(symptom_severity == "Asymptompatic, Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate", "symptomatic",
                                  if_else(symptom_severity == "Mild, Moderate + Severe", "symptomatic",
                                  if_else(symptom_severity == "Moderate, Severe", "symptomatic", "NA"))))))))))

#weeks after second dose
ve2$start_interval <- as.numeric(as.character(ve2$start_interval))
ve2$end_interval <- as.numeric(as.character(ve2$end_interval))
ve2 <- mutate(ve2, mid_point1 = ((end_interval - start_interval)/2) + start_interval)

#combine in "mid_point" tow columns
ve2 <- mutate(ve2, mid_point = if_else(is.na(ve2$mid_point1), as.numeric(ve2$mean_time_since.vaccination), as.numeric(ve2$mid_point1)))

#subset of studies only booster or not booster
ve2 <- subset(ve2, booster == 1) #booster == 0 or 1
ve2 <- subset(ve2, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
ve2 <- subset(ve2, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson")

#Curves for VE Aplha, Delta and Omicron
#ve2 <- subset(ve2, variant == "B.1.617.2")
ve2 <- subset(ve2,variant == "B.1.1.529" | variant == "B.1.1.529.1" | variant == "B.1.1.529.2")

#new variable name to account for differences between studies
ve2$study_id2 = paste(ve2$study_id, ve2$location_name2,  ve2$variant, ve2$severity, ve2$age_start, ve2$age_end,  sep = "&")

#new variable past vac and booster dose
ve2$vaccbooster = paste(ve2$vac_before_booster, ve2$vaccine_developer,  sep = "/Booster: ")

#calculate the OR from efficacy_mean
#ve2$efficacy_mean <- as.numeric(as.character(ve2$efficacy_mean))
#ve2 <- mutate(ve2, or = (1 - efficacy_mean))
#to convert the effectiveness CI in OR standard error 
#ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
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

ve2 <- subset(ve2, sev_severity == "severe")

#transform to logit
library(crosswalk002, lib.loc = "FILEPATH")
logit <- delta_transform(mean = ve2$efficacy_mean, sd = ve2$se, transformation = "linear_to_logit")

names(logit) <- c("mean_logit", "sd_logit")
vacc_logit <- cbind(ve2, logit)
ve2 <- vacc_logit

#meta-regression spline models with separate models by vaccine and outcome

library(dplyr)
library(mrbrt002, lib.loc = "FILEPATH")

#Pfizer - infection, symptomatic, severe
pfiz <- subset(ve2, vac_before_booster == "Pfizer & BioNTech" & sev_severity == "severe"
                  & vaccine_developer == "AstraZeneca") #Moderna #AstraZeneca #Pfizer & BioNTech #Johnson & Johnson
pfiz <- mutate(pfiz, in_var = (1/sqrt(se))/10)

#write.csv(pfiz, "FILEPATH/pfipfi_inf.csv", row.names = F)
#pfipfi_sev <- pfiz
#write.csv(pfipfi_sev, "FILEPATH/pfipfi_sev.csv", row.names = F)
#write.csv(pfiz, "FILEPATH/pfimod_sev.csv", row.names = F)
#write.csv(pfiz, "FILEPATH/pfipfi_symp_o.csv", row.names = F)
#pfipfi_sev_o <- pfiz
#write.csv(pfipfi_sev_o, "FILEPATH/pfipfi_sev_o.csv", row.names = F)
#write.csv(pfiz, "FILEPATH/pfiast_sev_o.csv", row.names = F)

#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = pfiz,  
  col_obs = "mean_logit",      #mean_logit #or_log
  col_obs_se = "sd_logit",     #sd_logit   #se_log
  col_covs = list("mid_point"),
  col_study_id = "study_id2" )

mod1 <- MRBRT(
  data = ve6,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel(
      alt_cov = "mid_point",
      use_spline = TRUE,
      #spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
      #spline_knots = array(c(0, 0.45, 0.90, 1)),             # 1.0 12 24  
      #spline_knots = array(c(0, 0.67, 1)),                    # 1.0 18
      spline_knots = array(seq(0, 1, length.out = 4)),       # 1.0  9.5 18.0 26.5    #(seq(0, 1, by = 0.2)
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = 'decreasing' #increasing #decreasing
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

df_pred3 <- data.frame(mid_point = seq(0, 20, by = 0.1))
dat_pred3 <- MRData()
dat_pred3$load_df(
  data = df_pred3, 
  col_covs=list('mid_point')
)

df_pred3$pred5 <- mod1$predict(data = dat_pred3)

#3.2.3 â Uncertainty from fixed effects and between-study heterogeneity

n_samples3 <- 1000L
set.seed(1)
samples3 <- mod1$sample_soln(sample_size = n_samples3)

draws3 <- mod1$create_draws(
  data = dat_pred3,
  beta_samples = samples3[[1]],
  gamma_samples = samples3[[2]], 
  random_study = TRUE )

df_pred3$pred5 <- mod1$predict(dat_pred3)
df_pred3$pred_lo <- apply(draws3, 1, function(x) quantile(x, 0.025)) #, na.rm = T
df_pred3$pred_hi <- apply(draws3, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#using pred5 UIs estimates Uncertainty from fixed effects and between-study heterogeneity
#calculate the VE from OR log scale
#df_pred3 <- mutate(df_pred3, ve = (1 - exp(pred5)))
#convert the OR CI to effectiveness CI 
#df_pred3 <- mutate(df_pred3, ve_lower = (1 - exp(pred_hi)))
#df_pred3 <- mutate(df_pred3, ve_upper = (1 - exp(pred_lo)))

#convert VE logit to VE ve = (plogis(pred5) # + 0.1
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5))) 
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
pfiz <- mutate(pfiz, insesqua = (1/sqrt(se))/10)

with(pfiz, plot(mid_point, efficacy_mean, xlim = c(0, 20), ylim = c(0, 1),  cex = insesqua)) #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(pfiz$study_id2)

for (grp in groups1) {
  df_tmp <- filter(pfiz, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, efficacy_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#pfipfi_inf_15w <- df_pred3
#write.csv(pfipfi_inf_15w, "FILEPATH/pfipfi_inf_15w.csv", row.names = F)

#pfipfi_sev_15w <- df_pred3
#write.csv(pfipfi_sev_15w, "FILEPATH/pfipfi_sev_15w.csv", row.names = F)

#pfimod_sev_11w <- df_pred3
#write.csv(pfimod_sev_11w, "FILEPATH/pfimod_sev_11w.csv", row.names = F)

#pfipfi_symp_o_17w <- df_pred3
#write.csv(pfipfi_symp_o_, "FILEPATH/pfipfi_symp_o_17w.csv", row.names = F)

#pfipfi_sev_o_20 <- df_pred3
#write.csv(pfipfi_sev_o_20, "FILEPATH/pfipfi_sev_o_20.csv", row.names = F)

#pfiast_sev_o_20 <- df_pred3
#write.csv(pfiast_sev_o_20, "FILEPATH/pfiast_sev_o_20.csv", row.names = F)

#Moderna - infection, symptomatic, severe
mod <- subset(ve2, vac_before_booster == "Moderna" & sev_severity == "severe"
                 & vaccine_developer == "AstraZeneca") #Moderna #AstraZeneca #Pfizer & BioNTech #Johnson & Johnson
mod <- mutate(mod, in_var = (1/sqrt(se))/10)

#write.csv(mod, "FILEPATH/modmodinf.csv", row.names = F) 
write.csv(mod, "FILEPATH/modastsev_o.csv", row.names = F) 

#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = mod,  
  col_obs = "mean_logit",      #mean_logit #or_log
  col_obs_se = "sd_logit",     #sd_logit   #se_log
  col_covs = list("mid_point"),
  col_study_id = "study_id2" )

mod1 <- MRBRT(
  data = ve6,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel(
      alt_cov = "mid_point",
      use_spline = TRUE,
      #spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
      #spline_knots = array(c(0, 0.44, 1)),
      spline_knots = array(seq(0, 1, length.out = 3)),   #(seq(0, 1, by = 0.2) #seq(0, 1, length.out = 4)
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = 'decreasing' #increasing #decreasing
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

df_pred3 <- data.frame(mid_point = seq(0, 13, by = 0.1))
dat_pred3 <- MRData()
dat_pred3$load_df(
  data = df_pred3, 
  col_covs=list('mid_point')
)

df_pred3$pred5 <- mod1$predict(data = dat_pred3)

#3.2.3 â Uncertainty from fixed effects and between-study heterogeneity

n_samples3 <- 1000L
set.seed(1)
samples3 <- mod1$sample_soln(sample_size = n_samples3)

draws3 <- mod1$create_draws(
  data = dat_pred3,
  beta_samples = samples3[[1]],
  gamma_samples = samples3[[2]], 
  random_study = TRUE )

df_pred3$pred5 <- mod1$predict(dat_pred3)
df_pred3$pred_lo <- apply(draws3, 1, function(x) quantile(x, 0.025)) #, na.rm = T
df_pred3$pred_hi <- apply(draws3, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#using pred5 UIs estimates Uncertainty from fixed effects and between-study heterogeneity
#calculate the VE from OR log scale
#df_pred3 <- mutate(df_pred3, ve = (1 - exp(pred5)))
#convert the OR CI to effectiveness CI 
#df_pred3 <- mutate(df_pred3, ve_lower = (1 - exp(pred_hi)))
#df_pred3 <- mutate(df_pred3, ve_upper = (1 - exp(pred_lo)))

#convert VE logit to VE ve = (plogis(pred5) # + 0.1
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5))) 
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
mod <- mutate(mod, insesqua = (1/sqrt(se))/10)

with(mod, plot(mid_point, efficacy_mean, xlim = c(0, 20), ylim = c(0, 1),  cex = insesqua)) # col = study_id2,
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(mod$study_id2)

for (grp in groups1) {
  df_tmp <- filter(mod, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, efficacy_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#modmod_inf_12w <- df_pred3
#write.csv(modmod_inf_12w, "FILEPATH/modmod_inf_12w.csv", row.names = F)

#modastsev_o_13 <- df_pred3
#write.csv(modastsev_o_13, "FILEPATH/modastsev_o_13.csv", row.names = F) 


#Astrazeneca - infection, symptomatic, severe
astra <- subset(ve2, vac_before_booster == "AstraZeneca" & sev_severity == "severe"
                   & vaccine_developer == "Pfizer & BioNTech") #Moderna #AstraZeneca #Pfizer & BioNTech #Johnson & Johnson
astra <- mutate(astra, in_var = (1/sqrt(se))/10)

#write.csv(astra, "FILEPATH/astpfi_sev.csv", row.names = F)
#astpfi_symp_o <- astra
#write.csv(astpfi_symp_o, "FILEPATH/astpfi_symp_o.csv", row.names = F)
#write.csv(astra, "FILEPATH/astast_symp_o.csv", row.names = F)
write.csv(astra, "FILEPATH/astpfi_sev_o.csv", row.names = F)

#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = astra,  
  col_obs = "mean_logit",      #mean_logit #or_log
  col_obs_se = "sd_logit",     #sd_logit   #se_log
  col_covs = list("mid_point"),
  col_study_id = "study_id2" )

mod1 <- MRBRT(
  data = ve6,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel(
      alt_cov = "mid_point",
      use_spline = TRUE,
      #spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
      #spline_knots = array(c(0, 0.44, 1)),
      spline_knots = array(seq(0, 1, length.out = 4)),   #(seq(0, 1, by = 0.2) #seq(0, 1, length.out = 4)
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = 'decreasing' #increasing #decreasing
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

df_pred3 <- data.frame(mid_point = seq(0, 15, by = 0.1))
dat_pred3 <- MRData()
dat_pred3$load_df(
  data = df_pred3, 
  col_covs=list('mid_point')
)

df_pred3$pred5 <- mod1$predict(data = dat_pred3)

#3.2.3 â Uncertainty from fixed effects and between-study heterogeneity

n_samples3 <- 1000L
set.seed(1)
samples3 <- mod1$sample_soln(sample_size = n_samples3)

draws3 <- mod1$create_draws(
  data = dat_pred3,
  beta_samples = samples3[[1]],
  gamma_samples = samples3[[2]], 
  random_study = TRUE )

df_pred3$pred5 <- mod1$predict(dat_pred3)
df_pred3$pred_lo <- apply(draws3, 1, function(x) quantile(x, 0.025)) #, na.rm = T
df_pred3$pred_hi <- apply(draws3, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#using pred5 UIs estimates Uncertainty from fixed effects and between-study heterogeneity
#calculate the VE from OR log scale
#df_pred3 <- mutate(df_pred3, ve = (1 - exp(pred5)))
#convert the OR CI to effectiveness CI 
#df_pred3 <- mutate(df_pred3, ve_lower = (1 - exp(pred_hi)))
#df_pred3 <- mutate(df_pred3, ve_upper = (1 - exp(pred_lo)))

#convert VE logit to VE ve = (plogis(pred5) # + 0.1
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5))) 
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
astra <- mutate(astra, insesqua = (1/sqrt(se))/10)

with(astra, plot(mid_point, efficacy_mean, xlim = c(0, 20), ylim = c(0, 1), cex = insesqua)) #col = study_id2,  
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(astra$study_id2)

for (grp in groups1) {
  df_tmp <- filter(astra, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, efficacy_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#astpfi_sev_15w <- df_pred3
#write.csv(astpfi_sev_15w, "FILEPATH/astpfi_sev_15w.csv", row.names = F)

#astpfi_symp_o_17 <- df_pred3
#write.csv(astpfi_symp_o_17, "FILEPATH/astpfi_symp_o_17.csv", row.names = F)

#astast_symp_o_17w <- df_pred3
#write.csv(astast_symp_o_17w, "FILEPATH/astast_symp_o_17w.csv", row.names = F)

astpfi_sev_o_15w <- df_pred3
write.csv(astpfi_sev_o_15w, "FILEPATH/astpfi_sev_o_15w.csv", row.names = F)


#Johnson & Johnson - infection, symptomatic, severe
jj <- subset(ve2, vac_before_booster == "Johnson & Johnson" & sev_severity == "severe"
                & vaccine_developer == "Johnson & Johnson") #Moderna #AstraZeneca #Pfizer & BioNTech #Johnson & Johnson


