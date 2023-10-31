#Waning VE

library(tidyverse)
library(reshape2)
library(data.table)

# severity type
severity_type <- 'severe'
# variant_name <- "Omicron"
combine_group <- "Non_Omicron"


# output directory
output_dir <- 'FILEPATH'

#tsvacc <- read.csv("/ihme/homes/cstein87/vaceff/tsvacc.csv")
tsvacc <- fread("FILEPATH/time_since_vaccination_2022_07_29.csv")

#keep only "0=inlcude" of "exclude this source from analysis" variable
ve1 <- subset(tsvacc, exclude == 0)

ve2 <- subset(ve1, select = c(study_id, author, first_dose_only, location_id, location_id2, location_name, location_name2, 
                              vac_before_booster, vaccine_developer, variant, symptom_severity, severity,
                              sample_size, age_start, age_end, efficacy_mean, efficacy_lower, efficacy_upper, booster, 
                              vemt, mean_time_since_vaccination, start_interval, end_interval))

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

#combine in "mid_point" two columns
ve2 <- mutate(ve2, mid_point = if_else(is.na(ve2$mid_point1), as.numeric(ve2$mean_time_since_vaccination), as.numeric(ve2$mid_point1)))

#subset of studies only booster or not booster
ve2 <- subset(ve2, booster == 0) #booster == 0 or 1
table(ve2$variant)
# Alpha (B.1.1.7)  Beta(B.1.351)  Delta (B.1.617.2)  and Omicron (B.1.1.529, B.1.1.529.1, B.1.1.529.2) variants
ve2 <- subset(ve2, variant == "B.1.1.7" | variant == "B.1.351" | variant == "B.1.617.2" |variant == "B.1.1.529" | 
                variant == "B.1.1.529.1" | variant == "B.1.1.529.2")
ve2 <- subset(ve2, vaccine_developer == "Pfizer & BioNTech"  | vaccine_developer == "Moderna" | 
                vaccine_developer == "AstraZeneca" | vaccine_developer == "Johnson & Johnson" |
                vaccine_developer == "CoronaVac/SinoVac")

# renaming variants 
ve2 <- mutate(ve2, variant = if_else(variant == "B.1.1.7", "Alpha",
                                                     if_else(variant == "B.1.351", "Beta",
                                                        if_else(variant == "B.1.617.2", "Delta",           #changed to "Ancestral" for this category be considered in the data availability plot
                                                         if_else(variant == "B.1.1.529", "Omicron",
                                                              if_else(variant == "B.1.1.529.1", "Omicron",
                                                               if_else(variant == "B.1.1.529.2", "Omicron", "NA")))))))

# adding column for non omicron and omicron for combine analysis
# renaming variants 
ve2 <- mutate(ve2, group_analysis = if_else(variant == "Alpha", "Non_Omicron",
                                     if_else(variant == "Beta", "Non_Omicron",
                                             if_else(variant == "Delta", "Non_Omicron",           #changed to "Ancestral" for this category be considered in the data availability plot
                                                     if_else(variant == "Omicron", "Omicron",
                                                             if_else(variant == "Omicron", "Omicron",
                                                                     if_else(variant == "Omicron", "Omicron", "NA")))))))


ve2 <- mutate(ve2, vaccine_type = if_else(vaccine_developer == "Pfizer & BioNTech", "mRNA",
                                          if_else(vaccine_developer == "Moderna", "mRNA",
                                                  if_else(vaccine_developer == "Johnson & Johnson", "non_mRNA",
                                                          if_else(vaccine_developer == "AstraZeneca", "non_mRNA",
                                                                  if_else(vaccine_developer == "CoronaVac/SinoVac", "non_mRNA", "NA"))))))


#Curves for VE Aplha (B.1.1.7), Delta and Omicron
# ve2 <- subset(ve2, variant == variant_name)

# filtering out combine analysis group
ve2 <- subset(ve2, group_analysis == combine_group)

# subsetting vaccine type
# ve2 <- ve2[vaccine_type == vax_type]

#ve2 <- subset(ve2, variant == "B.1.1.529" | variant == "B.1.1.529.1" | variant == "B.1.1.529.2")

#new variable name to account for differences between studies
ve2$study_id2 = paste(ve2$study_id, ve2$location_name2,  ve2$variant, ve2$severity, ve2$age_start, ve2$age_end,  sep = "&")

#run the model using log space                                   
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

#excluding values below 0 because <0 did not work in logit 
spaceve2 <- ve2[!(ve2$efficacy_mean < 0), ]

#efficacy_mean - 0.1
ve2 <- mutate(spaceve2, e_mean = efficacy_mean - 0.1)

# subsetting sev_severity dt
ve2 <- ve2[sev_severity == severity_type]

# storing negative e_mean values
negative_e_mean <- ve2[e_mean <= 0]

# manually setting negative values to 0.01 (values in negative_e_mean subset)

# infections non omicron (combine analysis)
# ve2[124,30] <- 0.01
# ve2[213,30] <- 0.01
# ve2[223,30] <- 0.01

# infections omicron (combine analysis)
# ve2[3,30] <- 0.01
# ve2[6,30] <- 0.01
# ve2[10,30] <- 0.01
# ve2[27,30] <- 0.01


# severe non omicron
# NA

# severe omicron
# NA

#infection alpha
# NA

# severe aplha
# NA

# infections beta
# NA

# severe beta
# NA

#infections delta
# ve2[114,28] <- 0.01
# ve2[205,28] <- 0.01
# ve2[215,28] <- 0.01

# infections omi 
# ve2[3,28] <- 0.01
# ve2[6,28] <- 0.01
# ve2[10,28] <- 0.01
# ve2[13,28] <- 0.01
# ve2[14,28] <- 0.01
# ve2[31,28] <- 0.01

# infections omi b11529_2
# NA

# severe delta 
# NA


#run the model using logit space
ve2$e_mean <- as.numeric(as.character(ve2$e_mean))
ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
ve2 <- mutate(ve2, se = (efficacy_upper - efficacy_lower)/3.92)

#transform to logit
library(crosswalk002, lib.loc = "FILEPATH")
logit <- delta_transform(mean = ve2$e_mean, sd = ve2$se, transformation = "linear_to_logit")

names(logit) <- c("mean_logit", "sd_logit")
vacc_logit <- cbind(ve2, logit)
ve2 <- vacc_logit

#meta-regression spline models with separate models by vaccine and outcome

library(dplyr)
library(mrbrt002, lib.loc = "FILEPATH")

#Pfizer - infection, symptomatic, severe
pfiz <- subset(ve2, vaccine_developer == "Pfizer & BioNTech" & sev_severity == severity_type)
pfiz <- mutate(pfiz, in_var = (1/sqrt(se))/10)

#delta outliers infection
#pfiz <- pfiz[!(pfiz$efficacy_mean == 0.060), ]
#pfiz <- pfiz[!(pfiz$efficacy_mean == 0.082), ]

#omicron_infection_outlier ve after second dose Denmark
#pfiz <- pfiz[!(pfiz$efficacy_mean == 0.098), ]

#write.csv(pfiz, "/ihme/homes/cstein87/vaceff/ve_curves/pfizinfec.csv", row.names = F)
#write.csv(pfiz, "/ihme/homes/cstein87/vaceff/ve_curves/pfizsymp.csv", row.names = F)
fwrite(pfiz, file.path(output_dir,"pfizsev_omi.csv")) 
#write.csv(pfiz, "/ihme/homes/cstein87/vaceff/ve_curves/pfizsevec_o.csv", row.names = F) 
#write.csv(pfiz, "/ihme/homes/cstein87/vaceff/ve_curves/pfizsymp_o.csv", row.names = F) 
#write.csv(pfiz, "/ihme/homes/cstein87/vaceff/ve_curves/pfizsev_o.csv", row.names = F)

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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
pfiz <- mutate(pfiz, in_var = (1/sqrt(se))/10)

with(pfiz, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1), cex = in_var)) #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(pfiz$study_id2)

for (grp in groups1) {
  df_tmp <- filter(pfiz, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#pfizer_inf_40w <- df_pred3
#write.csv(pfizer_inf_40w, "/ihme/homes/cstein87/vaceff/ve_curves/pfizer_inf_40w.csv", row.names = F)

#pfizer_symp_43w <- df_pred3
#write.csv(pfizer_symp_43w, "/ihme/homes/cstein87/vaceff/ve_curves/pfizer_symp_43w.csv", row.names = F)

pfizer_sev_104w <- df_pred3
fwrite(pfizer_sev_104w, file.path(output_dir, "pfizer_sev_104w_omi.csv"))

#pfizer_inf_o30w <- df_pred3
#write.csv(pfizer_inf_o30w, "/ihme/homes/cstein87/vaceff/ve_curves/pfizer_inf_o30w.csv", row.names = F)

#pfizer_symp_o_43 <- df_pred3
#write.csv(pfizer_symp_o_43, "/ihme/homes/cstein87/vaceff/ve_curves/pfizer_symp_o_43.csv", row.names = F)

#pfizer_sev_o_34w <- df_pred3
#write.csv(pfizer_sev_o_34w, "/ihme/homes/cstein87/vaceff/ve_curves/pfizer_sev_o_34w.csv", row.names = F)

#CoronaVac/SinoVac - infection, symptomatic, severe
cor_sino <- subset(ve2, vaccine_developer == "CoronaVac/SinoVac" & sev_severity == severity_type)
cor_sino <- mutate(cor_sino, in_var = (1/sqrt(se))/10)

# writing csv
fwrite(cor_sino, file.path(output_dir, "cor_sino_sev_omi.csv"))


#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = cor_sino,  
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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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

#convert VE logit to VE ve = (plogis(pred5) 
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5)))
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
cor_sino <- mutate(cor_sino, in_var = (1/sqrt(se))/10)
cor_sino <- mutate(cor_sino, in_var_recuded = in_var*0.75)

with(cor_sino, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1),   cex = in_var))  #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(cor_sino$study_id2)

for (grp in groups1) {
  df_tmp <- filter(cor_sino, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")


cor_sino_sev_104w <- df_pred3
fwrite(cor_sino_sev_104w, file.path(output_dir, "cor_sino_sev_104w_omi.csv"))


#Moderna - infection, symptomatic, severe
mod <- subset(ve2, vaccine_developer == "Moderna" & sev_severity == severity_type)
mod <- mutate(mod, in_var = (1/sqrt(se))/10)

#write.csv(mod, "/ihme/homes/cstein87/vaceff/ve_curves/modinfec.csv", row.names = F) 
#write.csv(mod, "/ihme/homes/cstein87/vaceff/ve_curves/modsymp.csv", row.names = F) 
fwrite(mod, file.path(output_dir, "modsev_omi.csv"))
#write.csv(mod, "/ihme/homes/cstein87/vaceff/ve_curves/modinfec_o.csv", row.names = F) 
#write.csv(mod, "/ihme/homes/cstein87/vaceff/ve_curves/modsymp_o.csv", row.names = F) 
#write.csv(mod, "/ihme/homes/cstein87/vaceff/ve_curves/modsev_o.csv", row.names = F)

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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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

#convert VE logit to VE ve = (plogis(pred5) 
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5)))
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
mod <- mutate(mod, in_var = (1/sqrt(se))/10)

with(mod, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1),   cex = in_var))  #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(mod$study_id2)

for (grp in groups1) {
  df_tmp <- filter(mod, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#mod_inf_45w <- df_pred3
#write.csv(mod_inf_45w, "/ihme/homes/cstein87/vaceff/ve_curves/mod_inf_45w.csv", row.names = F)

#mod_symp_43w <- df_pred3
#write.csv(mod_symp_43w, "/ihme/homes/cstein87/vaceff/ve_curves/mod_symp_43w.csv", row.names = F)

mod_sev_104w <- df_pred3
fwrite(mod_sev_104w, file.path(output_dir, "mod_sev_104w_omi.csv"))

#mod_inf_o_45w <- df_pred3
#write.csv(mod_inf_o_45w, "/ihme/homes/cstein87/vaceff/ve_curves/mod_inf_o_45w.csv", row.names = F)

#mod_symp_o_43 <- df_pred3
#write.csv(mod_symp_o_43, "/ihme/homes/cstein87/vaceff/ve_curves/mod_symp_o_43.csv", row.names = F)

#Moderna severe omicron 0 data points

#Astrazeneca - infection, symptomatic, severe 
astra <- subset(ve2, vaccine_developer == "AstraZeneca" & sev_severity == severity_type)
astra <- mutate(astra, in_var = (1/sqrt(se))/10)

#write.csv(astra, "/ihme/homes/cstein87/vaceff/ve_curves/astrainfec.csv", row.names = F) 
#write.csv(astra, "/ihme/homes/cstein87/vaceff/ve_curves/astrasymp.csv", row.names = F) 
fwrite(astra, file.path(output_dir, "astrasev_omi.csv"))
#write.csv(astra, "/ihme/homes/cstein87/vaceff/ve_curves/astrainfec_o.csv", row.names = F)
#write.csv(astra, "/ihme/homes/cstein87/vaceff/ve_curves/astrasymp_o.csv", row.names = F) 
#write.csv(astra, "/ihme/homes/cstein87/vaceff/ve_curves/astrasev_o.csv", row.names = F)

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
      #spline_knots = array(seq(0, 1, length.out = 3)),
      spline_knots = array(seq(0, 1, length.out = 4)),   #(seq(0, 1, by = 0.2) #seq(0, 1, length.out = 4)
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = TRUE,
      prior_spline_monotonicity = 'decreasing' #increasing #decreasing
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
astra <- mutate(astra, in_var = (1/sqrt(se))/10)

with(astra, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1),  cex = in_var)) #col = study_id2,
with(df_pred3, lines(mid_point, ve))

# # visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")            

groups1 <- unique(astra$study_id2)

for (grp in groups1) {
  df_tmp <- filter(astra, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#ast_inf_30w <- df_pred3
#write.csv(ast_inf_30w, "/ihme/homes/cstein87/vaceff/ve_curves/ast_inf_30w.csv", row.names = F)

#ast_symp_27w <- df_pred3
#write.csv(ast_symp_27w, "/ihme/homes/cstein87/vaceff/ve_curves/ast_symp_27w.csv", row.names = F)

ast_sev_104w <- df_pred3
fwrite(ast_sev_104w, file.path(output_dir, "ast_sev_104w_omi.csv"))

#AZ symptomatic, only two points from one study

#AZ symptomatic, only 5 points from one study

#ast_sev_o_30 <- df_pred3
#write.csv(ast_sev_o_30, "/ihme/homes/cstein87/vaceff/ve_curves/ast_sev_o_30.csv", row.names = F)

#J&J - infection, symptomatic, severe 
jj <- subset(ve2, vaccine_developer == "Johnson & Johnson" & sev_severity == severity_type)
jj <- mutate(jj, in_var = (1/sqrt(se))/10)

#Omicron severe disease outliers
jj <- jj[!(jj$e_mean == 0.264), ]
jj <- jj[!(jj$e_mean == 0.432), ]

#write.csv(jj, "/ihme/homes/cstein87/vaceff/ve_curves/jjinfec.csv", row.names = F) 
#write.csv(jj, "/ihme/homes/cstein87/vaceff/ve_curves/jjymp.csv", row.names = F) 
fwrite(jj, file.path(output_dir, "jjsev_omi.csv")) 
#write.csv(jj, "/ihme/homes/cstein87/vaceff/ve_curves/jjinfec_o.csv", row.names = F)
#write.csv(jj, "/ihme/homes/cstein87/vaceff/ve_curves/jjymp_o.csv", row.names = F)
#write.csv(jj, "/ihme/homes/cstein87/vaceff/ve_curves/jjsev_o.csv", row.names = F)

#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = jj,  
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
      #spline_knots = array(seq(0, 1, length.out = 3)),
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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
jj <- mutate(jj, in_var = (1/sqrt(se))/10)


with(jj, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1), cex = in_var)) #col = study_id2, 
with(df_pred3, lines(mid_point, ve))

# # visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")            

groups1 <- unique(jj$study_id2)

for (grp in groups1) {
  df_tmp <- filter(jj, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#jj_inf_30w <- df_pred3
#write.csv(jj_inf_30w, "/ihme/homes/cstein87/vaceff/ve_curves/jj_inf_30w.csv", row.names = F)

#JJ symptomatic, only two points from one study

jj_sev_104w <- df_pred3
fwrite(jj_sev_104w, file.path(output_dir, "jj_sev_104w_omi.csv"))

#JJ infection omicron 0 data points

#JJ infection omicron 0 data points

#JJ symptomatic omicron 0 data points

#JJ severe omicron 0 data points

################################################################################
# ADDING mRNA AND NON mRNA CURVES

#mRNA vaxes - infection, symptomatic, severe
mRNA_vaxes <- subset(ve2, vaccine_type == "mRNA" & sev_severity == severity_type)
mRNA_vaxes <- mutate(mRNA_vaxes, in_var = (1/sqrt(se))/10)

# writing csv
fwrite(mRNA_vaxes, file.path(output_dir, "mRNA_vaxes_sev_non_omi.csv"))


#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = mRNA_vaxes,  
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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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

#convert VE logit to VE ve = (plogis(pred5) 
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5)))
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
mRNA_vaxes <- mutate(mRNA_vaxes, in_var = (1/sqrt(se))/10)
mRNA_vaxes <- mutate(mRNA_vaxes, in_var_recuded = in_var*0.75)

with(mRNA_vaxes, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1),   cex = in_var))  #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(mRNA_vaxes$study_id2)

for (grp in groups1) {
  df_tmp <- filter(mRNA_vaxes, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")


mRNA_vaxes_sev_104w <- df_pred3
fwrite(mRNA_vaxes_sev_104w, file.path(output_dir, "mRNA_vaxes_sev_104w_non_omi.csv"))

################################################################################
# Non RNA vaxes
#mRNA vaxes - infection, symptomatic, severe
non_mRNA_vaxes <- subset(ve2, vaccine_type == "non_mRNA" & sev_severity == severity_type)
non_mRNA_vaxes <- mutate(non_mRNA_vaxes, in_var = (1/sqrt(se))/10)

# writing csv
fwrite(non_mRNA_vaxes, file.path(output_dir, "non_mRNA_vaxes_sev_non_omi.csv"))


#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = non_mRNA_vaxes,  
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

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))
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

#convert VE logit to VE ve = (plogis(pred5) 
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5)))
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 

#plot
non_mRNA_vaxes <- mutate(non_mRNA_vaxes, in_var = (1/sqrt(se))/10)
non_mRNA_vaxes <- mutate(non_mRNA_vaxes, in_var_recuded = in_var*0.75)

with(non_mRNA_vaxes, plot(mid_point, e_mean, xlim = c(0, 80), ylim = c(0, 1),   cex = in_var))  #col = study_id2,
with(df_pred3, lines(mid_point, ve))

#with(df_pred3, plot(mid_point, ve, ylim = c(0, 1)))

# visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(non_mRNA_vaxes$study_id2)

for (grp in groups1) {
  df_tmp <- filter(non_mRNA_vaxes, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")


non_mRNA_vaxes_sev_104w <- df_pred3
fwrite(non_mRNA_vaxes_sev_104w, file.path(output_dir, "non_mRNA_vaxes_sev_104w_non_omi.csv"))


# writing data set to output_dir
fwrite(ve2, file.path(output_dir, "waning_vax_eff_dt_omi.csv"))
fwrite(negative_e_mean, file.path(output_dir, "negative_emean_edited_vals_omi.csv"))
