#--------------------------------------------------------------
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate % asymptomatic among mild/moderate cases and among hospital cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  DRIVE <- 'FILEPATH'
  DRIVE <- 'FILEPATH'
} else {
  DRIVE <- 'FILEPATH'
  DRIVE <- 'FILEPATH'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(reticulate) 
library(mrbrt001, lib.loc = 'FILEPATH')
library(plyr)
library(msm, lib.loc = "FILEPATH")

folder <- "FILEPATH"
data <- "FILEPATH"
outputfolder <- "FILEPATH"


version <- 15
save_final <- 1



# graphing function
add_ui <- function(dat, x_var, lo_var, hi_var, color = "darkblue", opacity = 0.2) {
  polygon(
    x = c(dat[, x_var], rev(dat[, x_var])),
    y = c(dat[, lo_var], rev(dat[, hi_var])),
    col = adjustcolor(col = color, alpha.f = opacity), border = FALSE
  )
}


dataset_raw <- read.xlsx(data, colNames=TRUE)
dataset <- data.table(dataset_raw)
dataset <- dataset[sample_size>20 | upper>0]

or_data <- read.xlsx(paste0(folder, "asymptomatic age pattern calculation.xlsx"), colNames=TRUE, sheet='extraction')

################################################################
# DATA PREP
################################################################

table(dataset$sample_characteristics)
table(dataset$sample_age_group)
table(dataset$symptom_cluster)

dataset$hospital <- 0
dataset$hospital[dataset$sample_characteristics=="Hospitalized adults" | dataset$sample_characteristics=="Hospitalized adults and children" | dataset$sample_characteristics=="Hospitalized children"] <- 1

dataset$sample_age_group[dataset$sample_age_group=="?"] <- "all ages"

dataset$older_adults <- 0
dataset$older_adults[dataset$sample_age_group=="older adults" | dataset$sample_age_group=="all adults" | dataset$sample_age_group=="all ages" | dataset$sample_age_group=="adults"] <- 1
dataset$younger_adults <- 0
dataset$younger_adults[dataset$sample_age_group=="adults" | dataset$sample_age_group=="younger adults" | dataset$sample_age_group=="all adults" | dataset$sample_age_group=="all ages" | dataset$sample_age_group=="children and younger adults"] <- 1
dataset$children <- 0
dataset$children[dataset$sample_age_group=="all ages" | dataset$sample_age_group=="children"] <- 1

dataset$older_adults <- 0
dataset$older_adults[dataset$sample_age_group=="older adults"] <- 1
dataset$older_adults[dataset$sample_age_group!="older adults"] <- 0
dataset$younger_adults[dataset$sample_age_group=="younger adults"] <- 1
dataset$younger_adults[dataset$sample_age_group!="younger adults"] <- 0


dataset$exclude <- 0
# version 1
dataset$exclude[dataset$sample_age_group=="all ages ITA village" | dataset$sample_age_group=="blood donors" | dataset$sample_age_group=="healthcare workers"] <- 1
# version 2
dataset$exclude[dataset$hospital==1] <- 1
# version 3
dataset$exclude[dataset$sample_size<10] <- 1
dataset <- dataset[exclude==0]

table(dataset$older_adults, dataset$younger_adults, dataset$children)


df <- dataset[,c("author", "hospital", "older_adults", "younger_adults", "children", "cases", "sample_size", "mean", "standard_error")]

logitvals <- linear_to_logit(mean = array(df$mean), sd = array(df$standard_error))
logitmean <- logitvals[1]
logitse <- logitvals[2]
df$logitmean <- logitmean
df$logitse <- logitse

df$hospital <- 0
df$older_adults <- 0
df$children <- 0

######################################################################################
#   run model with all data with multiple follow up times, to get duration
#      and beta on follow_up_days to use as prior for cluster-specific models
######################################################################################

# set up folder
model_dir <- paste0("v", version, "/")
dir.create(paste0(outputfolder, model_dir))
dir.create(paste0(outputfolder, "plots"))

# set up data
mr_df <- MRData()

cvs <- list("hospital", "older_adults", "younger_adults", "children")
cvs <- list("hospital")

mr_df$load_df(
  data = df, col_obs = "logitmean", col_obs_se = "logitse",
  col_covs = cvs, col_study_id = "author")

model <- MRBRT(
  data = mr_df,
  cov_models =list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("hospital", use_re = FALSE)
  ),
  inlier_pct = 1)


# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

(coeffs <- rbind(model$cov_names, model$beta_soln))
write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs.csv"))

# save model object
py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")

# make predictions for full year
predict_matrix <- data.table(intercept = model$beta_soln[1], hospital=0, children=c(1,0,0), younger_adults=c(0,1,0), older_adults=c(0,0,1))
predict_matrix <- data.table(intercept = model$beta_soln[1], hospital=0)

predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=cvs)

n_samples <- 1000L
samples <- model$sample_soln(sample_size = n_samples)

draws_raw <- model$create_draws(
  data = predict_data,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE,
  sort_by_data_id = TRUE)
# write draws for pipeline
draws_raw <- data.table(draws_raw)
#draws_raw <- draws_raw[, lapply(.SD, exp), .SDcols=paste0('V', c(1:1000))]
draws_raw <- cbind(draws_raw, predict_matrix)
setnames(draws_raw, paste0("V", c(1:1000)), paste0("draw_", c(0:999)))
#draws <- melt(data = draws_raw, id.vars = c("intercept", "hospital", "children", "younger_adults", "older_adults"))
draws <- melt(data = draws_raw, id.vars = c("intercept"))
setnames(draws, "variable", "draw")
setnames(draws, "value", "proportion")
draws$proportion <- exp(draws$proportion) / (1 + exp(draws$proportion))



write.csv(draws, file =paste0(outputfolder, model_dir, "predictions_draws.csv"))







predict_matrix$pred <- model$predict(predict_data, sort_by_data_id = TRUE)
predict_matrix$pred <- exp(predict_matrix$pred) / (1 + exp(predict_matrix$pred))
predict_matrix$pred_lo <- apply(draws_raw, 1, function(x) quantile(x, 0.025))
predict_matrix$pred_lo <- exp(predict_matrix$pred_lo) / (1 + exp(predict_matrix$pred_lo))
predict_matrix$pred_hi <- apply(draws_raw, 1, function(x) quantile(x, 0.975))
predict_matrix$pred_hi <- exp(predict_matrix$pred_hi) / (1 + exp(predict_matrix$pred_hi))
used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
used_data <- as.data.table(used_data)
rr_summaries <- copy(predict_matrix)

rr_summaries
rr_summaries$gamma <- mean(samples[[2]])
write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary.csv"))



used_data$obs_raw <- exp(used_data$obs) / (1 + exp(used_data$obs))
used_data <- used_data[order(obs)]
df <- df[order(logitmean)]
used_data$se <- df$standard_error
used_data$weight <- 1/(15*used_data$se)
rr_summaries$study_id <- " ESTIMATE"
used_data$obs_lo <- used_data$obs-2*used_data$obs_se
used_data$obs_lo <- exp(used_data$obs_lo) / (1 + exp(used_data$obs_lo))
used_data$obs_hi <- used_data$obs+2*used_data$obs_se
used_data$obs_hi <- exp(used_data$obs_hi) / (1 + exp(used_data$obs_hi))



plot <- ggplot(data=rr_summaries, aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi)) +
  geom_pointrange(color="blue") + 
  geom_pointrange(data=used_data, aes(x=study_id, y=obs_raw, ymin=obs_lo, ymax=obs_hi)) + 
  coord_flip() +
  ylab("Proportion") +
  xlab("Study") +
  ggtitle("Proportion asymptomatic among COVID-19 cases") +
  theme_minimal() +
  theme(axis.line=element_line(colour="black")) +
  scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
  guides(fill=FALSE)

plot

ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_prop_asymptomatic.pdf"), width = 6, height = 4)




#########################################################
# AGE SPLIT USING BI ET AL
draws_raw <- draws

or_data$sd <- (or_data$log_upper - or_data$log_lower) / 3.96
draws <- draws_raw
head(draws)
draws <- draws[draw!="hospital"]


or_draws_5 <- rnorm(1000, mean = or_data$log_or[1], sd = or_data$sd[1])
or_draws_15 <- rnorm(1000, mean = or_data$log_or[2], sd = or_data$sd[2])
or_draws_25 <- rnorm(1000, mean = or_data$log_or[3], sd = or_data$sd[3])
or_draws_35 <- rnorm(1000, mean = or_data$log_or[4], sd = or_data$sd[4])
or_draws_45 <- rnorm(1000, mean = or_data$log_or[5], sd = or_data$sd[5])
or_draws_55 <- rnorm(1000, mean = or_data$log_or[6], sd = or_data$sd[6])
or_draws_65 <- rnorm(1000, mean = or_data$log_or[7], sd = or_data$sd[7])
or_draws_80 <- rnorm(1000, mean = or_data$log_or[8], sd = or_data$sd[8])
draws$lnor_5 <- c(or_draws_5)
draws$lnor_15 <- c(or_draws_15)
draws$lnor_25 <- c(or_draws_25)
draws$lnor_35 <- c(or_draws_35)
draws$lnor_45 <- c(or_draws_45)
draws$lnor_55 <- c(or_draws_55)
draws$lnor_65 <- c(or_draws_65)
draws$lnor_80 <- c(or_draws_80)

draws$infections_5 <- 148636616
draws$infections_15 <- 212303251
draws$infections_25 <- 198575121
draws$infections_35 <- 180030736
draws$infections_45 <- 140578953
draws$infections_55 <- 99968673
draws$infections_65 <- 60718822
draws$infections_80 <- 37245990

## proportion asymptomatic by age = 1 - OR_x * propmodsev_55 where propmodsev_55 = (1-meanasymp)*sum(infections)/sum(infections_x * OR_x)
draws <- draws[, propmodsev_55 := (1-proportion) * (infections_5 + infections_15 + infections_25 + infections_35 + infections_45 + infections_55 + infections_65 + infections_80) /
                 (infections_5 * exp(lnor_5) + infections_15 * exp(lnor_15) + infections_25 * exp(lnor_25) + infections_35 * exp(lnor_35) + infections_45 * exp(lnor_45) + 
                    infections_55 * exp(lnor_55) + infections_65 * exp(lnor_65) + infections_80 * exp(lnor_80))]
draws <- draws[, asymp_5 := 1 - exp(lnor_5) * propmodsev_55]
draws <- draws[, asymp_15 := 1 - exp(lnor_15) * propmodsev_55]
draws <- draws[, asymp_25 := 1 - exp(lnor_25) * propmodsev_55]
draws <- draws[, asymp_35 := 1 - exp(lnor_35) * propmodsev_55]
draws <- draws[, asymp_45 := 1 - exp(lnor_45) * propmodsev_55]
draws <- draws[, asymp_55 := 1 - propmodsev_55]
draws <- draws[, asymp_65 := 1 - exp(lnor_65) * propmodsev_55]
draws <- draws[, asymp_80 := 1 - exp(lnor_80) * propmodsev_55]

mean(draws$asymp_5)
mean(draws$asymp_15)
mean(draws$asymp_25)
mean(draws$asymp_35)
mean(draws$asymp_45)
mean(draws$asymp_55)
mean(draws$asymp_65)
mean(draws$asymp_80)

draws$proportion <- NULL
draws$lnor_5 <- NULL
draws$lnor_15 <- NULL
draws$lnor_25 <- NULL
draws$lnor_35 <- NULL
draws$lnor_45 <- NULL
draws$lnor_55 <- NULL
draws$lnor_65 <- NULL
draws$lnor_80 <- NULL
draws$infections_5 <- NULL
draws$infections_15 <- NULL
draws$infections_25 <- NULL
draws$infections_35 <- NULL
draws$infections_45 <- NULL
draws$infections_55 <- NULL
draws$infections_65 <- NULL
draws$infections_80 <- NULL
draws$propmodsev_55 <- NULL
draws$intercept <- NULL

draws$prop_5 <- mean(draws$asymp_5)
draws$sd_5 <- sd(draws$asymp_5)
draws$prop_15 <- mean(draws$asymp_15)
draws$sd_15 <- sd(draws$asymp_15)
draws$prop_25 <- mean(draws$asymp_25)
draws$sd_25 <- sd(draws$asymp_25)
draws$prop_35 <- mean(draws$asymp_35)
draws$sd_35 <- sd(draws$asymp_35)
draws$prop_45 <- mean(draws$asymp_45)
draws$sd_45 <- sd(draws$asymp_45)
draws$prop_55 <- mean(draws$asymp_55)
draws$sd_55 <- sd(draws$asymp_55)
draws$prop_65 <- mean(draws$asymp_65)
draws$sd_65 <- sd(draws$asymp_65)
draws$prop_80 <- mean(draws$asymp_80)
draws$sd_80 <- sd(draws$asymp_80)

draws$asymp_5 <- NULL
draws$asymp_15 <- NULL
draws$asymp_25 <- NULL
draws$asymp_35 <- NULL
draws$asymp_45 <- NULL
draws$asymp_55 <- NULL
draws$asymp_65 <- NULL
draws$asymp_80 <- NULL

draws$draw <- NULL
draws <- unique(draws)

df <- reshape(draws, varying = c("prop_5", "prop_15", "prop_25", "prop_35", "prop_45", "prop_55", "prop_65", "prop_80"), 
              direction = "long", sep = "_", v.names = c("prop"), times = c(5, 15, 25, 35, 45, 55, 65, 80))
df2 <- reshape(draws, varying = c("sd_5", "sd_15", "sd_25", "sd_35", "sd_45", "sd_55", "sd_65", "sd_80"), 
               direction = "long", sep = "_", v.names = c("sd"), times = c(5, 15, 25, 35, 45, 55, 65, 80))
df <- cbind(df[,c("time", "prop")], df2[,"sd"])

setnames(df, "time", "age")
df
df$study_id <- "Bi et al"

logitvals <- linear_to_logit(mean = array(df$prop), sd = array(df$sd))
logitmean <- logitvals[1]
logitse <- logitvals[2]
df$logitmean <- logitmean
df$logitse <- logitse


##############################################
# MR-BRT for age pattern of % asymptomatic

# set up data
mr_df <- MRData()

mr_df$load_df(
  data = df, col_obs = "logitmean", col_obs_se = "logitse",
  col_covs = list("age"), col_study_id = "study_id")

model <- MRBRT(
  data = mr_df,
  cov_models =list(
    LinearCovModel("intercept", use_re = TRUE),
    LinearCovModel("age", use_re = FALSE)
  ),
  inlier_pct = 1)


# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

(coeffs <- rbind(model$cov_names, model$beta_soln))
write.csv(coeffs, paste0(outputfolder, model_dir, "coeffs_age_pattern.csv"))

# save model object
py_save_object(object = model, filename = paste0(outputfolder, model_dir, "mod1_age_pattern.pkl"), pickle = "dill")

# make predictions for full year
source("FILEPATH/get_age_metadata.R")
ages <- get_age_metadata(gbd_round_id = 7)
ages <- ages[, age := (age_group_years_end + age_group_years_start) / 2]
ages <- ages[,c("age_group_id", "age")]
ages <- ages[age>100, age := 97.5]

predict_matrix <- data.table(intercept = model$beta_soln[1], age = ages$age, age_group_id = ages$age_group_id)

predict_data <- MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=list("age"))

n_samples <- 1000L
samples <- model$sample_soln(sample_size = n_samples)

draws_raw <- model$create_draws(
  data = predict_data,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE,
  sort_by_data_id = TRUE)
# write draws for pipeline
draws_raw <- data.table(draws_raw)
draws_raw <- cbind(draws_raw, predict_matrix)
setnames(draws_raw, paste0("V", c(1:1000)), paste0("draw_", c(0:999)))
draws <- melt(data = draws_raw, id.vars = c("intercept", "age_group_id", "age"))
setnames(draws, "variable", "draw")
setnames(draws, "value", "proportion")
draws$proportion <- exp(draws$proportion) / (1 + exp(draws$proportion))



write.csv(draws, file =paste0(outputfolder, model_dir, "predictions_draws_age_pattern.csv"))







predict_matrix$pred <- model$predict(predict_data, sort_by_data_id = TRUE)
predict_matrix$pred <- exp(predict_matrix$pred) / (1 + exp(predict_matrix$pred))
predict_matrix$pred_lo <- apply(draws_raw, 1, function(x) quantile(x, 0.025))
predict_matrix$pred_lo <- exp(predict_matrix$pred_lo) / (1 + exp(predict_matrix$pred_lo))
predict_matrix$pred_hi <- apply(draws_raw, 1, function(x) quantile(x, 0.975))
predict_matrix$pred_hi <- exp(predict_matrix$pred_hi) / (1 + exp(predict_matrix$pred_hi))
used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
used_data <- as.data.table(used_data)
rr_summaries <- copy(predict_matrix)

rr_summaries
rr_summaries$gamma <- mean(samples[[2]])
write.csv(rr_summaries, file =paste0(outputfolder, model_dir, "predictions_summary_age_pattern.csv"))



used_data$obs_raw <- exp(used_data$obs) / (1 + exp(used_data$obs))
used_data <- used_data[order(obs)]
df <- df[order(logitmean)]
used_data$se <- df$sd
used_data$weight <- 1/(used_data$se)
used_data$obs_lo <- used_data$obs-2*used_data$obs_se
used_data$obs_lo <- exp(used_data$obs_lo) / (1 + exp(used_data$obs_lo))
used_data$obs_hi <- used_data$obs+2*used_data$obs_se
used_data$obs_hi <- exp(used_data$obs_hi) / (1 + exp(used_data$obs_hi))





plot <- ggplot(data=rr_summaries, aes(x=age, y=pred), fill = "blue")+
  geom_ribbon(data= rr_summaries, aes(x=age, ymin=pred_lo, ymax=pred_hi),  fill="lightgrey", alpha=.5) +
  geom_line(data=rr_summaries, aes(x=age, y=pred), color = "blue", size=1) +
  ylab("Proportion") +
  xlab("Age") +
  ggtitle("Age pattern of % Asymptomatic") +
  theme_minimal() +
  scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.2), limits=c(0,1)) + 
  scale_x_continuous(expand=c(0,10), breaks = seq(0,100,10)) + 
  theme(axis.line=element_line(colour="black")) +
  geom_point(data=used_data[w ==1,], aes(x=age, y=obs_raw, color="black"), 
             size=(used_data$weight[used_data$w==1]), shape=16, alpha=1) +
  geom_point(data=used_data[w ==0,], aes(x=age, y=obs_raw) , color="dark gray", shape=1, alpha=1) +
  guides(fill=FALSE) +
  theme(legend.title = element_text(size = 8), 
        legend.text = element_text(size = 8)) +
  theme(legend.spacing.x = unit(.2, 'cm'))


plot

ggsave(plot, filename=paste0(outputfolder,"plots/v", version, "_prop_asymptomatic_age_pattern.pdf"), width = 6, height = 4)

















draws$intercept <- NULL
final_draws <- reshape(draws, idvar = c("age_group_id", "age"), timevar = "draw", direction = "wide")
setnames(final_draws, paste0("proportion.draw_", c(0:999)), paste0("draw_", c(0:999)))



head(final_draws)
final_draws$age <- NULL
final_draws[1:5,1:5]
final_draws[1:5,998:1001]

if(save_final==1) {
  write.csv(final_draws, file =paste0(outputfolder, "final_asymptomatic_proportion_draws_by_age.csv"))
}



