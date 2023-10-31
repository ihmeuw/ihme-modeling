#--------------------------------------------------------------
# Name: Carrie Purcell
# Date: 2019-05-23, updated by Alton Lu for GBD 2019, Sarah Wulf Hanson swulf@ for GBD 2020
# Project: IKF RR Evidence Score
# Purpose: prep data for KD data from CKD-PC collaborators for IHD and stroke
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/'
  h_root <- '~/'
} else {
  j_root <- 'J:/'
  h_root <- 'H:/'
}


# set directories
#input_dir<-paste0(j_root,"WORK/12_bundle/ckd/Documentation/gbd_2019/ikf/")
ckd_repo<-"/share/epi/ckd/ckd_code/"
ckd_repo<-"C:/Users/swulf/Desktop/gbd2020/"
#data_dir <- "J:/WORK/12_bundle/ckd/collab_data/CKDPC/GBD2020/"
#mrbrt_output_dir<-paste0(ckd_repo,"kd/mrbrt_model_ouputs/",outcome,"/")
mrbrt_output_dir<-paste0(ckd_repo,"kd/mrbrt_model_ouputs/")
# if(!dir.exists(mrbrt_output_dir)) dir.create(mrbrt_output_dir)
#mrbrt_output_dir<-paste0(j_root,"temp/lualton/")

# source functions
#source(paste0(ckd_repo,"function_lib.R"))
#repo_dir <- "/home/j/temp/reed/prog/projects/run_mr_brt/"
#source(paste0(repo_dir, "mr_brt_functions.R"))

# load packages
library(tidyverse)
library(ggplot2)
library(msm, lib.loc = "/ihme/code/ylds/emr/")
pacman::p_load(data.table, ggplot2, dplyr, stringr, DBI, openxlsx, gtools)

## Modify new data from Joe Coresh

# read data in
#data <- read.csv("~/optum_center_data_impairedkidney.csv")
data <- read.csv(paste0(ckd_repo, "kd/CKD-PC-GBD2019.csv"))

# set the control for blood pressure to 1/0
data$blood_pressure <- ifelse(data$model == "age female diabetes chol hdlc cursmk", 0, 1)
head(data)


# Gathering data to long format. One mean and SE per row
m <- data %>% dplyr::select(-se1, -se2, -se3, -se4) %>% gather(key = "type", value = "log_mean", c("b1", "b2", "b3", "b4"))
e <- data %>% dplyr::select(-b1, -b2, -b3, -b4) %>% gather(key = "type", value = "se", c("se1", "se2", "se3", "se4"))
t <- cbind(m, e$se)

names(t) <- c("outcome", "model", "study", "location_name", "follow_up_years", "follow_up_sd", "loss_to_follow_up", 
              "sample", "event", "mean_age", "blood_pressure", "type", "log_mean", "se")
glimpse(t)
t$cat[t$type == "b1"] <- "albuminuria"
t$cat[t$type == 'b2'] <- "stage3"
t$cat[t$type == 'b3'] <- "stage4"
t$cat[t$type == 'b4'] <- "stage5"

df <- t

# Need relative risk = non_logmean
# 

df$se1 <- (df$upper - df$lower) / 3.92

### basically a loop that goes through each row and calcs the se in log space
# df$delta_log_se <- sapply(1:nrow(df), function(i) {
#   ratio_i <- df[i, "non_logmean"] # relative_risk column
#   ratio_se_i <- df[i, "se1"]
#   deltamethod(~log(x1), ratio_i, ratio_se_i^2)
# })

# df$log_se <- df$delta_log_se

df <- na.omit(df)

### Joining age bin data
#ages <- read.csv("age_year_range_ikf.csv")
ages <- read.csv(paste0(ckd_repo, "kd/age_year_range_kd.csv"))

df <- left_join(df, ages, by = 'study')

table(df$blood_pressure)
#df <- df %>% filter(blood_pressure == 1)

### General explorations to understand the data ----------------------
df %>% group_by(outcome, study) %>%
  summarize(mean = mean(log_mean),
            se = mean(se),
            p25 = quantile(log_mean, .25),
            p75 = quantile(log_mean, .75)) %>%
  group_by(outcome) %>%
  summarize(mean = mean(mean),
            se = mean(se),
            p25 = quantile(mean, .25),
            p75 = quantile(mean, .75))

pdf(paste0(ckd_repo,"kd/diagnostics/raw_data_all.pdf"),width=10, height=7.5)
gg <- df %>%
  ggplot(aes(x = log_mean, y = study, color=mean_age)) +
  geom_point()
print(gg)

df$hazard_ratio <- exp(df$log_mean)
gg <- df %>%
  ggplot(aes(x = hazard_ratio, y = study, color=mean_age)) +
  geom_point()
print(gg)

gg <- df[df$cat=="albuminuria",] %>%
  ggplot(aes(x = log_mean, y = study, color=mean_age)) +
  geom_point() + ggtitle("Albuminuria") +
  xlab("log(hazard ratio)")
print(gg)

gg <- df[df$cat=="stage3",] %>%
  ggplot(aes(x = log_mean, y = study, color=mean_age)) +
  geom_point() + ggtitle("CKD Stage 3") +
  xlab("log(hazard ratio)")
print(gg)

gg <- df[df$cat=="stage4",] %>%
  ggplot(aes(x = log_mean, y = study, color=mean_age)) +
  geom_point() + ggtitle("CKD Stage 4") +
  xlab("log(hazard ratio)")
print(gg)

gg <- df[df$cat=="stage5",] %>%
  ggplot(aes(x = log_mean, y = study, color=mean_age)) +
  geom_point() + ggtitle("CKD Stage 5") +
  xlab("log(hazard ratio)")
print(gg)
dev.off()

#### ----------------------

df$type <- NULL
df$nid <- NA
df$underlying_nid <- NA
df <- df %>% rename("risk" = "cat")
df$risk_mapping <- NA
df$location_name <- ""
df$location_id <- NA
df <- df %>% rename("year_start_study" = "year_start")
df <- df %>% rename("year_end_study" = "year_end")
df <- df %>% rename("age_mean" = "mean_age")
df$age_sd <- NA
df$sex <- "Both"
df <- df %>% rename("sample_size" = "sample")
df <- df %>% rename("events" = "event")
df <- df %>% rename("study_name" = "study")
df$design <- "Cohort"
df$effect_size <- NA
df$lower <- NA
df$upper <- NA
df$cov_representativeness <- 0
df$cov_exposure_quality <- 0
df$cov_outcome_quality <- 0
# model controls for age, sex, hypertension (antihypertensio nmeds, SBP), cholesterol, glucose levels (hd1c), current smoker
df$cov_confounder_quality <- 0
df$cov_reverse_causation <- 0
df$cov_selection_bias <- 0
df$cov_selection_bias[df$loss_to_follow_up>=0.85 & df$loss_to_follow_up<=0.95] <- 1
df$cov_selection_bias[df$loss_to_follow_up<0.85] <- 2
df$notes_exposure_def <- "classified according to albuminuria or GFR cutoff"
df$notes_outcome_def <- ""
df$notes_general <- ""



# write.csv(df, paste0(ckd_repo,"kd/prepped_data/kd_prepped_data_all.csv"), row.names = FALSE)






