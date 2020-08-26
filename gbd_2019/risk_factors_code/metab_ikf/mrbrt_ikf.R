#--------------------------------------------------------------
# Name: USERNAME
# Date: 2019-05-23
# Project: IKF RR Evidence Score
# Purpose: Run MRBRT for each IKF exposure category - outcome pair
# TODO: make prediction step generalizabe depedendant on covs
#       included in the model
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())


# set directories
input_dir<-paste0(j_root,"FILEPATH")
mrbrt_output_dir<-paste0(ckd_repo,"FILEPATH",outcome,"/")
# if(!dir.exists(mrbrt_output_dir)) dir.create(mrbrt_output_dir)
mrbrt_output_dir<-paste0(j_root,"FILEPATH")

# source functions
ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "mr_brt_functions.R"))
library(tidyverse)

# load packages
require(pacman)
library(tidyverse)
library(ggplot2)
pacman::p_load(data.table, ggplot2, dplyr, stringr, DBI, openxlsx, gtools)

p_load(data.table,openxlsx,ggplot2)


## Relative Risk data
data <- read.csv("FILEPATH/relative_risk_data.csv")
ages <- read.csv("age_year_range_ikf.csv")
data$blood_pressure <- ifelse(data$model == "age female diabetes chol hdlc cursmk", 0, 1)
glimpse(data)


# Gathering data to long format. One mean and SE per row
m <- data %>% dplyr::select(-se1, -se2, -se3, -se4) %>% gather(key = "type", value = "log_mean", c("b1", "b2", "b3", "b4"))
e <- data %>% dplyr::select(-b1, -b2, -b3, -b4) %>% gather(key = "type", value = "se", c("se1", "se2", "se3", "se4"))
t <- cbind(m, e$se)

names(t) <- c("outcome", "model", "study", "sample", "event", "mean_age", "blood_pressure", "type", "log_mean", "se")
t[t$type == "b1",] <- "albuminuria"
t[t$type == 'b2',] <- "stage3"
t[t$type == 'b3',] <- "stage4"
t[t$type == 'b4',] <- "stage5"

df <- t
# write.csv(df, "rr_optum.csv", row.names = FALSE) #save modified file for future use

df$delta_log_se <- sapply(1:nrow(df), function(i) {
  ratio_i <- df[i, "non_logmean"]
  ratio_se_i <- df[i, "se1"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

df <- left_join(df, ages, by = 'study')
# write.csv(df, "relative_risks_upload.csv")

### General Stats about the data ----------------------
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


#### ----------------------
table(df$blood_pressure)
df <- df %>% filter(blood_pressure == 1)

df1 <- filter(df, outcome == "chdh")
df2 <- filter(df, outcome == 'str')

write.csv(df1, "ikf_chdh.csv", row.names = FALSE)
write.csv(df2, "ikf_str.csv", row.names = FALSE)

### Continue -------

types <- unique(df$type)

xcovs <- cov_info(covariate = "mean_age",
                  design_matrix = "X",
                  knot_placement_procedure = "frequency",
                  degree = 3,
                  n_i_knots = 2,
                  bspline_gprior_mean = "0, 0, 0",
                  bspline_gprior_var = "inf, inf, inf",
                  bspline_mono = "decreasing",
                  l_linear = T,
                  r_linear = T)

for (i in types) {
  print(paste0(i, "_rr"))

  # outcome <- "ihd"
  risk <- "str"
  blood <- 1

  output_directory <- paste0(i, risk, "paf_", blood, "_11152019")
  print(output_directory)

  print("Subset")
  df_sub <- df %>% filter(blood_pressure == blood, type == i, outcome == risk) %>%
    dplyr::select(outcome, model, study, type, sample, event, mean_age, log_mean, se,
                  non_logmean, log_se)

  print("Run MrBRT")
  assign(paste0(i, "rr"),
       run_mr_brt(
         output_dir = "/ihme/homes/lualton",
         model_label = output_directory, #ihd_paf, str_paf, ihd_paf_blood, str_paf_blood
         data = df_sub,
         mean_var = "log_mean",
         se_var = "se",
         overwrite_previous = TRUE,
         remove_x_intercept = FALSE,
         method = "trim_maxL",
         study_id = "study",
         trim_pct = .1,
         covs = list(xcovs)
       ))
}


mrbrt_mods <- list(b1rr, b2rr, b3rr, b4rr)

# create predictions
pred_data_x <- data.table(mean_age=seq(40,90,1))
mrbrt_preds <- lapply(mrbrt_mods, predict_mr_brt, newdata=pred_data_x, write_draws = TRUE)

# load (old) --------------------------------------------------------------------

# read in data
outdt <- as.data.table(read.xlsx(paste0(input_dir,outcome,"_rr.xlsx")))

# create vector of cohort-level covariates
cvs <- grep("cv_", names(outdt),value = T)

# drop any covariates that don't have any variation (i.e. are all 1 or all 0)
# drop any dummies that only have a value of 0 - these shouldn't vary by
# expsoure category
for (cv in cvs){
    min_cv<-min(outdt[,get(cv)])
    max_cv<-max(outdt[,get(cv)])
    if (min_cv==max_cv){
      outdt[,(cv):=NULL]
      print(paste0("dropped ", cv, " for becasue there were no observations"))
  }
}

# remove all age RRs
outdt<-outdt[all_age==0]

# make a list of all remaining z covariates
zcovs<-lapply(cvs[cvs%in%names(outdt)], function(x) cov_info(x, "Z"))


exp_cats<-unique(outdt[,exp_cat])
exp_cats <- exp_cats[1]

for (ecat in exp_cats){
  assign(paste0(outcome,"_",ecat),
         run_mr_brt(
           output_dir = mrbrt_output_dir,
           model_label = paste0(model_lab, "_", ecat, "_degree", spline_deg),
           data = outdt[exp_cat==ecat],
           mean_var = "log_rr",
           se_var = "log_rr_se",
           covs = c(xcovs,zcovs),
           study_id = "underlying_nid",
           method="trim_maxL",
           trim_pct = 0.05,
           overwrite_previous = TRUE))
}

# saveRDS(pad_alb, paste0(mrbrt_output_dir, model_lab,"_alb_degree", spline_deg, "/model_output.RDS"))
# saveRDS(pad_stage3, paste0(mrbrt_output_dir, model_lab,"_stage3_degree", spline_deg, "/model_output.RDS"))
# saveRDS(pad_stage4, paste0(mrbrt_output_dir, model_lab,"_stage4_degree", spline_deg, "/model_output.RDS"))
# saveRDS(pad_stage5, paste0(mrbrt_output_dir, model_lab,"_stage5_degree", spline_deg, "/model_output.RDS"))

# assemble list of models
mrbrt_mods<-lapply(grep(paste(paste0(outcome,"_",exp_cats),collapse = "|"),ls(),value=T),get)

mrbrt_mods <- list(b1rr, b2rr, b3rr, b4rr)

# # create predictions --------------------------

# Write predictions to model directory
lapply(mrbrt_preds, function(x){
  # print(paste0(x$working_dir,"model_predictions.csv"))
  write.csv(x$model_summaries, paste0(x$working_dir,"model_predictions.csv"), row.names = F)
})

