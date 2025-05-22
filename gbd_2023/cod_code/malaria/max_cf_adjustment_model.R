## mrbrt model on CF in logit space
#run two models one for each sex
#### use max cf of any location for <5 and MOZ only for >5

rm(list=ls())
data_root <- "FILEPATH"
cause <- "malaria"
run_date <- "FILEPATH"

library(data.table)
library(dplyr)

library(tidyr)

library(ggplot2)
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_envelope.R")
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_cod_data.R")

## specifications for MRBRT
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")

reticulate::use_python("FILEPATH")
library(reticulate)
mr <- import("mrtool")
cw <- import("crosswalk")

library(crosswalk002)
library(mrbrt002)

##****** functions
# Log transformation functions
logit     <- function(x){log(x/(1-x))}
inv_logit <- function(x){exp(x)/(1+exp(x))}

out_dir <- paste0(data_root,"FILEPATH/")

locs <- get_location_metadata(release_id =ADDRESS, location_set_id = ADDRESS)
locs <- locs[,.(location_id, location_name, ihme_loc_id)]
ages <- get_age_metadata(release_id =ADDRESS)
age_sub <- ages[,.(age_group_id, age_group_name)]
ages <- ages[,.(age_group_id, age_group_years_start, age_group_years_end)]
test_type <- "both_maxunder5"

#fix age_group_years_end
ages$age_group_years_end <- ages$age_group_years_end-0.00000001
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

##
df <- fread("/FILEPATH/updated_ssa_end_cod_data.csv")


## the test type will determine what max cf is chosen to feed into model
if (test_type == "MOZ"){

  cols <- c("age_group_id", "sex_id")
  df_test <- df[, max_cf := max(cf), by = cols] 
  
  df_test$flag <- ifelse(df_test$max_cf == df_test$cf, 1, 0)
  df_test <- df_test[df_test$flag == 1,]
  df_test$flag <- NULL
  
} else if (test_type == "both"){ #run model with both or just avg?
  df <- df[df$ihme_loc_id %in% c("MOZ", "BFA"),]
  
  cols <- c("age_group_id", "sex_id", "location_id")
  df_test <- df[, max_cf := max(cf), by = cols] 
  
  df_test$flag <- ifelse(df_test$max_cf == df_test$cf, 1, 0)
  df_test <- df_test[df_test$flag == 1,]
  df_test$flag <- NULL
  
} else if (test_type == "both_maxunder5"){ #run with max between MOZ and BFA under 5, MOZ over 5
  #df <- df[!(df$ihme_loc_id == "TZA"),]
  #df <- df[df$ihme_loc_id %in% c("MOZ", "BFA"),]
  df_sub <- df[df$age_group_id %in% c(3,34,238,388,389),]
  df <- df[!(df$age_group_id %in% c(3,34,238,388,389)),]
  df <- df[df$ihme_loc_id == "MOZ",]
  # df_sub_test <- df_sub %>% group_by(age_group_id, sex_id) %>% mutate(max_cf = max(cf))
  # df_test <- df %>% group_by("age_group_id", "age_group_name", "sex_id") %>% mutate(max_cf = max(cf))
  cols <- c("age_group_id", "sex_id")
  df_test <- df[, max_cf := max(cf), by = cols]
  df_sub_test <- df_sub[, max_cf := max(cf), by = cols]
  
  df_test$flag <- ifelse(df_test$max_cf == df_test$cf, 1, 0)
  df_sub_test$flag <- ifelse(df_sub_test$max_cf == df_sub_test$cf, 1, 0)
  df_test <- df_test[df_test$flag == 1,]
  df_test$flag <- NULL
  df_sub_test <- df_sub_test[df_sub_test$flag == 1,]
  df_sub_test$flag <- NULL
  df_test <- rbind(df_test, df_sub_test)
  rm(df_sub_test)
} else{ #run with any loc max under 5 with moz only over 5
  #df <- df[df$ihme_loc_id %in% c("MOZ", "BFA"),]
  df_sub <- df[df$age_group_id %in% c(3,34,238,388,389),]
  df <- df[!(df$age_group_id %in% c(3,34,238,388,389)),]
  df <- df[df$ihme_loc_id == "MOZ",]
  
  cols <- c("age_group_id", "sex_id")
  df_test <- df[, max_cf := max(cf), by = cols]
  df_sub_test <- df_sub[, max_cf := max(cf), by = cols]
  
  df_test$flag <- ifelse(df_test$max_cf == df_test$cf, 1, 0)
  df_sub_test$flag <- ifelse(df_sub_test$max_cf == df_sub_test$cf, 1, 0)
  df_test <- df_test[df_test$flag == 1,]
  df_test$flag <- NULL
  df_sub_test <- df_sub_test[df_sub_test$flag == 1,]
  df_sub_test$flag <- NULL
  df_test <- rbind(df_test, df_sub_test)
  rm(df_sub_test)
}

# add on age_meta to get age_start
df_test <- left_join(df_test, ages, by = "age_group_id")

#all <- copy(dalys_global_all)
df_test <- df_test[,.(location_id, year_id, age_group_id, age_start,age_end,sex_id, cf, variance_rd_logit_cf, location_name, ihme_loc_id, age_group_name,nid)]

#all <- all[rate != 0]  
# transform into logit space

df_test[, logit_cf := logit(cf)]

# df_test<- as.data.frame(df_test)
# df_test$log_mr <- crosswalk002::delta_transform(mean = df_test$mrate, sd = df_test$se_mr, transformation = "linear_to_logit")

# we have CF RD variance (re-distribution) which we can calculate SD from
# First need to transform back from logit space

min_val<-df_test[df_test$variance_rd_logit_cf > 0,]
min_val <- min_val[which.min(min_val$variance_rd_logit_cf),]

df_test$variance_rd_logit_cf <- ifelse(df_test$variance_rd_logit_cf == 0, unique(min_val$variance_rd_logit_cf), df_test$variance_rd_logit_cf)

df_test[,variance_rd_cf := inv_logit(variance_rd_logit_cf)]
df_test[,cf_se := sqrt(variance_rd_cf)]
df_test[,logit_cf_se := sqrt(variance_rd_logit_cf)]

# subset to sex_specific models
df_f <- df_test[df_test$sex_id == 2,]
df_m <- df_test[df_test$sex_id == 1,]
#***********************************************************************************************************************

#----RUN MODELS---------------------------------------------------------------------------------------------------------

################## running females
message("Running female model")
dt_f <- copy(df_f)
dt_f<- dt_f[order(rank(age_start))]
# Set MR-BRT data format
dat_f <- MRData()
dat_f$load_df(
  data = dt_f,
  col_obs = "cf",
  col_obs_se = "cf_se",
  col_covs = list("age_start"),
  col_study_id = "location_id" #or year_id?
)


mod_f <- MRBRT(
  data = dat_f,
  cov_models = list(
    LinearCovModel("intercept", use_re = FALSE),
    LinearCovModel(
      alt_cov = "age_start",
      use_spline = TRUE,
      spline_knots = array(c(0,.02, .10, 0.5, 1)),
      #spline_knots = array(seq(0, 1, by = 0.2)),
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = FALSE,
      spline_l_linear = FALSE
      # prior_spline_monotonicity = 'increasing'
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod_f$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# save out model
py_save_object(object = mod_f, filename = paste0(out_dir, "malaria_female_max_cf_spline_model.pkl"), pickle = "dill")

# get gammma, beta coef, intercept
#view the results--fixed effects 
mod_f$beta_soln
mod_f$fe_soln
mod_f$gamma_soln

dat_pred_f <- MRData()
dat_pred_f$load_df(
  data = dt_f, 
  col_covs=list('age_start')
)

dt_f$pred_f <- mod_f$predict(data = dat_pred_f)
with(dt_f, plot(age_start, cf, ann = FALSE)) #col=ifelse(location_name =="Mozambique","blue","red"),
with(dt_f, lines(age_start, pred_f))
title(main= "Malaria max CF using any location <5, MOZ only >5; Females", 
           sub = "TZA not included",
           xlab = "Age group start (years)",
           ylab = "CF")

# # visualize knot locations
for (k in mod_f$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

################################################################################################
########################## running males
message("Running male model")
dt_m <- copy(df_m)
dt_m<- dt_m[order(rank(age_start))]
# Set MR-BRT data format
dat_m <- MRData()
dat_m$load_df(
  data = dt_m,
  col_obs = "cf",
  col_obs_se = "cf_se",
  col_covs = list("age_start"),
  col_study_id = "location_id" #or year_id?
)


mod_m <- MRBRT(
  data = dat_m,
  cov_models = list(
    LinearCovModel("intercept", use_re = FALSE),
    LinearCovModel(
      alt_cov = "age_start",
      use_spline = TRUE,
      spline_knots = array(c(0,.02, .10, 0.5, 1)),
      #spline_knots = array(seq(0, 1, by = 0.2)),
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = FALSE,
      spline_l_linear = FALSE
      # prior_spline_monotonicity = 'increasing'
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod_m$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

dat_pred_m <- MRData()
dat_pred_m$load_df(
  data = dt_m, 
  col_covs=list('age_start')
)

dt_m$pred_m <- mod_m$predict(data = dat_pred_m)
with(dt_m, plot(age_start, cf, ann = FALSE))#col=ifelse(location_name =="Mozambique","blue","red"),
with(dt_m, lines(age_start, pred_m))
title(main = "Malaria max CF using any location <5, MOZ only >5; Males", 
      sub = "TZA not included",
      xlab = "Age group start (years)",
      ylab = "CF")

# # visualize knot locations
for (k in mod_m$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")


pdf(onefile = TRUE, file = paste0(out_dir, "malaria_spline_tests.pdf"))


setnames(dt_m, "pred_m", "pred")
setnames(dt_f, "pred_f", "pred")

final_dt <- rbind(dt_m, dt_f)

write.csv(final_dt, "FILEPATH/malaria_spline_adjusted_cf_caps_anyloc_under5_moz_above5_",date,".csv", row.names = FALSE)
