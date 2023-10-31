########################################################################################################################
## Project: Neonatal Sepsis
## Purpose: Estimate EMR via MR-BRT for neonatal sepsis life table calculations
########################################################################################################################

rm(list=ls())
os <- .Platform$OS.type
if (os=="Windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  k <- "FILEPATH"
} else {
  j<- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH/", user)
  k <- "FILEPATH/"
}

code_dir <- "FILEPATH"
setwd(code_dir)

library(msm); library(matrixStats); library(reticulate); library(ggplot2)
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/utility.r")
## Source MR-BRT Functions
library(mrbrt001, lib.loc = "FILEPATH")

### Define Functions

## Get Best Run ID
best_run_id <- function(me) {
  df <- fread("FILEPATH/neonatal_sepsis_run_log.csv")
  run_id <- df[me_name==me & is_best==1]$run_id
  if (length(run_id) > 1) stop("More than 1 run_id")
  return(run_id)
}
## Melt Draws
melt_draws <- function(df, value.name){
  id_vars <- names(df)[!names(df) %like% "draw_"]
  df <- melt(df,id.vars=id_vars, variable.name="draw", value.name=value.name)
  return(df)
}
## Collapse Draws
collapse_draws <- function(df, draws_name="draw", variance=FALSE) {
  
  collapsed <- copy(df)
  
  # columns to collapse
  collapse_cols   <- c(colnames(collapsed)[grep(draws_name, colnames(collapsed))])
  
  # column names to keep
  cols <- c(colnames(collapsed)[colnames(collapsed) %in% c("super_region_id", "region_id", "location_id", "ihme_loc_id", "year_id", "age_group_id", "sex_id")], 
            "mean", "lower", "upper")
  
  # calculate mean, lower, and upper
  collapsed$mean  <- rowMeans(subset(collapsed, select=collapse_cols))
  collapsed$lower <- apply(subset(collapsed, select=collapse_cols), 1, function(x) quantile(x, 0.025))
  collapsed$upper <- apply(subset(collapsed, select=collapse_cols), 1, function(x) quantile(x, 0.975))
  
  # calculate variance
  if (variance) { collapsed$variance <- apply(subset(collapsed, select=collapse_cols), 1, function(x) var(x))
  cols <- c(cols, "variance") }
  
  # just keep needed columns
  collapsed <- subset(collapsed, select=cols)
  
  return(collapsed)
}
## Save Draws
save_draws <- function(df, folder) {
  unlink(folder, recursive=TRUE)
  dir.create(folder, showWarnings=FALSE)
  location_ids <- unique(df$location_id)
  mclapply(location_ids, function(x) {
    write.csv(df[location_id==x], paste0(folder, "/", x, ".csv"), row.names=FALSE)
  }, mc.cores=10)
}
## Get Data
pull_data <- function(run_id, gbd_round=7){
  wd <- getwd()
  
  # pull incidence data
  message("Pulling incidence data")
  setwd(paste0("FILEPATH"))
  files <- list.files()
  inc <- lapply(files, fread) %>% rbindlist(use.names=TRUE)
  inc <- inc[year_id %in% c(1990,1995,2000,2005,2010,2015,2019,2020)]
  
  # pull deaths
  message("Pulling mortality data")
  mort <- get_draws(gbd_id_type="cause_id", gbd_id=383, source="codem", measure_id=1, metric_id=1,
                    location_id=unique(inc$location_id), year_id=unique(inc$year_id), age_group_id=unique(inc$age_group_id),
                    sex_id=unique(inc$sex_id), gbd_round_id=gbd_round, decomp_step="step3")[,-c("cause_id","sex_name","metric_id","measure_id")]
  
  # reshape and merge 
  message("Calculating MI Ratio")
  mort <- melt_draws(mort, "mort")
  mort[, mort:=mort/pop]
  inc <- melt_draws(inc, "inc")
  
  df <- merge(mort,inc,by=c("location_id","year_id","age_group_id","sex_id","draw"))
  df[, mi_ratio:=mort/inc]
  df[,c("envelope","pop","mort","inc"):=NULL]
  df <- dcast(df, location_id + year_id + age_group_id + sex_id ~ draw)
  df <- collapse_draws(df,variance=TRUE)
  df[, se:=sqrt(variance)]
  
  # pull HAQI
  message("Merging HAQI")
  haqi <- get_covariate_estimates(1099,year_id=unique(df$year_id),gbd_round=gbd_round,decomp_step="iterative")[,.(location_id,year_id,mean_value)]
  setnames(haqi,"mean_value","haqi")
  
  # merge HAQI
  df <- merge(df,haqi,by=c("location_id","year_id"),all.x=TRUE)
  df[, haqi:=haqi/100]
  
  # close
  setwd(wd)
  return(df)
}
## Prep for MR-BRT
prep_mrbrt <- function(df) {

  # format vars
  df$mean_logit <- logit(df$mean)
  df$se_logit <- sapply(1:nrow(df), function(i) {
    mean_i <- as.numeric(df[i, "mean"])
    se_i <- as.numeric(df[i, "se"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  df[, study:=.I]
  df[, early_neonatal:=ifelse(age_group_id==2,1,0)]
  df[, male:=ifelse(sex_id==1,1,0)]

  # create MR data object
  dat <- MRData()
  dat$load_df(data=df, col_obs="mean_logit", col_obs_se="se_logit",
              col_covs=list("early_neonatal","male","haqi"), col_study_id="study")
  
  # return
  return(list(dat))
}
## Run MR-BRT
fit_mrbrt <- function(data,trim) {
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for neonatal sepsis EMR"))
  
  mod <- MRBRT(data=data,
               cov_models=list(
                 LinearCovModel("intercept", use_re=TRUE),
                 LinearCovModel("male"),
                 LinearCovModel("early_neonatal"),
                 LinearCovModel("haqi",
                                use_spline=TRUE,
                                spline_knots = array(c(0,0.4,0.8,1)),
                                spline_degree=2L,
                                spline_knots_type="domain",
                                prior_spline_monotonicity="decreasing",
                                spline_r_linear = TRUE,
                                spline_l_linear = TRUE
                 )
               ),
               inlier_pct = 1-trim)
  mod$fit_model(inner_print_level=5L, inner_max_iter=500L)
  
  # Predict out for plotting
  df_pred <- expand.grid(male=c(0,1),
                         early_neonatal=c(0,1),
                         haqi=seq(0,1,by=.01)) %>% data.table
  dat_pred <- MRData()
  dat_pred$load_df(
    data = df_pred, 
    col_covs=list("early_neonatal","male","haqi")
  )
  
  df_pred[, pred := mod$predict(data = dat_pred)]
  df_pred[, `:=` (sex = ifelse(male==1,"Male","Female"),
                  age = ifelse(early_neonatal==1,"ENN","LNN"))]
  
  # Draws of uncertainty
  samples <- mod$sample_soln(sample_size = 100L)
  
  draws <- mod$create_draws(
    data = dat_pred,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = FALSE
  )
  
  df_pred[, `:=` (lower = apply(draws, 1, function(x) quantile(x, 0.025)),
                  upper = apply(draws, 1, function(x) quantile(x, 0.975)))]
  
  # Pull model data with weights
  df_mod <- cbind(mod$data$to_df(), data.frame(w = mod$w_soln)) %>% data.table
  df_mod[, w := ifelse(w>=0.5,"Included","Trimmed")]
  df_mod[, `:=` (age=ifelse(early_neonatal==1,"ENN","LNN"),
                 sex=ifelse(male==1,"Male","Female"))]
  
  message("Predicting out EMR results by location/year/age/sex")
  
  # Make square df
  loc_ids <- get_location_hierarchy(35,decomp_step="iterative")
  loc_ids <- loc_ids[level >= 3, location_id]
  
  df <- expand.grid(location_id=loc_ids, sex_id=c(1,2), age_group_id=c(2,3), year_id=c(1990:2022)) %>% data.table
  df[, male:=ifelse(sex_id==1,1,0)]
  df[, early_neonatal:=ifelse(age_group_id==2,1,0)]
  
  # Pull and merge HAQI
  haqi <- get_covariate_estimates(1099,gbd_round_id=7,decomp_step="iterative")[,.(location_id,year_id,mean_value)]
  setnames(haqi,"mean_value","haqi")
  
  df <-  merge(df, haqi, by=c("location_id","year_id"), all.x=TRUE)
  df[, haqi:=haqi/100]

  # Predict out model draws
  pred <- MRData()
  pred$load_df(
    data = df, 
    col_covs=list("early_neonatal","male","haqi")
  )
  
  # Draws of uncertainty
  samples <- mod$sample_soln(sample_size = 1000L)
  
  draws <- mod$create_draws(
    data = pred,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = FALSE
  ) %>% inv.logit()
  
  colnames(draws) <- paste0("draw_",0:999)

  # Combine and return
  message("Draws by Location/Year/Age/Sex created!")
  df <- cbind(df,draws) %>% data.table
  df <- df[,-c("male","early_neonatal","haqi")]
  
  return(df)
}

### Run
run_id <- best_run_id("neonatal_sepsis")
df <- pull_data(run_id)
data <- prep_mrbrt(df)
draws <- fit_mrbrt(data,trim=0.2)
save_draws(draws,folder="FILEPATH")

