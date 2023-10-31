################################################################################################
### Purpose: Create ratios between alternative & ref gbd hearing thresholds for extracted data from NHANES
#################################################################################################
#setup
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
} else {
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
j_hear <- paste0(j,"FILEPATH", user, "FILEPATH")
hxwalk_temp <- paste0(j_root, "FILEPATH", user, "FILEPATH")

library(data.table)
library(openxlsx)
library(dplyr)
library(boot)
library(msm, lib.loc = "FILEPATH")
library(ggplot2)
library(logitnorm)
source(paste0(h_root,"FILEPATH"))
repo_dir <- "FILEPATH"

source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))


################################### BEGINNING OF HEARING PARALLELIZATION CODE ########################################################

#DEFINE ARGS-----------------------------------------------------------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
row <- as.numeric(args[1])
out_dir <- as.character(args[2])
job_name <- as.character(args[3])
combo_path <- as.character(args[4])

print(paste( "Let's check arguments! Row is", row))
cat(paste( "out_dir is", out_dir))

#PREP INPUT DATA-----------------------------------------------------------------------------------------------------------------------------
print("reading in microdata file")
combos <- data.table(read.xlsx(combo_path))
combos[ , mref := ifelse(name_alt == name_ref, 1, 0)]
combos <- combos[mref != 1]  #take out crosswalks for gbd ref thresholds
prepped_microdt <- data.table(read.xlsx(paste0(hxwalk_temp, "FILEPATH"))) #prevalences for each threshold

create_dt<- function(dt, row, dt_comb, by_vars){
  #format alt data
  alt_colname <- dt_comb[row , name_alt]
  alt_dt <- data.table(age2 = dt[,age2], sex = dt[,sex]) #change this to age or age2 depending on collapse
  alt_dt[ , alt_sample_size := .N, by = get(by_vars)]
  alt_dt$alt_prev <-  dt[ ,get(alt_colname)]
  alt_dt[alt_prev > 0.99, alt_prev := 0.99] 
  alt_dt[alt_prev < 0.001, alt_prev := 0.001] 
  z <- qnorm(0.975)
  alt_dt[ , alt_se := sqrt(alt_prev*(1-alt_prev)/alt_sample_size + z^2/(4*alt_sample_size^2))]
  alt_dt[ , logit_alt := logit(alt_prev)]
  alt_dt <- unique(alt_dt, by = by_vars) #year

  alt_dt[,logit_alt_se:=sapply(1:nrow(alt_dt), function(i){
    mean_i <- alt_dt[i,alt_prev]
    mean_se_i <- alt_dt[i,alt_se]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]

  #format ref data
  ref_colname <- dt_comb[row ,name_ref]
  ref_dt <- data.table(age2 = dt[,age2], sex = dt[,sex])
  ref_dt[ , ref_sample_size := .N, by = get(by_vars)]
  ref_dt$ref_prev <-  dt[ ,get(ref_colname)]
  ref_dt[ref_prev > 0.99, ref_prev := 0.99]
  ref_dt[ref_prev < 0.001, ref_prev := 0.001] 
  z <- qnorm(0.975)
  ref_dt[ , ref_se := sqrt(ref_prev*(1-ref_prev)/ref_sample_size + z^2/(4*ref_sample_size^2))]
  ref_dt[ , logit_ref := logit(ref_prev)]
  ref_dt <- unique(ref_dt, by = by_vars) #year

  ref_dt[,logit_ref_se:=sapply(1:nrow(ref_dt), function(i){
    mean_i <- ref_dt[i, ref_prev]
    mean_se_i <- ref_dt[i, ref_se]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]

  #merge dts
  dt_merge <- merge(alt_dt, ref_dt, by = by_vars) #year
  dt_merge[ ,logit_diff:=logit_alt-logit_ref]
  dt_merge[,logit_diff_se:=sqrt(logit_alt_se^2+logit_ref_se^2)]
  dt_merge[ , ratio := alt_prev/ref_prev]
  dt_merge[, `:=` (ratio_se = sqrt((alt_prev^2 / ref_prev^2) * (alt_se^2/alt_prev^2 + ref_se^2/ref_prev^2)))]
  dt_merge[ ,log_ratio := log(ratio)]

  dt_merge$log_ratio_se <- sapply(1:nrow(dt_merge), function(i) {
    mean_i <- dt_merge[i, "ratio"]
    se_i <- dt_merge[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  dt_merge <- dt_merge[!is.nan(ratio_se), ]
  dt_merge[ ,nvals_age := .N, by = age2] #see how many ratios there were for each age group

  return(dt_merge)
}

input_data <- create_dt(dt = prepped_microdt, row = row, dt_comb= combos, by_vars = c("age2","sex")) #gbd_age2 is now age2 

#RUN MRBRT MODEL-----------------------------------------------------------------------------------------------------------------------------

hearing_crosswalk <- run_mr_brt(
  output_dir = out_dir, 
  model_label = job_name,
  data = input_data,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.15,
  remove_x_intercept = FALSE
)

saveRDS(hearing_crosswalk, paste0(out_dir, job_name,"FILEPATH",job_name, "FILEPATH"))
predictions <- predict_mr_brt(hearing_crosswalk, write_draws = T)
saveRDS(predictions, paste0(out_dir, job_name,"FILEPATH",job_name, "FILEPATH"))
