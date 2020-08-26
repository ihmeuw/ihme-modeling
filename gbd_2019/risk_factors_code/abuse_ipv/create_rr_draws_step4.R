# AUTHOR
# Updated by AUTHOR for step4
# 23 August 2019
# GBD
# IPV, CSA risk factors
# create RR draws for all risk-outcome pairs related to IPV and CSA

rm(list=ls())

ipv_path <- paste0("FILEPATH/rr_ipv_",date,".csv")

library(data.table)
source("FILEPATH/get_draws.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/mr_brt_functions.R")

model_dir <- "FILEPATH"
draws <- paste0("draw_",0:999)

fit <- readRDS(paste0(model_dir, "/", model_label, "/model_output.RDS"))


## FN TO SUBSET MRBRT OUTPUTS TO DESIRED ROW ---------------------------- ##
## adapted from USERNAME's script

format_preds <- function(fit){
  pred_dt <- as.data.table(load_mr_brt_preds(fit)["model_draws"])
  names(pred_dt) <- sub("model_draws.", "", names(pred_dt))
  
  # only keep where all covariates==0
  xcovs <- unique(fit$model_coefs$x_cov)
  xcovs <- xcovs[xcovs!="intercept"]
  xcovs <- paste0("X_", xcovs)
  max <- 1+sum(startsWith(colnames(pred_dt), "X_cv"))
  pred_dt$drop <- 0
  pred_dt$drop <- rowSums(pred_dt[, 2:max])
  
  return(pred_dt)
}


## FN TO EXPONENTIATE AND FORMAT DRAWS ----------------------------------- ##

format_rr <- function(pred_dt, cause, rei){
  pred_dt <- pred_dt[pred_dt$drop==0,]
  pred_dt <- pred_dt[1,]
  data <- subset(pred_dt, select = grep("draw", names(pred_dt)))
  data <- melt(data = data, measure.vars = draws, variable.name = "draws", value.name = "log_mean")
  
  #exponentiate out of log space
  data[,mean:=exp(log_mean)]
  
  #cast to wide
  data <- dcast(data = data, formula = .~ draws, value.var = "mean")
  data[,.:=NULL] # deleting column that got added in dcast
  
  #add reference rows
  ref <- matrix(rep(1,1000),1,1000) %>% as.data.table()
  names(ref) <- names(data)
  data <- rbind(data, ref)
  data[,cause_id:=cause]
  data[,parameter:= c("cat1","cat2")]
  
  #this is format we need to fit
  step2 <- get_draws("rei_id", rei, source="rr", decomp_step="step2", gbd_round_id=6)
  step2 <- step2[cause_id==cause,]
  format <- step2[,-draws,with=F]
  
  #create final sheet
  upload_dt <- merge(format, data, by = c("cause_id","parameter"), all.x = T)
  return(upload_dt)
}

## CSA: RR FOR ALCOHOL AND DEPRESSION -------------------------------------- ##

## alcohol - settings
model_label <- "csa_alc_ss"
cause_id <- 560
rei_id <- 244

#format
fit <- readRDS(paste0(model_dir, "/", model_label, "/model_output.RDS"))
pred_dt <- format_preds(fit)
csa_alc_dt <- format_rr(pred_dt, cause = cause_id, rei = rei_id)

#depression - settings
model_label <- "csa_dep_ss_confound"
cause_id <- 568
rei_id <- 244

#format
fit <- readRDS(paste0(model_dir, "/", model_label, "/model_output.RDS"))
pred_dt <- format_preds(fit)
csa_depr_dt <- format_rr(pred_dt, cause = cause_id, rei = rei_id)

#combine 
csa_dt_female <- rbind(csa_depr_dt,csa_alc_dt)
csa_dt_male <- copy(csa_dt_female)[,sex_id = 2]

#save to flat
save_path_female <- paste0("FILEPATH/csa_female_rr_",date,".csv")
write.csv(csa_dt_female, save_path_female, row.names = F)

save_path_male <- paste0("FILEPATH/csa_male_rr_",date,".csv")
write.csv(csa_dt_male, save_path_male, row.names = F)

## IPV: RR FOR DEPRESSION -------------------------------------- ##

# settings
model_label <- "ipv_dep_ss_sb_rc"
cause_id <- 568
rei_id <- 167

# format
fit <- readRDS(paste0(model_dir, "/", model_label, "/model_output.RDS"))
pred_dt <- format_preds(fit)
ipv_dt <- format_rr(pred_dt, cause = cause_id, rei = rei_id)

#save to flat
save_path <- paste0("FILEPATH",model_label,"_",date,".csv")
write.csv(ipv_dt, save_path, row.names = F)


# SAVE_RESULTS

input_dir <- "FILEPATH"
description <- 'updated RR from MRBRT'
risk_type <- 'rr'
decomp_step <- 'step4'
mark_best <- TRUE



# Save IPV RR  
sex_id <- 2
modelable_entity_id <- 9076
input_file_pattern <- "final_ipv_rr_formatted_for_upload.csv"
save_results_risk(input_dir=input_dir, input_file_pattern=ipv_path, modelable_entity_id=modelable_entity_id,
                  description=description, risk_type=risk_type, sex_id=sex_id, decomp_step=decomp_step, mark_best=mark_best)

# Save CSA RR  
sex_id <- 2
modelable_entity_id <- 9086
input_file_pattern <- "final_csaF_rr_formatted_for_upload.csv"
save_results_risk(input_dir=input_dir, input_file_pattern=input_file_pattern, modelable_entity_id=modelable_entity_id,
                  description=description, risk_type=risk_type, sex_id=sex_id, decomp_step=decomp_step, mark_best=mark_best)
sex_id <- 1
modelable_entity_id <- 9087
input_file_pattern <- "final_csaM_rr_formatted_for_upload.csv"
save_results_risk(input_dir=input_dir, input_file_pattern=input_file_pattern, modelable_entity_id=modelable_entity_id,
                  description=description, risk_type=risk_type, sex_id=sex_id, decomp_step=decomp_step, mark_best=mark_best)

