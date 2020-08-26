################################################################################
## DESCRIPTION ##  Do network diet analysis; called by launch_crosswalk. Three parts to this script. 1- prep data, 2-do the network analysis in mr-brt, 3- adjust the data
###############################################################################

rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

code_dir <- if (os == "Linux") paste0("/ihme/code/dbd/", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, 'shared/utils/primer.R'))

library(ggplot2)
library(data.table)
library(openxlsx)
library(reshape2)
library(msm)
library(dplyr)
library(readstata13)

# source
source(FILEPATH)

args = commandArgs(trailingOnly=TRUE)

print(args)
if (length(args)!=7) {
  stop("Seven arguments needed")
} else if (length(args)>0) {
  risk <- args[1]
  dir <- args[2]
  mrbrt_version <- args[3]
  dp <- args[4]
  rbrt <- args[5]
  adjdata <- args[6]
  split_data_version <- args[7]
}



######################################################################################
# Part 1: #Load data and functions
######################################################################################

print(dp)
print(rbrt)
print(adjdata)

print(risk)

dir <- "FILEPATH"

# -----------------------------------

all_diet_data <- fread("FILEPATH/all_diet_data_post_as_2019.csv")
setnames(all_diet_data, "val", "mean")

predict_data_prep <- function(risk, save = FALSE ){
  
  output_dir <- "FILEPATH/adjustment_input/"
  
  if(!exists("all_diet_data")){ stop("all_diet_data is not loaded in the environment")}
  if(!risk %in% unique(all_diet_data$ihme_risk)){
    stop("risk is not one of the gbd diet risk")}
  risk_data <- all_diet_data[ihme_risk==risk]
  if(risk=="diet_transfat"){
    
    risk_data <- as.data.table(read.dta13("FILEPATH/as_split_diet_transfat.dta"))
    risk_data[,sex:="Both"]
    setnames(risk_data, "new_mean", "mean")
    setnames(risk_data, "new_se", "standard_error")
    risk_data[, variance:=standard_error^2]
     }

  setnames(risk_data, "cv_sales_data", "cv_sales")
  setnames(risk_data, "se_adj", "was_se_imputed")

  risk_data[cv_dr==1, type:="dr"]

  risk_data[cv_FFQ==1, type:="ffq"]
  risk_data[cv_cv_hhbs==1, type:="hhbs"]
  risk_data[cv_fao==1, type:="fao"]
  risk_data[cv_cv_urinary_sodium==1, type:="usodium"]
  risk_data[cv_sales==1, type:="sales"]
  
  risk_data[sex=="Both", c("sex_id", "was_both_sex"):=list(1,1)]
  risk_data[sex!="Both", was_both_sex:=0]
  
  rep_both <- risk_data[sex=="Both"]
  rep_both$sex_id <- 2
  risk_data <- rbind(risk_data,rep_both)
  
  if (save){write.csv(risk_data, paste0(output_dir, risk, "_pre_xwalk.csv"))
  message(paste0("saved data to: ",output_dir, risk, "_pre_xwalk.csv"))
  }
  
  return(risk_data)
}

predict_bundle_version_prep <- function(risk){
  

  risk_data <- fread(paste0("FILEPATH",split_data_version, "/compiled_split/",risk,".csv"))
  
  if( !"crosswalk_parent_seq" %in% colnames(risk_data)){ risk_data[, crosswalk_parent_seq:=as.numeric(NA)]}
  setnames(risk_data, "val", "mean")
  setnames(risk_data, "cv_sales_data", "cv_sales")
  setnames(risk_data, "se_adj", "was_se_imputed")
  
  
  risk_data[sex=="Both", sex_id:=3]
  risk_data[sex=="Male", sex_id:=1]
  risk_data[sex=="Female", sex_id:=2]
  
  
  risk_data[cv_dr==1, type:="dr"]
  risk_data[cv_sales==1, type:="sales"]
  risk_data[cv_FFQ==1, type:="ffq"]
  risk_data[cv_cv_hhbs==1, type:="hhbs"]
  risk_data[cv_fao==1, type:="fao"]  
  risk_data[cv_cv_urinary_sodium==1, type:="usodium"] 
  risk_data <- risk_data[is_outlier==0]
  risk_data[sex=="Both", c("sex_id", "was_both_sex"):=list(1,1)]
  risk_data[sex!="Both", was_both_sex:=0]
  rep_both <- risk_data[sex=="Both"]
  rep_both$sex_id <- 2
  risk_data <- rbind(risk_data,rep_both)

  
  return(risk_data)
}

data_prep <- function(risk) {
  if(!exists("all_diet_data")){ stop("all_diet_data is not loaded in the environment")}
  if(!risk %in% unique(all_diet_data$ihme_risk)){
    stop("risk is not one of the gbd diet risk")}

  risk_data <- all_diet_data[ihme_risk==risk & is_outlier==0]
  if(risk=="diet_whole_grains"){
    grains_fao <- fread("FILEPATH/compiled_split/diet_whole_grains.csv")
    grains_fao <- grains_fao[cv_fao==1, ]
    grains_fao[, c("urine_2","iso3"):=NA]
    setnames(grains_fao, "val", "mean")
    grains_fao <- grains_fao[, colnames(risk_data), with=FALSE]
    risk_data <- risk_data[cv_fao==0]
    risk_data <- rbind(risk_data, grains_fao)
  }
  
  
  setnames(risk_data, "mean", "data")
  setnames(risk_data, "cv_sales_data", "cv_sales")

  print("The data distribution of both sex  datapoints is: ")
  print(risk_data[sex=="Both", .(datapoint_count = .N), by = list(cv_fao, cv_sales, cv_FFQ, cv_cv_hhbs)])
  
  risk_data$sex_id[risk_data$sex == "Male"] <- 1 
  risk_data$sex_id[risk_data$sex == "Female"] <- 2
  risk_data$sex_id[risk_data$sex == "Both"] <- 1
  
  rep_both <- risk_data[sex=="Both"]
  rep_both$sex_id <- 2
  risk_data <- rbind(risk_data,rep_both)

  if(risk == "diet_zinc"){
    risk_data <- risk_data[age_group_id==5,]
  }
  
  #outlier some risk specific things
  if(risk=="diet_ssb"){
    risk_data[svy=="National Health and Nutrition Examination Survey", is_outlier:=1]
  }
  if(risk=="diet_whole_grains"){
    risk_data[location_id==130, is_outlier:=1]
  }
  
  if(risk=="diet_legumes"){
    risk_data[cv_cv_hhbs==1, is_outlier:=1]  # unstable data
  }
  if(risk=="diet_omega_3"){
    risk_data[location_id==67, is_outlier:=1]   #outliering extremely high omega 3 intake values
    risk_data[cv_FFQ==1, is_outlier:=1]
    risk_data[year_id < 1998, is_outlier:=1]   #
    risk_data[location_id!=102, is_outlier:=1]   #USA has huge impact
    
    message("dropping some things")
  }
  
  if(risk=="diet_procmeat"){
    risk_data[cv_fao==1, is_outlier:=1]
    locs <- get_location_metadata(35)
    india_locs <- locs[location_id==6 | parent_id==6, location_id]
    risk_data[location_id %in% india_locs, is_outlier:=1]     #unstable data
   }
  
  risk_data <- risk_data[is_outlier==0]

    keepvar <- c("location_id", "year_id","data","standard_error","se_adj","nid","sex_id","age_group_id", "sample_size","cv_fao","cv_dr","cv_sales","cv_FFQ","cv_cv_hhbs", "cv_cv_urinary_sodium")

  risk_data <- risk_data[, keepvar, with=FALSE]
  print(risk_data[, .(datapoint_count = .N), by = list(cv_fao, cv_dr, cv_sales, cv_FFQ, cv_cv_hhbs)])
  print(risk_data[se_adj==1, .(se_adjustment = .N), by = list(cv_fao, cv_dr, cv_sales, cv_FFQ, cv_cv_hhbs)])
  ################
  return(risk_data)
  
}

match_data <- function(risk) {
  print(paste0("-------------- ", risk, " ------------"))
  print("--------------------------------------")
  bundle_prep <- data_prep(risk)
  #determine which types of data the risk has and which reference-alternate definitions to find matches for
  all_cvs <- c("cv_fao", "cv_sales", "cv_FFQ","cv_cv_hhbs","cv_cv_urinary_sodium")
  alternate_defs <- c("cv_dr")
  for(cv in all_cvs){
    if(sum(bundle_prep[,get(cv)], na.rm=TRUE) > 1){alternate_defs <- c(alternate_defs, cv)}}
  xwalks <- as.data.table(t(combn(alternate_defs,2)))
  setnames(xwalks, c("V1","V2"), c("reference","alternate"))
  if(risk=="diet_salt"){setnames(xwalks, c("reference","alternate"), c("alternate","reference"))
    setcolorder(xwalks, c("reference", "alternate"))}
  if(risk=="diet_legumes"){
    xwalks <- xwalks[!(reference=="cv_fao" & alternate=="cv_sales")]
  }
  alternate_defs <- xwalks$alternate
  reference_defs <- xwalks$reference
  print(xwalks)
  
  # for each reference-alternate definition, find all common "full" matches, reshapes wide and makes ratio and se columns. 
  risk_matched <- data.table()
  for( n in 1:nrow(xwalks)){
    pair_1 <- xwalks[n,1]$reference
    pair_2 <- xwalks[n,2]$alternate
    
    message(n)
    bundle_prep <- data_prep(risk)
    #------------------------------------------------------------
    if(risk=="diet_salt"){ 
      bundle_prep[, year_id:=round_any(year_id, 10)]
      bundle_prep[age_group_id %in% c(1:20), age_group_id:=round_any(age_group_id, 2)]}     #5 year bins and 10 year age bins
    if( pair_1== "cv_FFQ" | pair_2=="cv_FFQ"){
      bundle_prep[, year_id:=round_any(year_id, 5)]}  #expand to 5 year bings
    if(pair_1=="cv_fao" & pair_2=="cv_sales"){
      bundle_prep <- bundle_prep[year_id %in% c(1980, 1985, 1990, 1995, 2000, 2005, 2010) & age_group_id %in% c(seq(2, 20, by=3),30)]
    }
    
    bundle_prep[, full := paste(location_id, age_group_id, year_id, sex_id, sep = "-")] 
    #-------------------------------------------------------------
    for (def in unique(c(reference_defs,alternate_defs))) {
      col <- paste0("has_", def)
      bundle_prep[, paste0(col) := as.numeric(sum(get(def)) > 0), by=full]
    }
    matched_data <- bundle_prep[get(paste0("has_", pair_1))==1 & get(paste0("has_", pair_2))==1]
    matched_data <- matched_data[get(pair_1)==1 | get(pair_2)==1]
    if( nrow(matched_data)>1){
      matched_data$def <- "ref"
      matched_data[get(pair_2)==1, def:="alt"]
      ref <- matched_data[def=="ref"]
      alt <- matched_data[def=="alt"]
      matched_wide <- merge(ref, alt, by=c("full","location_id", "year_id","sex_id","age_group_id"), allow.cartesian = TRUE)
      if(risk=="diet_salt" & (pair_1=="cv_cv_urinary_sodium" & pair_2=="cv_dr")){
        validation_data <- fread(paste0(h,"FILEPATH/sodium_validation_data_cleaned.csv"))
        validation_data <- validation_data[comparison=="dr"]
        place <- colnames(validation_data)[which(colnames(validation_data) %in% colnames(matched_wide))]
        validation_data <- validation_data[,place, with=FALSE]
        matched_wide <- rbind(matched_wide, validation_data, fill=TRUE)
      }
      
      matched_wide[, ratio:= data.y / data.x]
      matched_wide[, log_ratio:= log(ratio)]
      matched_wide[, ratio_se := sqrt(data.y^2/data.x^2 * (standard_error.y^2/data.y^2 + standard_error.x^2/data.x^2))]
      matched_wide$log_ratio_se <- sapply(1:nrow(matched_wide), function(i) {
        ratio_i <- matched_wide[i, "ratio"]
        ratio_se_i <- matched_wide[i, "ratio_se"]
        deltamethod(~log(x1), ratio_i, ratio_se_i^2)
      })
      setnames(matched_wide, c("nid.x","nid.y", "data.x", "data.y"), c("nid.ref","nid.alt", "data.ref", "data.alt"))
      keep_vars <- c("location_id", "year_id","sex_id","age_group_id", "nid.alt","nid.ref","log_ratio","log_ratio_se", "ratio", "ratio_se", "data.ref", "data.alt")
      matched_wide <- matched_wide[,keep_vars, with=FALSE]
      matched_wide[, reference := pair_1]
      matched_wide[, alternate := pair_2]
      matched_wide[, comp := paste0(pair_1, "-", pair_2)]
      matched_wide[, id := paste0(nid.alt, "-", nid.ref)]
      clean_data <- matched_wide
      risk_matched <- rbind(risk_matched, clean_data)
      
      
    }
  }
  
  large_se <- which(risk_matched$log_ratio_se > 9999)
  large_rr <- which(risk_matched$log_ratio > 9999)
  if(length(large_se) > 0 | length(large_rr) > 0 ){
    n <- unique(c(large_se, large_rr))
    print(paste0("There are ", length(n), " impossibly large se or rr values that have been dropped."))
    risk_matched <- risk_matched[-n, ]      
  }
  
  if(nrow(risk_matched > 0)){
  if(risk=="diet_salt"){ risk_matched[alternate=="cv_dr", cv_dr:=1]}  
  if("cv_fao" %in% risk_matched$alternate){risk_matched[alternate=="cv_fao",cv_fao:=1]}
  if("cv_sales" %in% risk_matched$alternate){risk_matched[alternate=="cv_sales",cv_sales:=1]}
  if("cv_FFQ" %in% risk_matched$alternate){risk_matched[alternate=="cv_FFQ",cv_FFQ:=1]}
  if("cv_cv_hhbs" %in% risk_matched$alternate){risk_matched[alternate=="cv_cv_hhbs",cv_cv_hhbs:=1]}
  if("cv_fao" %in% risk_matched$reference){risk_matched[reference=="cv_fao",cv_fao:=-1]}
  if("cv_sales" %in% risk_matched$reference){risk_matched[reference=="cv_sales",cv_sales:=-1]}
  if("cv_FFQ" %in% risk_matched$reference){risk_matched[reference=="cv_FFQ",cv_FFQ:=-1]}
  if("cv_cv_hhbs" %in% risk_matched$reference){risk_matched[reference=="cv_cv_hhbs",cv_cv_hhbs:=-1]}
  risk_matched[is.na(risk_matched)] <- 0
  #####
  print(paste0("The number of matches for ", risk, " are:"))
  print(table(risk_matched$comp))
  print("The location id breakdown is: ")
  print(risk_matched[, .(loc_count = .N), by = location_id])
  print("--------------------------------------")
  risk_matched$comp <- NULL
  }else{
    print("Alert! There are no matches for the data!")
  }
return(risk_matched)
}

fit_diet_crosswalks <- function(risks_to_brt, use_dir, save_coefs= TRUE, version=2, overwrite=TRUE, plot_funnel=TRUE){
  
  include_sex <- TRUE
  new_coefficients <- data.table("cv_fao"=NA,"cv_sales"=NA,"cv_cv_hhbs"=NA, "cv_FFQ"=NA,"Z_intercept"=NA , "Y_mean"=NA  ,"Y_negp"=NA , "Y_mean_lo"=NA  ,  "Y_mean_hi"=NA, "Y_mean_fe"=NA,"Y_negp_fe"=NA,"Y_mean_lo_fe"=NA,"Y_mean_hi_fe"=NA ,"beta"=NA,"risk"=NA)
  input_dir <- paste0(use_dir,"/input_files/")
  
  #make folder for outputs if it doesn't exist yet
  save_dir <- paste0(use_dir, "/output",version)
  
  if(!dir.exists(save_dir)){
    dir.create(save_dir)
  }
  
  for(risk in risks_to_brt){
    data_file <- paste0(input_dir, risk, ".csv")
    label <- paste0(risk, "_v", version)
    cols <- names(data)[which(colnames(data) %in% c("cv_fao","cv_sales","cv_cv_hhbs","cv_FFQ", "cv_dr"))]
    data[, comp := paste0(reference, "-", alternate)]
    num_pairs <- length(unique(data$comp))
    
    trim <- 0.1
    if(risk=="diet_redmeat" | risk=="diet_calcium_low" | risk=="diet_fiber" | risk=="diet_veg"){trim <- 0.2}
    
    
    
    ##### Either a network analysis or a normal reg if there is only one ratio comparison
    ### 1) network analysis
    if ( num_pairs > 1){
      message(paste0(risk, " requires a network analysis"))
      
      if(include_sex){ cov_list <- list( cov_info(c(cols, "sex_id"), "X"))
      }else{cov_list <- list( cov_info(cols, "X")) }
      
      fit1 <- run_mr_brt(
        output_dir = paste0(save_dir),
        model_label = label,
        data = data_file,
        mean_var = "log_ratio",
        se_var = "log_ratio_se",
        remove_x_intercept = TRUE,
        method = "trim_maxL",
        trim_pct = trim,
        study_id = "id",
        covs = cov_list,    
        overwrite_previous = overwrite
      )
      coefs <- fit1$model_coefs
      coefs$risk <- risk
      
     #now predict to get coefs
        n <- length(cols)
        df_pred <- matrix(0, nrow=n+1, ncol=n+1)
        for(i in 2:(n+1)){
          df_pred[i,i] <- 1}
        colnames(df_pred) <- c("intercept",cols)
        df_pred <- as.data.table(df_pred)
        
        if(include_sex){  
          df_pred$sex_id <- 0
          other_pred <- df_pred
          other_pred$sex_id <- 1
          df_pred <- rbind(df_pred, other_pred)
        }
        pred1 <- predict_mr_brt(fit1, newdata = df_pred)
        
        new_coefs <- pred1$model_summaries
        new_coefs <- as.data.table(new_coefs)
        new_coefs$beta <- 1/exp(new_coefs$Y_mean)
        new_coefs$risk <- risk
        if("X_cv_fao" %in% colnames(new_coefs)){setnames(new_coefs, "X_cv_fao", "cv_fao")}else{ new_coefs[, cv_fao:=0]}
        if("X_cv_dr" %in% colnames(new_coefs)){setnames(new_coefs, "X_cv_dr", "cv_dr")}else{ new_coefs[, cv_dr:=0]}
        if("X_cv_sales" %in% colnames(new_coefs)){setnames(new_coefs, "X_cv_sales", "cv_sales")}else{ new_coefs[, cv_sales:=0]}
        if("X_cv_FFQ" %in% colnames(new_coefs)){setnames(new_coefs, "X_cv_FFQ", "cv_FFQ")}else{ new_coefs[, cv_FFQ:=0]}
        if("X_cv_cv_hhbs" %in% colnames(new_coefs)){setnames(new_coefs, "X_cv_cv_hhbs", "cv_cv_hhbs")}else{ new_coefs[, cv_cv_hhbs:=0]}
        if("X_sex_id" %in% colnames(new_coefs)){setnames(new_coefs, "X_sex_id", "sex_id")}
        new_coefs[cv_fao==0 & cv_sales==0 & cv_cv_hhbs==0 & cv_FFQ==0 & cv_dr==0, drop:=1]
        new_coefs <- new_coefs[is.na(drop),]
        new_coefs$drop <- NULL
        setcolorder(new_coefs, c("risk", "cv_fao","cv_sales","cv_cv_hhbs","cv_FFQ","cv_dr","sex_id"))
        if(save_coefs){write.csv(new_coefs, paste0(use_dir,"/coefficients/", risk, ".csv"),row.names = FALSE)}
        
        new_coefficients <- rbind(new_coefficients, new_coefs, fill=TRUE)
      
      
    }else{
      #######
      # 2) not a network analysis
      message("Not doing a network here")
      
      if(include_sex){ cov_list <- list( 
        cov_info( "sex_id", "X"))
      }else{cov_list <- list() }
      
      fit1 <- run_mr_brt(
        output_dir = paste0(save_dir),
        model_label = label,
        data = data_file,
        mean_var = "log_ratio",
        se_var = "log_ratio_se",
        covs = cov_list, 
        method = "trim_maxL",
        trim_pct = trim,
        study_id = "id",
        overwrite_previous = overwrite
      )
      coefs <- fit1$model_coefs
      coefs$risk <- risk
      
      #now predict to get coefs
        n <- length(cols)
        df_pred <- data.table("intercept"=c(0,1))
        if(include_sex){
          df_pred$sex_id <- 1
          other_pred <- df_pred
          other_pred$sex_id <- 2
          df_pred <- rbind(df_pred, other_pred)
        }
        pred1 <- predict_mr_brt(fit1, newdata = df_pred)
        
        new_coefs <- pred1$model_summaries
        new_coefs <- as.data.table(new_coefs)
        new_coefs$beta <- 1/exp(new_coefs$Y_mean)
        new_coefs$risk <- risk
        ref <- unique(data$reference)
        alt <-  unique(data$alternate) 
        new_coefs[X_intercept==1, paste(alt):=1]
        new_coefs[is.na(new_coefs)] <- 0 
        if(risk!="diet_salt"){
          new_coefs <- new_coefs[X_intercept==1,]
        }
        if("X_sex_id" %in% colnames(new_coefs)){setnames(new_coefs, "X_sex_id", "sex_id")}
        setcolorder(new_coefs, c("risk",paste(alt), "sex_id"))
        if(save_coefs){write.csv(new_coefs, paste0(use_dir, "/coefficients/", risk, "_v", version,".csv"),row.names = FALSE)}
        
        new_coefficients <- rbind(new_coefficients, new_coefs, fill=TRUE)
      
    } #end non-network analysis
    
    if(plot_funnel){
      plot_mr_brt(fit1)
    }
    
    save_dirs = paste0(save_dir, "/", label, "/modelfit_obj.rds")
    saveRDS( fit1, file=save_dirs)
    
    
  }  #end risk loop
  
  
}

implement_diet_crosswalk <- function(df, coef_file_name){
  
  # prep coefficients for merge
  coefs <- fread(paste0(coef_file_name))
  all_cvs <- c("cv_fao", "cv_sales", "cv_FFQ","cv_cv_hhbs","cv_cv_urinary_sodium", "cv_dr")
  
  for(cv in all_cvs){ if(! cv %in% colnames(coefs)){coefs[, paste(cv):=0]}}
  coefs[, pred_mean:=Y_mean_fe]
  coefs[, pred_se:=(Y_mean_hi_fe-Y_mean_lo_fe)/3.92]
  
  coefs <- coefs[, .(cv_fao, cv_sales, cv_FFQ,cv_cv_hhbs,cv_cv_urinary_sodium, cv_dr, sex_id, pred_mean, pred_se)]
  coefs[sex_id==0, sex_id:=2] 
  # prep data to merge on coefficients
  
  message("coefs have been read in! beginning first delta method")
  to_drop <- c("diet_2","metc","met", "urine_2","microdata_tabs", "student_lit", "ihme_data","level", "upper","lower")
  df[, c(to_drop):=NULL]
  df[, mean_log:=log(mean)]
  df$se_log <- sapply(1:nrow(df), function(i) {
    mean_i <- df[i, "mean"]
    se_i <- df[i, "standard_error"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  
  # merge data together and then adjust it (log space makes these adjustments simple addition/subtraction)
  if(risk=="diet_transfat"){all_cvs <- c("cv_sales")}  #just for quick euromonitor test
  risk_data_with_preds <- merge(df, coefs, by=c(all_cvs, "sex_id"), all.x=TRUE )
  
  message("woo! we have merged on the coefs")
  #print(head(risk_data_with_preds[type=="dr", .(pred_mean, mean_log, type)]))
  
  if(risk!="diet_salt"){
  if(nrow(risk_data_with_preds[is.na(pred_mean) & type!="dr"]) > 0){ 
    #print(head(risk_data_with_preds[is.na(pred_mean) & type!="dr"]))
    message( "STOP! There are some types of data that don't have a coefficient to match with, labeling these rows with no_coef_to_adjust & is_outlier=1 ")
    risk_data_with_preds[is.na(pred_mean) & type!="dr", no_coef_to_adjust:=1]
    risk_data_with_preds[is.na(pred_mean) & type!="dr", is_outlier:=1]
  }}
  if(risk=="diet_salt"){
    if(nrow(risk_data_with_preds[is.na(pred_mean) & type!="usodium"]) > 0){ 
      message( "STOP! There are some types of data that don't have a coefficient to match with, labeling these rows with no_coef_to_adjust & is_outlier=1 ")
      risk_data_with_preds[is.na(pred_mean), no_coef_to_adjust:=1]
      risk_data_with_preds[is.na(pred_mean), is_outlier:=1]
    }
  }
  
  risk_data_with_preds[, mean_log_adj:= mean_log-pred_mean]
  risk_data_with_preds[, var_log_adj:= se_log^2+pred_se^2]
  risk_data_with_preds[, se_log_adj:= sqrt(var_log_adj)]
  #only adjust if data is not gold standard (diet recall)

  if(risk=="diet_salt"){
      risk_data_with_preds[ cv_cv_urinary_sodium==1, mean_log_adj:=mean_log]
      risk_data_with_preds[ cv_cv_urinary_sodium==1, se_log_adj:= se_log]
  }else{
      risk_data_with_preds[ cv_dr==1, mean_log_adj:=mean_log]
      risk_data_with_preds[ cv_dr==1, se_log_adj:= se_log]
  }
  
  risk_data_with_preds[, lo_log_adj:=mean_log_adj - 1.96*se_log_adj]
  risk_data_with_preds[, hi_log_adj:=mean_log_adj + 1.96*se_log_adj]
  #convert to normal space
  risk_data_with_preds[, mean_adj:=exp(mean_log_adj)]
  risk_data_with_preds[, lo_adj:=exp(lo_log_adj)]
  risk_data_with_preds[, hi_adj:=exp(hi_log_adj)]
  
  message("Beginning second delta method")
  risk_data_with_preds$se_adj <- sapply(1:nrow(risk_data_with_preds), function(i) {
    ratio_i <- risk_data_with_preds[i, "mean_log_adj"]
    ratio_se_i <- risk_data_with_preds[i, "se_log_adj"]
    deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
  })
  
  message("close!")
  crosswalked_risk_data <- copy(risk_data_with_preds)
  
  #clean and change names just a bit to make them easy input into stgpr 
  crosswalked_risk_data[is.na(variance), variance:=standard_error^2]
  drop_cols <- c("mean_log","se_log","pred_mean","pred_se","mean_log_adj","var_log_adj","se_log_adj","lo_log_adj","hi_log_adj","standard_error", "sex")
  crosswalked_risk_data[, c(drop_cols):=NULL]
  setnames(crosswalked_risk_data, c("mean","variance","mean_adj","lo_adj","hi_adj","se_adj"), c("orig_data", "orig_variance", "mean", "mean_lower","mean_upper", "standard_error"))
  crosswalked_risk_data[, variance:=standard_error^2]

  crosswalked_risk_data[ sex_id==1, sex:="Male"]
  crosswalked_risk_data[sex_id==2, sex:="Female"]
  
  # need to keep seq the whole way through! 
  if(risk!="diet_transfat"){
    setcolorder(crosswalked_risk_data, c("ihme_risk", "modelable_entity_id","nid", "location_id", "location_name", "year_id","age_start","age_end","age_group_id","sex","sex_id","mean","standard_error","mean_lower","mean_upper","variance","sample_size","svy","type","parameter_type","source_type"))
  }
  
  return(crosswalked_risk_data)
  
}

do_everything <- function(risk, mrbrt_version, output_dir="FILEPATH", do_data_prep=TRUE, run_mrbrt=TRUE, adjust_data=TRUE, bundle_prep=TRUE){
  all_diet_data <- fread("FILEPATH/all_diet_data_post_as_2019_3.csv")   #using this to generate the crosswalk coefficients
  setnames(all_diet_data, "val", "mean")
  
  mrbrt_dir <- paste0(output_dir, "/fit_crosswalk/",mrbrt_version)
  adjusted_dir <- paste0(output_dir, "/adjust_data/",split_data_version)
  if(risk=="diet_zinc"){ adjusted_dir <- paste0(output_dir, "/adjust_data/resub/") 
  }
  
  if( sum(!risk %in% all_diet_data$ihme_risk )>0 ){ stop("Risk is not a valid risk")}
  
  if(!dir.exists(mrbrt_dir)){dir.create(mrbrt_dir)}
  if(!dir.exists(adjusted_dir)){dir.create(adjusted_dir)}
  if(!dir.exists(paste0(mrbrt_dir, "/coefficients/"))){dir.create(paste0(mrbrt_dir, "/coefficients/")) }
  if(!dir.exists(paste0(mrbrt_dir, "/input_files/"))){dir.create(paste0(mrbrt_dir, "/input_files/")) }  
  if(!dir.exists(paste0(adjusted_dir, "/bundle_prep/"))){dir.create(paste0(adjusted_dir, "/bundle_prep/")) }
  if(!dir.exists(paste0(adjusted_dir, "/compiled_prep/"))){dir.create(paste0(adjusted_dir, "/compiled_prep/")) }
  
  
  #step 1- prep data
  if(do_data_prep){
    input_data_dir <- paste0(mrbrt_dir,"/input_files/")
    data <- match_data(risk)
    write.csv(data, paste0(input_data_dir, risk, ".csv"), row.names=FALSE )
    message(paste0("Data has been prepped for ", risk, " and saved to: ", input_data_dir))
  }
  
  if(run_mrbrt){
    message("Beginning mr-brt analysis....")
    #step 2- fit mrbrt and save coefficients
    fit_diet_crosswalks(risks_to_brt=risk, use_dir=mrbrt_dir, version=mrbrt_version, save_coefs = TRUE)
      }
  
  #step 3- adjust non gold-standard data 
  # using the predict_data_prep function pulls in data from the "new compiled" set. Probably want to use bundle versions here instead 
  if(adjust_data){
    coef_file <- paste0(mrbrt_dir, "/coefficients/", risk,".csv")
    if(risk=="diet_transfat"){ coef_file <- "FILEPATH/final_transfat_crosswalk_coefs.csv"
    message("using the specific transfat coef file")}
    
    if(bundle_prep){
      data <- predict_bundle_version_prep(risk)

    }else{data <- predict_data_prep(risk)}
    write.csv(data, "FILEPATH/prepped_xwalk_data.csv", row.names = FALSE)
    message("Adjusting data using mr-brt coefficients")
    output_data <- implement_diet_crosswalk(df=data, coef_file_name = coef_file)
    message("Saving data real quick!")
    write.csv(output_data, "FILEPATH/xwalked_data.csv", row.names = FALSE)
    message(paste0(adjusted_dir, "/compiled_prep/"))
    if(bundle_prep){
      output_data[, unit_value_as_published:=as.numeric(unit_value_as_published)]
      output_data[, unit_value_as_published:=1]
      output_data[ is.na(crosswalk_parent_seq) & ( orig_data!= mean |  was_both_sex==1), crosswalk_parent_seq:=seq]
      output_data[orig_data!= mean |  was_both_sex==1, seq:=NA]
      setnames(output_data, "mean", "val")
       openxlsx::write.xlsx(output_data, paste0(adjusted_dir, "/bundle_prep/", risk, ".xlsx"), sheetName="extraction")
    }else{
      write.csv(output_data, paste0(adjusted_dir, "/compiled_prep/", risk, "_v", mrbrt_version, ".csv"), row.names = FALSE)
      }

    return(output_data)
    message(paste0("Congrats! adjusted data and coefficients have been prepped for ", risk, " and saved to: ", adjusted_dir))
    message("------------------------------------------------------------------------------------------------------------------")
  }
  }


######################################################################################
# Part 2: #Do everything for the risk
######################################################################################


#do_everything(risk, mrbrt_version = mrbrt_version, do_data_prep = dp, output_dir= dir, adjust_data=adjdata, run_mrbrt = rbrt, bundle_prep = TRUE)
do_everything(risk, mrbrt_version = mrbrt_version, do_data_prep = dp, output_dir= dir, adjust_data=adjdata, run_mrbrt = rbrt, bundle_prep = FALSE)

message("Done")

