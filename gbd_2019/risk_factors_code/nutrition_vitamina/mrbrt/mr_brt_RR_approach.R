################################################################################
## DESCRIPTION ##  Do Vitamin A RR analysis

################################################################################

rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

code_dir <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, '/primer.R'))

library(ggplot2)
library(data.table)
library(openxlsx)
library(reshape2)
library(msm)
library(dplyr)
library(plyr)

# source
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_model_results.R")
source("/FILEPATH/run_mr_brt/mr_brt_functions.R")
source("/FILEPATH/run_mr_brt/cov_info_function.R")
# 

#---------------------------------
### Set paths
#---------------------------------
my_dir <- "/FILEPATH/"
versions <- c("cochrane", "extracted","hybrid")

update_data_files <- TRUE

label_add <- ""


#---------------------------------
### Step 1: Update extraction sheets and save combo
#---------------------------------

if(update_data_files){
  extraction_sheet <- as.data.table(read.xlsx("/FILEPATH/cc_vitaminA_extractions.xlsx", sheet = "extraction"))
  extraction_sheet[outcome=="Diarrheal diseases", outcome:="Diarrhea"]
  extraction_sheet[outcome=="Lower respiratory infections", outcome:="LRTI"]
  extracted_data <- extraction_sheet[!is.na(nid) & outcome %in% c("Diarrhea", "Measles", "LRTI")]
  extracted_data[outcome_type=="Incidence", cv_incidence:=1]
  extracted_data[outcome_type=="Mortality", cv_incidence:=0]
  extracted_data[, data:="ihme_extracted"]
  extracted_data <- extracted_data[!is.na(upper) & !is.na(lower)]   
  save_vals <-  c("nid","outcome","effect_size","upper","lower", "cv_incidence","year_start_study","year_end_study", "field_citation_value", "data")
  
  extracted_data <- extracted_data[!(nid %in% c(165677,165678,165685))] #seroconversion trials
  
  write.csv(extracted_data[, save_vals, with=FALSE], "/FILEPATH/Vitamin_A_CC_extracted.csv", row.names = FALSE)
  extracted_data[, field_citation_value:=NULL]
  
  ##Pull in the cochrane data
  cochrane_vals <- fread("/FILEPATH/Cochrane_reported_vals.csv")
  cochrane_vals <- cochrane_vals[ nid %in% c(165677,165678,165685), add_in_for_combo:=0] #seroconversion trials
  
  
  #########################################
  
  
  cochrane_vals_append <- cochrane_vals[add_in_for_combo==1,]
  save_vals <-  c("nid","outcome","effect_size","upper","lower", "cv_incidence", "data")
  hybrid_data <- rbind(extracted_data[,save_vals,with=FALSE], cochrane_vals_append[,save_vals,with=FALSE], fill=TRUE)
  write.csv(hybrid_data, "/FILEPATH/Hybrid_approach_data.csv", row.names = FALSE)
  
  for( out in c("Diarrhea","Measles","LRTI")){
  #for( out in c("Measles")){
    for( v in versions){
      message(v)
      if(v=="cochrane"){
        data <- fread("/FILEPATH/Cochrane_reported_vals.csv") 
        data <- data[!(nid %in% c(165677,165678,165685))] #seroconversion trials
        
        }
      if(v=="extracted"){
        data <- fread("/FILEPATH/Vitamin_A_CC_extracted.csv")
        }
      if(v=="hybrid"){
        data <- fread("FILEPATH/Hybrid_approach_data.csv")
        }
      
      save_dir <- paste0(my_dir,"/", v)
      dir.create(save_dir)
      
      data[, effect_size:=as.numeric(effect_size)]
      data[, upper:=as.numeric(upper)]
      data[, lower:=as.numeric(lower)]
      data <- data[outcome==out]
      data$log_effect_size <- log(data$effect_size)*1   #keep in harmful space
      data$log_se <- (log(data$upper)-log(data$lower))/3.92
      data$intercept <- 1
      keep_vars <- c("nid","outcome","log_effect_size","log_se","intercept", "cv_incidence", "data")
      data <-data[log_se!=Inf]  
      data <- data[,keep_vars,with=FALSE]
      write.csv(data, file=paste0(save_dir,"/",out, "_data_file.csv"), row.names = FALSE)
    }
  }
}
#---------------------------------
### Step 2: run MRBRT
#---------------------------------

x_cov <- "cv_incidence"   #choose one effective_dose or vad_prev

for(version in versions){
  data_dir <- paste0(my_dir, "/", version)
  
  for(i in c(1,2)){
  
    if(i==1){include_xcov <- FALSE}
    if(i==2){include_xcov <- TRUE}
for( out in c("Diarrhea","Measles","LRTI")){

  data_file <- paste0(data_dir,"/",out, "_data_file.csv")
  data <- fread(data_file)

  if(include_xcov){label <- paste0(out, "_", x_cov, label_add)
  }else{label <- paste0(out, label_add)}
  
  
  #only keep bias cov with variation across ro pair datapoints
  bias_covs <- c("representative","exposure_1","exposure_2","exposure_3","outcome_1","outcome_2","confounder_1","confounder_2")
  active_bias_covs <- c()
  for(cov in bias_covs){
    if(nrow(unique(data[,.(get(cov))]))> 1){
      active_bias_covs <- c(active_bias_covs, cov)
    }
  }
  active_bias_covs <- c()
  
  
  if(length(active_bias_covs)>0){
    if(include_xcov){cov_list <- list(rbind(cov_info(active_bias_covs, "Z"), cov_info(paste(x_cov), "X")))
    }else{cov_list <- list(cov_info(active_bias_covs, "Z"))}
    
  }else(
    if(include_xcov){cov_list <- list(cov_info(paste(x_cov), "X"))
    }else{cov_list <- list()}
  )
  
  fit1 <- run_mr_brt(
    output_dir = paste0(data_dir),
    model_label = label,
    data = data_file,
    mean_var = "log_effect_size",
    se_var = "log_se",
    #remove_x_intercept = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1,
    study_id = "nid",
    covs = cov_list,    
    overwrite_previous = TRUE
  )
  
  plot_mr_brt(fit1)
  
  # now to get the p-values and effect sizes:
  #for the dichotomous iteration we predict out draws and the p-value is the proportion below 0
  
  if(include_xcov){
    new_data <- data.table("intercept"=c(1), "x_cov"=c(0,1))
    setnames(new_data, "x_cov", x_cov)
    pred1 <- predict_mr_brt(fit1, newdata=new_data)
  }else{
      pred1 <- predict_mr_brt(fit1, newdata = data.table("intercept"=c(1)))
      }
  
}
  
  }
}
#---------------------------------
## Step 2: summarize the predicted p-values
#---------------------------------
all_version_summaries <- data.table()
for(version in versions){
  message(version)
data_dir <- paste0(my_dir,"/", version)
options <- list.dirs(data_dir)
options <- options[-1]
all_model_summaries <- data.table()
print(options)
for(v in options){
  name <- gsub(paste0(data_dir,"/"),"",v)
  coefs <- fread(paste0(v, "/model_summaries.csv"))
  coefs$outcome <- name
  all_model_summaries <- rbind(all_model_summaries, coefs, fill=TRUE)
  all_model_summaries[, model:=version]
}

  all_model_summaries[, mean_exp := exp(Y_mean_fe)]
  all_model_summaries[, mean_lower_exp := exp(Y_mean_lo_fe)]
  all_model_summaries[, mean_upper_exp := exp(Y_mean_hi_fe)]

  all_model_summaries[, A:=round(mean_exp, digits=2)]
  all_model_summaries[, B:=round(mean_lower_exp, digits=2)]
  all_model_summaries[, C:=round(mean_upper_exp, digits=2)]
  all_model_summaries[, summary:= paste0(A, " (", B ,",", C, ")")]
  all_model_summaries[, c("A","B","C"):=NULL]

write.xlsx(all_model_summaries, paste0(data_dir, "/summary_modeled_results.xlsx"))
all_version_summaries <- rbind(all_version_summaries, all_model_summaries, fill=TRUE)
}  
write.csv(all_version_summaries, paste0(my_dir, "/Summary_results.csv"), row.names = FALSE)

