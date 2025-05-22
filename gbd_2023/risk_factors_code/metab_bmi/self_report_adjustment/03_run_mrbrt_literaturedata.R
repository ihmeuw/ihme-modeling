##### Get crosswalk adjustment values for self-report literature data
##### Use collapsed datasets to calculate

#####################
## Set up
#####################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
repo <- paste0("FILEPATH", user, "/") 

save_dir <- "FILEPATH" 

dbd_func_dir <- "FILEPATH" 
dbd_funcs <- c('cluster_tools', 'version_tools')
for(dbd_func in dbd_funcs) source(sprintf('%s/%s.R', dbd_func_dir, dbd_func))
library(ini)

## Load configuration for meta data variables
load_config(paste0(code_dir, "FILEPATH"), c('gbd', 'qsub', 'bmi'))

library(reticulate)
reticulate::use_python("FILEPATH")
cw <- reticulate::import("crosswalk")

invisible(sapply(list.files("FILEPATH" , full.names = T), source))
library(openxlsx)
library(dplyr)
library(plyr)
library(assertable)
library(gtools)
library(ggplot2)
library(splines)
library(stringr)
library(tidyr)
library(lme4)
source("FILEPATH")
source("FILEPATH/most_recent_date.R")
source("FILEPATH")

## Create binary variables for crosswalk
encode_one_hot <- function(df, col, reference_cat){
  
  if(!is.character(df[, get(col)])) {
    
    message(sprintf("Converting %s to character variable to generate factors", col))
    df[, paste0('copy_', col) := as.character(get(col))]
    
  } else{
    df[, paste0('copy_', col) := as.character(get(col))] 
  }
  
  encoded <- data.table(unique(df[, get(paste0('copy_', col))])) %>% setnames(., names(.), paste0('copy_', col))
  
  for(lev in encoded[, get(paste0('copy_', col))]) encoded[, paste0(col,'_', lev) := as.integer(get(paste0('copy_', col)) == lev)]
  
  if(!is.character(reference_cat)) ref_col <- grep(sprintf('_%d$', reference_cat), names(encoded), value = T)
  if(is.character(reference_cat)) ref_col <- grep(sprintf('_%s$', reference_cat), names(encoded), value = T)
  encoded[, (ref_col) := NULL]
  
  encoded <- merge(df[, c(col, paste0('copy_', col)), with = F], encoded, by = paste0('copy_', col))
  encoded[, paste0('copy_', col) := NULL]
  df[, paste0('copy_', col) := NULL]
  
  return(encoded)
  
}

## Helper datasets
ages <- get_age_metadata(age_group_set_id=age_group_set_id, release_id=release_id)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages$age_start <- round(ages$age_start)
ages$age_end <- round(ages$age_end)
ages[age_end > 1 & age_end %% 5 == 0, age_end := age_end - 1][, age_mid := .5*(age_start + age_end)]
locs <- get_location_metadata(location_set_id, release_id=release_id)
locs[, country_name := gsub("_.*","", ihme_loc_id)]
country_locs <- locs[level==3]

## Filepaths to collapsed prevalence data
path_to_data <- c("FILEPATH","FILEPATH")
path_to_data <- lapply(path_to_data, paste0, c("prevalence_all","proportion_ob","proportion_suw")) %>% unlist()
path_to_data <- list.files(path_to_data, full.names=T, recursive=F, pattern=".csv") 

## get most recent mean BMI data
mean <- list.files("FILEPATH", pattern= "BUNDLE_ID")
mean_data <- fread(paste0("FILEPATH",max(mean)))

##proportion data
for(prop in c("proportion_ob","proportion_suw")){
  paths <- path_to_data[grepl(prop, path_to_data)]
  df <- do.call(rbind.fill, lapply(paths, fread)) %>% as.data.table() ## combine all collapsed datasets for group
  if(prop=="proportion_ob") df <- df[grepl("obese",var)]
  if(prop=="proportion_suw") df <- df[grepl("severe_underweight",var)]
  ##save to global
  assign(gsub("proportion","prop",prop), df)
}

## Prevalence data
paths <- path_to_data[grepl("prevalence_all", path_to_data)]
prev <- do.call(rbind.fill, lapply(paths, fread)) %>% as.data.table() ## combine all collapsed datasets for group
##separate into suw, uw, ow, ob
for(cat in c("overweight","obese","underweight","severe_underweight","bmi")){
  ## keep rows that have correct prevalence data, self-reported
  df <- prev[grepl(cat,var)] 
  name <- ifelse(cat=="overweight","ow", ifelse(cat=="obese","ob", ifelse(cat=="underweight","uw",ifelse(cat=="severe_underweight","suw","bmi"))))
  if(cat!="bmi") assign(paste0("prev_",name), df)
  if(cat=="bmi") assign(paste0("mean_",name), df)
}

for(cat in c("prev_ow","prop_ob","prop_suw","prev_ob","prev_suw","prev_uw","mean_bmi")){ #
  if(grepl("ob",cat)) name <- "obese"
  if(grepl("suw",cat)) name <- "severe underweight"
  if(grepl("ow",cat)) name <- "overweight"
  if(grepl("uw",cat)) name <- "underweight"
  if(grepl("bmi",cat)) name <- "bmi"

  #Get dataset
  df <- get(cat)
  df <- df[grepl("rep", var)] ## only keep self-report values
  if(grepl("prev", cat)) df <- df[sample_size>=10] ## only keep if N>10
  if(grepl("prop", cat)) df <- df[sample_size>=5] ## only keep if N>5
  
  ## Add on important information
  df <- merge(df[,-c("age_group_id")], ages[, .(age_group_id, age_start, age_end, age_mid)], by = c("age_start", "age_end"), all.x=T)
  df <- merge(df[,-c("location_id","location_name")], locs[, .(ihme_loc_id, location_id, location_name, region_id, region_name, super_region_name, super_region_id, country_name)], by = c("ihme_loc_id"), all.x=T)
  df[, nid_id :=.GRP, by = .(nid)]
  
  ##only keep country level data
  df <- df[location_id %in% country_locs$location_id]
  
  ## Outlier using function
  df[, diagnostic:= "measured"][grepl("rep", var), diagnostic := "self-report"]
  df[, val:= mean]
  df <- merge(df, unique(mean_data[,c("nid","field_citation_value","year_id")]), all.x=T, by="nid")
  if(!grepl("bmi", cat)) df <- outlier_prevalence(df, cat, country_name=T)
  if(grepl("bmi", cat)) df <- outlier_mean_bmi(df, cat)

    # Find out the duplicated rows
    print(paste0("There are ", sum(duplicated(df[,-c("file_path")])), " rows of duplicate data in the overweight files. Removing them should change the number of rows from ",
                 nrow(df[,-c("file_path")]), " to ", nrow(df[,-c("file_path")])-sum(duplicated(df[,-c("file_path")])), "."))
    
    df <- distinct(df[,-c("file_path")]) %>% as.data.table()
  
  #logit transformation cannot handle 0 and 1 values. Offset these cases.
  mean_lower_offset <-  min(df$mean[df$mean!=0])/2
  mean_upper_offset <-  (1-max(df$mean[df$mean!=1]))/2
  df[mean == 0, mean := mean_lower_offset][mean == 1, mean := 1-mean_upper_offset]
  df[is.na(standard_error) | standard_error<0.001, standard_error := 0.2] ## make standard error slightly larger than largest SE in non-missing data
  df[(is.na(standard_error) | standard_error<0.001) & sample_size>50, variance := 0.1] ## smaller SE for larger sample sizes

  ## Combine age and location variables into combined
  df[, ages_grp := "2-20"][age_group_id %in% c(9:14), ages_grp := "20-50"][age_group_id %in% c(15:18), ages_grp := "50-70"][age_group_id %in% c(19:20, 30:32,235), ages_grp := "70-125"]
  df[, reg_grp :=.GRP, by = .(region_id)]
  df[, ages_reg := paste(ages_grp, region_id, sep="_")]
  
  one_hot <- encode_one_hot(df, 'ages_reg', "20-50_100") %>% unique
  df <- Reduce(function(x, y){
    col_names <- c(names(x), names(y))
    common_col <- col_names[duplicated(col_names)]
    print(common_col)
    merge(x, y, by = common_col, all.x = T)
    
  }, list(df, unique(one_hot)))

  # transform val and standard_error into logit space for adjustment later on
  df[, c("logit_val", "logit_se")] <- as.data.table(cw$utils$linear_to_logit(mean = array(df$mean), sd = array(df$standard_error)))
  df[, variable:= "adj"][grepl("original",var), variable:="non_adj"]
  df <- unique(df)

  ## Issue will collapse with admin1 and admin2 that match previous level. Get rid of those rows, which appear as duplicated
  df <- df[order(-sample_size)] ##order df by sample size largest to smallest
  df[, n :=1:.N, by = .(nid, survey_name, ihme_loc_id, location_id, location_name, region_id, region_name, super_region_id, super_region_name, year_start, year_end,
                        survey_module, sex_id, age_group_id, variable)]
  df <- df[n==1]
  df$n <- NULL

  if(length(unique(df$variable)) > 2) stop("Dataset contains more than two referent categories.
                                                    Check to makes sure only one gold-standard and one non-gold standard are present.")

  ##reshape long to wide
  df <- as.data.frame(df)
  df2 <- pivot_wider(df[,c("nid","nid_id", "survey_name", "ihme_loc_id", "location_id", "location_name", "region_id", "region_name", "super_region_id", "super_region_name",
                           "year_start", "year_end", "survey_module", "sex_id", "age_start", "age_end", "age_group_id","mean","standard_error" ,
                           "variable", "logit_val", "logit_se", names(one_hot))],
                     names_from=variable, values_from = c("logit_val","logit_se","mean","standard_error" )) %>% as.data.table()
  df2[, `:=`(diff = logit_val_non_adj - logit_val_adj, diff_se = sqrt(logit_se_non_adj^2 + logit_se_adj^2))]
  
  mr_brt_df <- df2[,.(year_id = floor((year_start+year_end)/2)), by=c("nid", "nid_id", "diff", "diff_se", "age_group_id", "sex_id", "location_id", "region_id", 
                                                                      "super_region_id", names(one_hot))]
  
  ## Add in reference, alternative information
  mr_brt_df[, dorm_ref := "adj"][, dorm_alt := "non_adj"]

  all_data <- data.table()
  all_coeffs <- data.table()
  adj_comb <- data.table()
  df <- as.data.table(df)
  for(s in 1:2){
    ### Start crosswalk
    mr_brt_df_s <- mr_brt_df[sex_id==s]
    df_xwalk <- df[sex_id==s]
    
    missing <- setdiff(unique(one_hot[,c("ages_reg")]), unique(mr_brt_df_s[,c("ages_reg")]))
    covs_list <- list(cw$CovModel("intercept"), cw$CovModel("ages_reg_20-50_138"), cw$CovModel("ages_reg_2-20_138"), cw$CovModel("ages_reg_50-70_138"), cw$CovModel("ages_reg_20-50_96"), cw$CovModel("ages_reg_2-20_96"), cw$CovModel("ages_reg_50-70_96"),
                 cw$CovModel("ages_reg_70-125_96"), cw$CovModel("ages_reg_20-50_32"), cw$CovModel("ages_reg_2-20_32"), cw$CovModel("ages_reg_50-70_32"), cw$CovModel("ages_reg_70-125_32"), cw$CovModel("ages_reg_20-50_104"),
                 cw$CovModel("ages_reg_20-50_70"), cw$CovModel("ages_reg_2-20_70"), cw$CovModel("ages_reg_50-70_70"), cw$CovModel("ages_reg_70-125_70"), cw$CovModel("ages_reg_20-50_73"), cw$CovModel("ages_reg_2-20_73"), 
                 cw$CovModel("ages_reg_50-70_73"), cw$CovModel("ages_reg_70-125_73"), cw$CovModel("ages_reg_20-50_199"), cw$CovModel("ages_reg_2-20_199"), cw$CovModel("ages_reg_50-70_199"), cw$CovModel("ages_reg_70-125_199"),
                 cw$CovModel("ages_reg_20-50_159"), cw$CovModel("ages_reg_2-20_159"), cw$CovModel("ages_reg_50-70_159"), cw$CovModel("ages_reg_20-50_42"), cw$CovModel("ages_reg_2-20_42"), cw$CovModel("ages_reg_50-70_42"), 
                 cw$CovModel("ages_reg_70-125_42"), cw$CovModel("ages_reg_20-50_56"), cw$CovModel("ages_reg_2-20_56"), cw$CovModel("ages_reg_50-70_56"), cw$CovModel("ages_reg_70-125_56"), cw$CovModel("ages_reg_2-20_104"), 
                 cw$CovModel("ages_reg_50-70_104"), cw$CovModel("ages_reg_70-125_104"), cw$CovModel("ages_reg_20-50_120"), cw$CovModel("ages_reg_20-50_134"), cw$CovModel("ages_reg_2-20_134"), cw$CovModel("ages_reg_50-70_134"),
                 cw$CovModel("ages_reg_70-125_134"), cw$CovModel("ages_reg_20-50_192"), cw$CovModel("ages_reg_2-20_100"), cw$CovModel("ages_reg_50-70_100"), cw$CovModel("ages_reg_70-125_100"),
                 cw$CovModel("ages_reg_20-50_5"), cw$CovModel("ages_reg_2-20_5"), cw$CovModel("ages_reg_50-70_5"), cw$CovModel("ages_reg_70-125_5"), cw$CovModel("ages_reg_20-50_167"), cw$CovModel("ages_reg_2-20_167"), cw$CovModel("ages_reg_50-70_167"), 
                 cw$CovModel("ages_reg_70-125_167"), cw$CovModel("ages_reg_20-50_21"), cw$CovModel("ages_reg_20-50_124"), cw$CovModel("ages_reg_2-20_124"), cw$CovModel("ages_reg_50-70_124"), cw$CovModel("ages_reg_70-125_124"), 
                 cw$CovModel("ages_reg_20-50_174"), cw$CovModel("ages_reg_2-20_174"), cw$CovModel("ages_reg_50-70_174"), cw$CovModel("ages_reg_70-125_174"), cw$CovModel("ages_reg_2-20_120"), cw$CovModel("ages_reg_50-70_120"), 
                 cw$CovModel("ages_reg_70-125_120"), cw$CovModel("ages_reg_70-125_138"), cw$CovModel("ages_reg_2-20_21"), cw$CovModel("ages_reg_50-70_21"), cw$CovModel("ages_reg_70-125_21"), cw$CovModel("ages_reg_20-50_9"), 
                 cw$CovModel("ages_reg_2-20_9"), cw$CovModel("ages_reg_50-70_9"), cw$CovModel("ages_reg_70-125_9"), cw$CovModel("ages_reg_70-125_159"), cw$CovModel("ages_reg_20-50_65"), cw$CovModel("ages_reg_2-20_65"), cw$CovModel("ages_reg_50-70_65"), 
                 cw$CovModel("ages_reg_70-125_65"), cw$CovModel("ages_reg_2-20_192"), cw$CovModel("ages_reg_50-70_192"), cw$CovModel("ages_reg_70-125_192"))    
  
    ## If age region combination not in dataset drop from covs list
    drop <- NA
    if(nrow(missing)>0 | nrow(one_hot)<84){ 
      for(n in 1:length(covs_list)){ 
        for(nn in 1:nrow(missing)) if(grepl(missing[nn], covs_list[[n]]$cov_name)) drop <- append(drop, n) ## get list index for missing IDs
        if(!covs_list[[n]]$cov_name %in% names(mr_brt_df_s) & covs_list[[n]]$cov_name != "intercept") drop <- append(drop, n) ## get list index for IDs not present in the dataset
      }
      drop <- drop[-1] ## remove initial NA
      drop <- rev(drop) ## reverse list so higher numbers dropped first and index isn't changed for earlier numbers during process
      for(n in 1:length(drop)) covs_list[[drop[n]]] <- NULL
    }
  
    # format data for meta-regression; pass in data.frame and variable names
    dat1 <- cw$CWData(
      df = mr_brt_df_s,
      obs = "diff",       # matched differences in logit space
      obs_se = "diff_se", # SE of matched differences in logit space
      alt_dorms = "dorm_alt",   # var for the alternative def/method
      ref_dorms = "dorm_ref",   # var for the reference def/method
      covs = names(one_hot)[-1],         # list of (potential) covariate columns #
      study_id = "nid_id"          # var for random intercepts; i.e. (1|study_id) ## Use NID as group to determine between-study heterogeneity
    )

    # create crosswalk object called fit1. Meta regression model, data needs to be formatted by CWData function.
    fit1 <- cw$CWModel(
      cwdata = dat1,               # result of CWData() function call
      obs_type = "diff_logit",     # must be "diff_logit" or "diff_log"
      cov_models = covs_list 
    )
    fit1$fit()
    
    ## adjust without study_id
    preds1 <- fit1$adjust_orig_vals(
      df = df_xwalk,
      orig_dorms = "variable",
      orig_vals_mean = "mean",
      orig_vals_se = "standard_error" ,
      study_id = "nid_id" # optional argument to add a user-defined ID to the predictions;
    )

    df_xwalk[, c("meanvar_adjusted", "sdvar_adjusted",
                 "pred", "pred_se", "data_id")] <- preds1
    df_xwalk[, gamma := fit1$gamma]
    
    adj <- unique(df_xwalk[, c('location_id',"ihme_loc_id", 'region_id', 'super_region_id', 'age_group_id','variable', 'sex_id', 'pred',"pred_se","ages_grp","reg_grp","ages_reg" )])
    adj[,`:=` (category=cat)]
    adj_comb <- rbind(adj_comb, adj, fill=T)

    # pull coefficients and other variables from crosswalk object, save to all_coeffs
    ## For each age group and CF, amount of bias compared to gold (in logit space)
    df_result <- data.table(sex_id=s, category=cat, fit1$create_result_df())
    df_result <- df_result[!(cov_names!="intercept" & beta==0 & beta_sd==0)]
    all_coeffs <- rbind(all_coeffs, df_result, fill = T)
  }
  ##save to global
  # assign(paste0(cat,"_adj"), all_data)
  assign(paste0(cat,"_coeffs"), all_coeffs)
  assign(paste0(cat,"_adj"), adj_comb)
  
  ## get all age and region combinations
  adj_comb2 <- unique(adj_comb[variable=="non_adj", -c("location_id","ihme_loc_id","ages_grp","age_group_id","region_id","super_region_id","reg_grp")])
  ages_grouped <- unique(df[,c("ages_grp","age_group_id")])
  regs_grouped <- unique(df[,c("reg_grp","region_id")])
  sex_grouped <- unique(df[,c("sex","sex_id")])[!is.na(sex)]
  all_comb <- tidyr::crossing(ages_grouped, regs_grouped) %>% as.data.table()
  all_comb <- tidyr::crossing(all_comb, sex_grouped) %>% as.data.table()
  all_comb[, ages_reg := paste(ages_grp, region_id, sep="_")]
  all_comb <- merge(all_comb, adj_comb2, by=c("ages_reg","sex_id"), all=T)
  all_comb <- all_comb[, -c("ages_reg","ages_grp","reg_grp")]
  
  fwrite(all_comb, paste0("FILEPATH/xwalk_", cat,"_lit_adj_", Sys.Date(),".csv"))
  fwrite(all_coeffs, paste0("FILEPATH/xwalk_", cat,"_lit_coeffs_", Sys.Date(),".csv"))

}
