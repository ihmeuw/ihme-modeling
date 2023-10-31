#####################################################
#   TITLE     crosswalk new hearing data
#   AUTHOR    NAME
#   DATE      DATE
#   PURPOSE   prep new hearing survey data by:
#             1- age-sex splitting data
#             2- sex-splitting data
#             2- crosswalking the data
#             3- age-splitting the data
#             4- appending it to the previous step-4 crosswalk version
#             5- saving a crosswalk version
#####################################################
# setup ----
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}
user <- Sys.info()["user"]
date <- gsub("-","_",Sys.Date())
draws <- paste0("draw_", 0:999)

# packages  ----
library(data.table)
library(openxlsx)
library(dplyr)
library(msm)
library(gtools)
library(haven)
library(mlr,lib.loc = "FILEPATH")
library(ParamHelpers,lib.loc = "FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = "FILEPATH")
library(msm)
library(Hmisc, lib.loc = paste0("FILEPATH")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")

mr_brt_dir <- "FILEPATH"
source(paste0(mr_brt_dir, "cov_info_function.R"))
source(paste0(mr_brt_dir, "run_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_outputs_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_outputs_function.R"))
source(paste0(mr_brt_dir, "predict_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_preds_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_preds_function.R"))
source(paste0(mr_brt_dir, "plot_mr_brt_function.R"))

# filepaths ----
user <- Sys.info()["user"]
date <- gsub("-","_",Sys.Date())
hxwalk_temp <- 'FILEPATH'
plot_dir<-  'FILEPATH'
sex_split_dir<- 'FILEPATH'
sex_split_dir<- 'FILEPATH'
flat_dir<- paste0('FILEPATH', date)
dir.create(flat_dir)

out_dir <- paste0(hxwalk_temp,"FILEPATH")
combo_path<- 'FILEPATH' #unique combos of alt:ref
repo_dir <- "FILEPATH"
mr_brt_dir <- "FILEPATH"

# functions ----
functions_dir <- "FILEPATH"
functs <- c('get_location_metadata', 'get_population','get_age_metadata', 
            'get_ids', 'get_outputs','get_draws', 'get_cod_data',
            'get_bundle_data', 'upload_bundle_data', 'get_bundle_version', 'save_bulk_outlier',
            'save_bundle_version', 'get_crosswalk_version', 'save_crosswalk_version')
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

source('FILEPATH/predict_mr_brt_function_for_hearing.R') #for crosswalk
source(paste0(repo_dir, "predict_mr_brt_function.R")) #for sex split
source('FILEPATH/age_split_2020.R') #for age split
source("FILEPATH")


# objects ----
hearing<- data.table(read.csv(paste0('FILEPATH')))

combos <- data.table(read.xlsx(combo_path))
combos<- combos[name_alt!= name_ref]
draws <- paste0("draw_", 0:999)


# these are the severity-specific bundles that will be prepped
sevs<- c('none', 'mild', '35+', 'moderate', 'moderately_severe', 'severe', 'profound', 'complete')
hearing[cause_name %in% sevs, bundle] #for list of bundle ids in order

bundle<- 379 
bundle_id<- bundle 



save_hearing_bv<- function(bundle_id){
    print(paste0('saving bundle version for bundle ', bundle_id))
    result<-save_bundle_version(bundle_id, decomp_step= 'iterative', gbd_round_id= 7)
    bv_id<-result$bundle_version_id
    hearing[bundle== bundle_id, bundle_version_id:= bv_id]
  # write the updated map to a flat file
  write.csv(hearing, paste0('FILEPATH'))
}


# prep new hearing data ----
process_hearing_data<- function(bundle){
  bundle_id<- bundle

  # get identifying information ----
  ref_thresh<- hearing[bundle== bundle_id, ref_thresh]
  bv_id<- hearing[bundle== bundle_id, bundle_version_id]
  id<- hearing[bundle== bundle_id, me]
  print(paste0('preparing data for reference threshold: ', ref_thresh, ' bundle: ', bundle))
  
  #get bundle version
  bv<-get_bundle_version(bv_id, fetch= 'all')
  
  
  # fill out cases SE SS  ----
  to_xwalk<- bv
  mgd_ext <- copy(to_xwalk)
  mgd_ext <- get_cases_sample_size(mgd_ext)
  mgd_ext <- get_se(mgd_ext)
  mgd_ext <- calculate_cases_fromse(mgd_ext)
  mgd_ext[, crosswalk_parent_seq:= NA]
  
  # age-sex split ----
  print(paste0('age-sex splitting bundle ', bundle))
  total <- age_sex_split(mgd_ext) 

  unique(total$note_modeler)

  as_dir <- paste0(hxwalk_temp, "FILEPATH", date, "_")
  write.xlsx(total, file = paste0(as_dir, "FILEPATH"), sheetName = "extraction")
  
  # apply previous sex splits to new input data ----
  print('sex splitting data')
  source(paste0(repo_dir, "predict_mr_brt_function.R")) #for sex split
  if (ref_thresh %in% c('20_34', '35_49', '50_64', '65_79', '80_94')) {
    ss_date<- 'DATE'
  } else if (ref_thresh== '0_19') {
    ss_date<- 'DATE'
    } else ss_date<- 'DATE'
  
  hmodel_name <- paste0("tr_", ref_thresh, "_", ss_date)
  sex_model <- paste0(sex_split_dir, hmodel_name,"/")
  predict_sex <- split_data_from_dir(total, sex_model) 
  # split the data
  split <- predict_sex$graph \
  total <- predict_sex$final 
  diff<- setdiff(bv$nid, dt$nid)
  

  # save to directory
  write.xlsx(predict_sex$graph, file = paste0(flat_dir, "FILEPATH "), sheetName = "extraction")
  write.xlsx(predict_sex$final, file = paste0(flat_dir, "FILEPATH"), sheetName = "extraction")
  
  # graph split
  pdf(paste0(flat_dir, "/", hmodel_name, "_", ref_thresh, "FILEPATH"))
  graph_predictions(dt = split, ref_thresh= ref_thresh)
  dev.off()
  
  
  # apply crosswalks ----
  print(paste0('crosswalking data for ref thresh', ref_thresh))
  source('FILEPATH') #for crosswalk
  
  dt <- copy(total)
  ref<- ref_thresh
  dt[, ref_thresh:= as.character(ref_thresh)]
  dt[, ref_thresh:= ref]
  dt[!is.na(thresh) & (thresh != ref_thresh), alt_def := 1]
  dt[is.na(alt_def), `:=` (alt_def = 0)]

  mgd_adjust <- dt[alt_def == 1 & mean != 0]
  mgd_adjust[mean == 1, `:=` (mean = 0.9986, cases = NA)]  
  mgd_noadjust <- dt[!(alt_def %in% c(1))  | mean  == 0, ] 
  mgd_adjust[, mean_logit := logit(mean)]
  mgd_adjust[ ,mean_logit_se:=sapply(1:nrow(mgd_adjust), function(k){
        mean_i <- mgd_adjust[k, mean]
        mean_se_i <- mgd_adjust[k, standard_error]
        deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
      })]
  
  # crosswalk
  for (i in 1:nrow(combos)) {
        print("Checking that the right adjustment is being made...")

        if (ref_thresh == gsub("prev_","",combos$name_ref[i])) { #make sure you are adjusting alt for the correct threshold(some alts are crosswalked to multiple thresholds)
          
          alt_thresh <- gsub("prev_","", combos$name_alt[i])
          alt_colname <- paste0("alt_", alt_thresh)
          
          #create alt vars for adjustment
          print("Creating cv columns")
          mgd_adjust[ , paste0(alt_colname) := ifelse(thresh == alt_thresh, 1, 0)]
          
          #call in the crosswalk files
          job_name <- paste0("hxwalk_", combos[i, name_alt], "_to_", combos[i, name_ref])
          model_dir<- paste0(out_dir, job_name,"/")
          model <- readRDS(paste0(out_dir, job_name,"/",job_name, ".xlsx"))
          
          preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
          pred_dt <- as.data.table(preds$model_draws)
          pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
          pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
          pred_dt[, c(draws, "Z_intercept", "X_intercept") := NULL]
          
          adj_vlogit <- pred_dt$ladj
          adj_vlogit_se <- pred_dt$ladj_se
          print( paste("Adjusting", alt_colname, "by logit difference ", adj_vlogit))
          mgd_adjust[get(alt_colname)== 1, note_modeler := paste0(note_modeler, " | Adjusted ", alt_colname, " by logit difference ", adj_vlogit)]
          mgd_adjust[get(alt_colname)== 1 & !is.na(seq), crosswalk_parent_seq:= seq]
          mgd_adjust[get(alt_colname)== 1 & !is.na(seq), seq:= '']
          
          
          mgd_adjust[get(alt_colname) == 1, `:=` (adj_logit = adj_vlogit, #calc factor to adjust by
                                                  adj_logit_se = adj_vlogit_se)]
          
          mgd_adjust[get(alt_colname) == 1, `:=` (mean_adj_logit = mean_logit - adj_vlogit, #adjust mean in logit space
                                                  mean_adj_logit_se = sqrt(mean_logit_se^2 + adj_vlogit_se^2))]
          
          mgd_adjust[get(alt_colname) == 1, `:=` (prev_adjusted = exp(mean_adj_logit)/(1+exp(mean_adj_logit)), #transform adjusted mean back to normal space
                                                  lo_logit_adjusted = mean_adj_logit - 1.96 * mean_adj_logit_se, #calc CI in logit space
                                                  hi_logit_adjusted = mean_adj_logit + 1.96 * mean_adj_logit_se)]
          
          mgd_adjust[get(alt_colname) == 1, `:=` (lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)), #calc CI in normal space
                                                  hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted)) )]
          print(paste0('crosswalked row: ', i))
        }
      }
      print('finished crosswalking')
      crosswalked<- get_cases_sample_size(mgd_adjust)
      crosswalked <- get_se(mgd_adjust)
      crosswalked<- calculate_cases_fromse(mgd_adjust)
      
      #save a version for graphing
      all_adj <- copy(crosswalked)
      all_adj[ , `:=` (mean_orig = mean, lo_orig = lower, hi_orig = upper, mean_se_orig = standard_error)]
      all_adj[ , `:=` (mean_new = prev_adjusted, lower_new = lo_adjusted, upper_new = hi_adjusted, standard_error_new = mean_adj_logit_se, note_modeler = paste(note_modeler, " | adjusted with logit difference", adj_logit))]
      graph_fpath <- paste0(flat_dir,"FILEPATH")
      write.xlsx(x=all_adj, file = graph_fpath )
      
      # save a full version of the dataset for upload
      upload<- copy(crosswalked)
      upload[ , `:=` (mean = prev_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = mean_adj_logit_se, note_modeler = paste(note_modeler, " | adjusted with logit difference", adj_logit))]
      extra_cols <- setdiff(names(upload), names(total))
      upload <- upload[, c(extra_cols) := NULL]
      full_dt <- rbind(upload, mgd_noadjust, fill = T)
      full_fpath <- paste0(flat_dir,"/FILEPATH")
      print(full_fpath)
      write.xlsx(x=full_dt, file = full_fpath , sheetName = "extraction")
      
    
      
      # age-split data with age bin wider than 25 ----
      print('age splitting data')
      ages <- get_age_metadata(12, gbd_round_id= 6)
      setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
      age <- ages[age_start >= 0, age_group_id]
    
      dt <- copy(full_dt)
      df<- dt       # run this line if you want to run age-split line-by-line
      gbd_id <- id  # run this line if you want to run line-by-line
      
      
      final_dt <- age_split(gbd_id = id, df = dt, age , region_pattern = F, location_pattern_id = 1)
      age_split_fpath <- paste0(flat_dir,"FILEPATH") 
      write.xlsx(x=final_dt, file = age_split_fpath , sheetName = "extraction")

      # prepare for upload  ----
      print('preparing for upload')
      upload<- copy(final_dt)
      upload[is.na(upper), `:=` (lower = NaN, uncertainty_type_value = NA)]
      upload[!is.na(upper), uncertainty_type_value := 95]
      upload[measure == "prevalence" & !is.na(standard_error) & upper > 1, `:=` (upper = NaN, lower = NaN, uncertainty_type_value = NA)]
      upload[measure == "prevalence" & standard_error > 1 & !is.na(cases) & !is.na(sample_size), standard_error := NaN]
      upload<- upload[is.na(group_review) | group_review==1]
      upload[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq]
      upload[, seq:= '']
      
      
      setdiff(bv$nid, upload$nid)
     # if(nrow(upload)< nrow(bv)){
      #  stop('you dropped data')
      #}
      if(length(unique(upload$nid))< length(unique(bv$nid))){
        stop(paste0('you dropped these nids:', setdiff(bv$nid, upload$nid)))
      }
      # write prepped data to a filepath
      filepath<- paste0('FILEPATH')
      write.xlsx(upload, filepath, sheetName= 'extraction')
      print(paste0('you prepped the data for bundle ', bundle, ', now try to save a crosswalk version'))
}

filepath<- paste0('FILEPATH')
save_crosswalk_version(bundle_version_id= bv_id, filepath, description= 'outlier Peru Hubei Lagos Taiwan dropped redundant Brazil outliered Brazil')

