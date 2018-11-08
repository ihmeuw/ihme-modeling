###########################################################
### Author: 
### Date: 5/9/2018
### Project: Migraine Definite/Total XWalk
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, ggplot2, boot, MASS)
library(openxlsx, lib.loc = paste0(j_root, FILEPATH))
library(msm, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
repo_dir <- paste0(h_root, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
doc_dir <- paste0(j_root, FILEPATH)
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTION
source(paste0(functions_dir, "get_epi_data.R"))
source(paste0(functions_dir, "upload_epi_data.R"))
source(paste0(repo_dir, "data_processing_functions.R"))
source(paste0(functions_dir, "get_location_metadata.R"))

## GET MAP
map <- fread(paste0(repo_dir, "bundle_map.csv"))

## GET LOCATIONS
loc_dt <- get_location_metadata(location_set_id = 9)
super_region_dt <- copy(loc_dt[, .(location_id, super_region_name)])

## GET DATA
both <- get_epi_data(bundle_id = map[bundle_name == "migraine_adj", bundle_id])
def <- get_epi_data(bundle_id = map[bundle_name == "def_migraine_adj", bundle_id])

total <- rbindlist(list(both[!type==""], def[!type==""]), fill = T)
total[, midage := (age_start + age_end) / 2]
total <- merge(total, super_region_dt, by = "location_id")


total[, n_type := length(unique(type)), by = c("nid", "sex", "age_start", "measure")]
total <- total[n_type == 2] ## only get matched data

col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0(temp_dir, FILEPATH), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

## TRY CROSSWALK CODE
crosswalk_from_matched <- function(dt, matched_dt, ref, alts, measure_name, out_dir){
  
  library(plyr)
  library(data.table)
  library(ggplot2)
  
  ###################################
  ## PREP MATCHED X-WALK DATA
  ###################################
  
  ## Input data
  xwalk_data <- as.data.table(matched_dt)
  
  ## subset on measure
  xwalk_data <- xwalk_data[measure==measure_name,]
  
  ## add case_type column
  ## epi template already has case_name, case_definition, and case_diagnostics fields
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    xwalk_data[get(cv)==1, case_type:=t]
  }
  
  xwalk_data <- xwalk_data[,c("case_type","nid","age_start","age_end","sex","location_id","location_name","sample_size","mean")]
  
  ## log transform mean
  xwalk_data[,mean:=log(mean)]
  
  ## Reshape data wide by case type
  xwalk_data <- dcast.data.table(xwalk_data, nid + age_start + age_end + sex +location_id + location_name + sample_size ~ case_type, value.var = "mean")
  
  ## Generate age_mid variable (independent var in regression)
  xwalk_data[, age_mid := ((age_start+age_end)/2)]
  
  ####################################
  ## LOAD & PREP PRE-CROSSWALK BUNDLE
  ####################################
  
  dt <- as.data.table(dt)
  n <- nrow(dt)
  dt$id <- seq(1,n,1) ## set unique identifier 
  pre_data <- copy(dt)
  
  ## just crosswalk data with the measure you've specified
  pre_data <-pre_data[measure==measure_name,]
  
  ## add case_type column
  ## epi template already has case_name, case_definition, and case_diagnostics fields
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    pre_data[get(cv)==1, case_type:=t]
  }
  
  ## Add original unadjusted mean to note_modeler column for reference
  pre_data[case_type %in% alts & !is.na(note_modeler), note_modeler := paste0(note_modeler, " | unadjusted mean : ", mean)]
  pre_data[case_type %in% alts & is.na(note_modeler), note_modeler := paste0("unadjusted mean : ", mean)]
  
  ## Generate age_mid variable
  pre_data[, age_mid := (age_start+age_end)/2]
  
  ## save pre_data as post_data to preserve pre_data as unadjusted
  post_data <- copy(pre_data)
  
  ## drop mean=0 & log transform mean
  post_data <- post_data[mean>0,]
  post_data[, log_mean:=log(mean)]
  
  
  ####################################
  ## LOOP OVER ALT DEFINITION
  ####################################
  
  for(alt in alts){
    
    print(paste0("Crosswalking ", alt))
    
    #Subset xwalk data to rows with column entries for both ref and alt
    xwalk_subset <- xwalk_data[!is.na(get(ref)) & !is.na(get(alt)),]
    
    if(nrow(xwalk_subset)<10) print("WARNING: you have fewer than 10 rows to inform this crosswalk")
    
    # Perform regression and save as a function
    model <- lm(get(ref)~get(alt)+age_mid, data=xwalk_subset)
    regression <- summary(model)
    print(regression)
    
    # Apply regression to data & propagte uncertainty
    pred_data <- copy(post_data[type == c(alt)])
    pred_data <- pred_data[, .(age_mid, log_mean)]
    pred_data[, "(Intercept)":= 1]
    setnames(pred_data, "log_mean", "definite")
    predictions <- as.data.table(predict(model, pred_data, interval = "predict"))
    predictions[, `:=` (mean = exp(fit), lower = exp(lwr), upper = exp(upr))]
    predictions <- predictions[, .(mean, lower, upper)]
    predictions[upper > 1, upper := 1]
    
    post_data[, c("mean", "upper", "lower") := NULL]
    post_data[, uncertainty_type_value := 95]
    post_data <- cbind(post_data, predictions)
    post_data[case_type==alt, case_type:=paste0(alt,", crosswalked")]
    
  } # end loop over alts
  
  ## Remove lower and upper, epi uploader will recalculate based on new standard error
  post_data[mean>0,c("standard_error", "uncertainty_type",
                     "effective_sample_size", "cases", "sample_size"):=NA]
  
  ## Cap SE at 1 and remove SE if mean=0
  post_data[standard_error>=1, standard_error:=0.999]
  post_data[mean==0, standard_error:=NA]
  post_data <- post_data[mean!=0 | upper!=0,] ## remove if mean lower and upper are all zero
  post_data[, seq := ""]
  
  ## add uncrosswalked rows back onto data
  crosswalked_ids <- unique(post_data$id)
  pre_crosswalked_rows <- dt[(id %in% crosswalked_ids)]
  pre_crosswalked_rows[, `:=` (group_review = 0, note_modeler = paste0(note_modeler, " | parent data, has been crosswalked to both tth"), 
                               specificity = paste0(note_modeler, " | crosswalk parent"))]
  dt <- rbind.fill(post_data, pre_crosswalked_rows)
  dt <- as.data.table(dt)
  dt[, seq := ""]
  dt[,id:=NULL]
  
  ###################################################
  ## Generate plots of crosswalked data
  ###################################################
  sex <- dt[, unique(sex)]
  
  if(out_dir!=0){
    
    for(alt in alts){
      
      print(paste0("Generating ggplot for ",alt))
      
      pre_data_short <- pre_data[(case_type==alt|case_type==ref) & measure==measure_name ,
                                 c("location_id","nid","sex","age_mid","mean","case_type")]
      post_data_short <- post_data[(case_type==paste0(alt,", crosswalked") | case_type==ref) & measure==measure_name,
                                   c("location_id","nid","sex","age_mid","mean","case_type")]
      
      data <- rbind(pre_data_short,post_data_short)
      
      gg<-ggplot(data=data,aes(x=age_mid,y=mean,color=case_type)) + 
        geom_point(alpha=0.2) + 
        geom_smooth(method=loess) + 
        labs(y = "mean", x = "age") +
        theme_classic() +
        theme(legend.title = element_blank())
      pdf(paste0(out_dir,"/",alt,"_", sex, "_crosswalk.pdf"))
      print(gg)
      dev.off()
      
    }
    
  }
  
  ## return crosswalked data.table
  return(dt)
  
}


## SET UP ELEMENTS AND CROSSWALK
matched <- copy(total[is_outlier == 0 & group_review == 1])
matched[type == "definite", `:=` (cv_definite = 1, cv_both = 0)]
matched[type == "both", `:=` (cv_both = 1, cv_definite = 0)]
matched_series <- copy(matched[, .(nid, sex, location_id)]) ## get those with both def and unique so exclude from xwalk
matched_series <- unique(matched_series)
matched_series[, flag := 1]
def_dt <- copy(def[!type=="" & is_outlier == 0 & group_review == 1])
def_dt <- merge(def_dt, matched_series, by = c("nid", "sex", "location_id"), all.x = T)
def_dt <- def_dt[is.na(flag)]
def_dt[, flag := NULL]
def_dt[, `:=` (cv_definite = 1, cv_both = 0)]

##BY SEX
matchedmale <- copy(matched[sex == "Male"])
matchedfemale <- copy(matched[sex == "Female"])
defmale <- copy(def_dt[sex == "Male"])
deffemale <- copy(def_dt[sex == "Female"])
finalmale <- crosswalk_from_matched(dt = defmale, matched_dt = matchedmale, ref = "both", alts = "definite", 
                                measure_name = "prevalence", out_dir = doc_dir)
finalfemale <- crosswalk_from_matched(dt = deffemale, matched_dt = matchedfemale, ref = "both", alts = "definite", 
                                      measure_name = "prevalence", out_dir = doc_dir)
final <- rbind(finalmale, finalfemale, fill = T)
final <- col_order(final)

## UPLOAD
final[standard_error>1, standard_error := ""]
final[, c("cv_both", "cv_definite", "case_type", "age_mid", "log_mean") := NULL]
bundle_id <- map[bundle_name == "migraine_adj", bundle_id]
write.xlsx(final, paste0(FILEPATH), sheetName = "extraction")
upload_epi_data(bundle_id = bundle_id, filepath = paste0(FILEPATH))
