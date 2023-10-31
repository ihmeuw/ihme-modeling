###########################################################
### Project: Migraine Definite/Total XWalk
### Purpose: GBD 2019 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

pacman::p_load(data.table, ggplot2, boot, MASS)
library(readxl)
library(openxlsx, lib.loc = paste0("FILEPATH"))
library(msm, lib.loc = paste0("FILEPATH"))

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
step <- "step4"

## SOURCE FUNCTION
source(paste0("FILEPATH", "get_bundle_data.R"))
source(paste0("FILEPATH", "upload_bundle_data.R"))
source(paste0("FILEPATH", "save_bundle_version.R"))
source(paste0("FILEPATH", "get_bundle_version.R"))
source(paste0("FILEPATH", "save_crosswalk_version.R"))
source(paste0("FILEPATH", "get_crosswalk_version.R"))
source(paste0("FILEPATH", "data_processing_functions.R"))
source(paste0("FILEPATH", "get_location_metadata.R"))

## GET MAP
map <- fread(paste0("FILEPATH", "bundle_map.csv"))

## GET LOCATIONS
loc_dt <- get_location_metadata(location_set_id = 9)
super_region_dt <- copy(loc_dt[, .(location_id, super_region_name)])

## GET DATA
data <- get_crosswalk_version(1, export = F)
data <- as.data.table(data)

nids <- unique(data$nid)
both <- data[both_or_def == "both"]
def <- data[both_or_def == "def"]

total <- rbindlist(list(both[!type==""], def[!type==""]), fill = T)
total[, midage := (age_start + age_end) / 2]
total <- merge(total, super_region_dt, by = "location_id")

total[, n_type := length(unique(type)), by = c("nid", "sex", "age_start", "measure")]
unmatched_data <- total[n_type == 1] 
unmatched_data <- unmatched_data[both_or_def == "both"]
total <- total[n_type == 2] 

col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH", "upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  epi_order <- epi_order[!epi_order == ""]
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

## CROSSWALK CODE
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
  
  ## add uncrosswalked rows back onto data
  crosswalked_ids <- unique(post_data$id)
  pre_crosswalked_rows <- dt[(id %in% crosswalked_ids)]
  pre_crosswalked_rows[, `:=` (group = 1, group_review = 0, note_modeler = paste0(note_modeler, " | parent data, has been crosswalked to both migraine"), 
                               specificity = paste0(note_modeler, " | crosswalk parent"))]
  dt <- rbind.fill(post_data, pre_crosswalked_rows)
  dt <- as.data.table(dt)
  dt[, crosswalk_parent_seq := seq]
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
matched <- copy(total[is_outlier == 0]) #& group_review == 1
matched[type == "definite", `:=` (cv_definite = 1, cv_both = 0)]
matched[type == "both", `:=` (cv_both = 1, cv_definite = 0)]
matched_series <- copy(matched[, .(nid, sex, location_id)]) 
matched_series <- unique(matched_series)
matched_series[, flag := 1]
def_dt <- copy(def[!type=="" & is_outlier == 0]) 
def_dt <- merge(def_dt, matched_series, by = c("nid", "sex", "location_id"), all.x = T)
append_back <- matched[type == "both"]
def_dt <- def_dt[is.na(flag)]
def_dt[, flag := NULL]
def_dt[, `:=` (cv_definite = 1, cv_both = 0)]

##BY SEX
matchedmale <- copy(matched[sex == "Male"])
matchedfemale <- copy(matched[sex == "Female"])
defmale <- copy(def_dt[sex == "Male"])
deffemale <- copy(def_dt[sex == "Female"])
finalmale <- crosswalk_from_matched(dt = defmale, matched_dt = matchedmale, ref = "both", alts = "definite", 
                                measure_name = "prevalence", out_dir = "FILEPATH")
finalfemale <- crosswalk_from_matched(dt = deffemale, matched_dt = matchedfemale, ref = "both", alts = "definite", 
                                      measure_name = "prevalence", out_dir = "FILEPATH")
final <- rbind(finalmale, finalfemale, fill = T)
final <- col_order(final)
final_delete <- setdiff(names(final), names(unmatched_data))
final_delete <- final_delete[!(final_delete %in% c("group", "specificity", "group_review"))]
final[, paste(final_delete)] <- NULL
unmatched_delete <- setdiff(names(unmatched_data), names(final))
unmatched_data[, paste(unmatched_delete)] <- NULL
final2 <- rbind(unmatched_data, final, fill = T)
append_delete <- setdiff(names(append_back), names(final))
append_back[, paste(append_delete)] <- NULL
final_3 <- rbind(append_back, final2, fill = T)

final_3[, seq := ifelse(!(is.na(crosswalk_parent_seq)), "", seq)]
length(unique(final_3$nid))

## UPLOAD
bundle_version <- get_bundle_version(1, export = F, transform = F)
vers_seqs <- unique(bundle_version$seq)
del_seqs <- setdiff(unique(final2$seq), vers_seqs)
final2[seq %in% del_seqs, seq := ""]

final2[standard_error>1, standard_error := ""]
final2[, c("cv_both", "cv_definite", "case_type", "age_mid", "log_mean") := NULL]
final2[grepl("parent data, has been crosswalked to both", note_modeler) & group_review == 0, group := 1]
bundle_version_id <- 1
bundle_id <- map[bundle_name == "migraine_adj", bundle_id]
write.xlsx(final2, "FILEPATH", sheetName = "extraction")
path <- "FILEPATH"
final2 <- as.data.table(read_xlsx(path))
final2 <- final2[group_review == 1 | is.na(group_review)]
path <- "FILEPATH"
write.xlsx(final2, path, sheetName = "extraction")
save_crosswalk_version(bundle_version_id = bundle_version_id, path, description = "xwalked, definite to both xwalked")

## UPLOAD - DECOMP 4
bundle_version <- get_bundle_version(1, export = F, transform = F)
vers_seqs <- unique(bundle_version$seq)
del_seqs <- setdiff(unique(final_3$seq), vers_seqs)
del_seqs <- del_seqs[del_seqs != ""]
final_3[seq %in% del_seqs, seq := ""]

final_3[standard_error>1, standard_error := ""]
final_3 <- final_3[is.na(group_review) | group_review == 1]

path <- "FILEPATH"
write.xlsx(final_3, path, sheetName = "extraction")

result <- save_crosswalk_version(1, data_filepath = path, description = "def to both xwalk")
