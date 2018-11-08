##################################################################
## PREP & CROSSWALK NON-PARTNER DATA
## Include lifetime non-partner data with an age-specific xwalk
##################################################################

## SET-UP
library(data.table)
library(ggplot2)
library(dplyr)
library(tidyr)
library(plyr)
library("openxlsx", lib.loc = "FILEPATH")
library("readxl", lib.loc = "FILEPATH")

shared_functions_dir <- "FILEPATH"

source(paste0(shared_functions_dir,"/get_epi_data.R"))
source(paste0(shared_functions_dir,"/upload_epi_data.R"))
source(paste0(shared_functions_dir, "get_ids.R"))
source(paste0(shared_functions_dir, "get_demographics.R"))

## AGE TABLE
ages <- get_ids(table = "age_group")
dems <- get_demographics(gbd_team = "epi")$age_group_id
ages <- ages[age_group_id %in% dems & !age_group_id %in% c(2, 3, 4), ]
neonates <- data.table(age_group_name = c("0 to 0.01", "0.01 to 0.1", "0.1 to 0.997"), age_group_id = c(2, 3, 4))
ages <- rbind(ages, neonates, use.names = TRUE)
ages <- separate(ages, age_group_name, into = c("age_start", "age_end"), sep = " to ")
ages[, age_start := gsub(" plus", "", age_start)]
invisible(ages[age_group_id == 235, age_end := 99])
ages[, age_start := as.numeric(age_start)]
ages[, age_end := as.numeric(age_end)]


# CROSSWALK FUNCTION ------------------------------------------------------

crosswalk_from_microdata <- function(dt, xwalk_data, ref, alts, measure_name){

  xwalk_data <- as.data.table(xwalk_data)
  xwalk_data <- xwalk_data[measure==measure_name,]
  ## add case_type column
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    xwalk_data[get(cv)==1, case_type:=t]
  }
  xwalk_data <- xwalk_data[,c("case_type","nid","age_start","age_end","sex","location_id","location_name","sample_size","mean")]
  ## Reshape data wide by case definition
  xwalk_data <- dcast.data.table(xwalk_data, nid + age_start + age_end + sex +location_id + location_name + sample_size ~ case_type, value.var = "mean")
  ## Generate age_mid variable (independent var in regression)
  xwalk_data[, age_mid := ((age_start+age_end)/2)]
  ## load and prep pre-crosswalk data
  dt <- as.data.table(dt)
  n <- nrow(dt)
  dt$id <- seq(1,n,1)
  pre_data <- dt
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
  ## loop over alternate definition
  for(alt in alts){
    print(paste0("Crosswalking ", alt))
    #Subset xwalk data to rows with column entries for both ref and alt
    xwalk_subset <- xwalk_data[!is.na(get(ref)) & !is.na(get(alt)),]
    # by age calculate ratio ref:alt
    ratio <- xwalk_subset[,ratio:=get(ref)/get(alt)]
    # subset columns before merge
    ratio <- ratio[,c("age_mid","ratio")]
    post_data <- merge(post_data,ratio,by="age_mid")
    post_data[get(paste0("cv_",alt))==1 ,mean:=mean*ratio]
    post_data[get(paste0("cv_",alt))==1,c("lower", "upper", "uncertainty_type_value","effective_sample_size",	"cases", "sample_size",	"design_effect"):=NA]
    post_data[get(paste0("cv_",alt))==1,note_modeler:=paste0(note_modeler," | crosswalked from observed ratio")]
    post_data[get(paste0("cv_",alt))==1,case_type:=paste0(alt,", crosswalked")]
    post_data[get(paste0("cv_",alt))==1,group_review:=1]
    post_data[get(paste0("cv_",alt))==1,specificity:="post-crosswalk"]
    post_data[get(paste0("cv_",alt))==1,group:=1]
    post_data[,c("ratio"):=NULL]
  }
  ## add pre-crosswalked rows
  for(alt in alts){
    pre_crosswalked_rows <- dt[get(paste0("cv_",alt))==1]
    pre_crosswalked_rows[,group_review:=0]
    pre_crosswalked_rows[,specificity:="pre-crosswalk"]
    pre_crosswalked_rows[,group:=1]
    dt <- rbind(post_data,pre_crosswalked_rows,fill=T)
  }
  dt <- as.data.table(dt)
  dt[,id:=NULL]
  
  return(dt)
}


# USE CROSSWALK FUNCTION --------------------------------------------------

lastyr <- get_epi_data(645) %>% as.data.table
lifetime <- get_epi_data(771) %>% as.data.table

lastyr[,cv_recall_1yr:=1]
lastyr[,cv_recall_lifetime:=0]

lifetime[,cv_recall_1yr:=0]
lifetime[,cv_recall_lifetime:=1]
lifetime[,seq:=NA]

dt <- rbind(lastyr,lifetime,fill=T)

dt <- reorder_columns(dt,covs=c("cv_cv_sv_physical","cv_pene_only","cv_sv_physical","cv_recall_lifetime","cv_recall_1yr", "cv_sv_no_coerc","cv_sdg_choose_1_perp","cv_cv_sv_no_coerc"))

nid_in_both <- intersect(unique(lastyr$nid),unique(lifetime$nid))
dt_matched <- dt[nid %in% nid_in_both,]

## use loess to create dataset for input to dt_from_microdata
dt_smooth <- dt_matched
dt_smooth[,age_mid:=(age_start+age_end)/2]

smoother_lastyr <- loess(mean~age_mid,data=dt_smooth[cv_recall_1yr==1,],weights=sample_size)
smoother_lifetime <- loess(mean~age_mid,data=dt_smooth[cv_recall_lifetime==1,],weights=sample_size)

dt_smooth_lastyr <- ages
dt_smooth_lifetime <- ages
dt_smooth_lastyr[,age_mid:=(age_start+age_end)/2]
dt_smooth_lifetime[,age_mid:=(age_start+age_end)/2]

dt_smooth_lastyr$mean <- predict(smoother_lastyr, newdata=dt_smooth_lastyr)
dt_smooth_lifetime$mean <- predict(smoother_lifetime, newdata=dt_smooth_lifetime)

dt_smooth_lastyr$cv_recall_1yr <- 1
dt_smooth_lifetime$cv_recall_lifetime <- 1

dt_smooth <- rbind(dt_smooth_lastyr,dt_smooth_lifetime, fill=T)

## enter these fields for dt_from_microdata function
dt_smooth$nid <- 11111
dt_smooth$sex <- "Female"
dt_smooth$location_id <- 1
dt_smooth$location_name <- "Global"
dt_smooth$sample_size <- mean(dt$sample_size)
dt_smooth$measure <- "prevalence"

dt_from_microdata <- crosswalk_from_microdata(dt=dt,
                                              xwalk_data=dt_smooth,
                                              ref="recall_1yr",
                                              alts=c("recall_lifetime"),
                                              measure_name="prevalence")

## formatting
dt_from_microdata <- dt_from_microdata[bundle_id!=645,]
dt_from_microdata[,bundle_id:=NA]
dt_from_microdata[,c("age_mid","case_name"):=NULL]
setnames(dt_from_microdata,"case_type","case_name")

dt_from_microdata[group_review==0,is_outlier:=1]
dt_from_microdata[group_review==0,case_name:="recall_lifetime"]
dt_from_microdata[group_review==0,group_review:=1]
dt_from_microdata[standard_error>1,standard_error:=0.99]

write.xlsx(dt_from_microdata, "FILEPATH", sheetName = "extraction")
upload_epi_data(645,"FILEPATH")

## END


