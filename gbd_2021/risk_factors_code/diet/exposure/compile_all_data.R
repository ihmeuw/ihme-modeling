################################################################################
## DESCRIPTION ##  Compiles all of the diet data   #############################
################################################################################

rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, 'FILEPATH/primer.R'))
library(readstata13)


###########################################################################
############################ helper functions #############################
###########################################################################

source(sprintf("FILEPATH/diet_helper_functions.R", user))

load_intake_data <- function(nhanes_version = "2021_1", microdata_tab_version = "2021_1", gbd_round = "gbd2021"){
 
  nhanes <- as.data.table(read.dta13(sprintf("FILEPATH/`version'/NHANES_collapsed", gbd_round, nhanes_version)))
  microdata_tab <- as.data.table(read.dta13(sprintf("FILEPATH/microdata_tabulations_compiled_%s.dta", gbd_round, microdata_tab_version)))
  china1 <- as.data.table(read.dta13("FILEPATH/china1992_prepped_for_diet_compiler_2.dta"))
  china2 <- as.data.table(read.dta13("FILEPATH/china1992_2002_prepped_for_diet_compiler_3.dta"))
  additional_diet <- as.data.table(read.dta13("FILEPATH/additional_diet_data_12.dta"))
  metc_codebook <- fread("FILEPATH/metc_codebook.csv")  
  
  #make id columns for adjustments
  nhanes[,c("student_lit", "microdata_tabs", "usa", "chn", "chn2") := list(0,0,1,0,0)]
  china1[,c("student_lit", "microdata_tabs", "usa", "chn", "chn2") := list(0,0,0,1,0)]
  china2[,c("student_lit", "microdata_tabs", "usa", "chn", "chn2") := list(0,0,0,0,1)]
  setnames(additional_diet, c("yr_st","yr_end","age_st"), c("year_start", "year_end", "age_start"))
  diet_data <- rbind(nhanes, microdata_tab, fill= TRUE)
  china <- rbind(china1, china2, fill = TRUE)
  diet_data <- rbind(diet_data, china, fill=TRUE)
  diet_data[, ihme_data:=0]
  
  #more formatting 

  diet_data[gbd_cause=="sodium" & usa==1, met:=2]
  diet_data[gbd_cause %in% c("pufa", "satfat") & usa==1, mean_1unadj:=mean_1enadj]  
  diet_data[gbd_cause %in% c("pufa", "satfat") & usa==1, sd_1unadj:=sd_1enadj]
  diet_data[(chn==1 & location_id!=6) | chn2==1, ihme_data:=1]
  diet_data <- diet_data[!(metc %in% c(19,1,3,10,6) & chn==1)]  
  diet_data <- diet_data[!(use==0)] 
  diet_data[is.na(nid), nid:=103215]
  diet_data[svy=="1992 China National Nutrition Survey", nid:=119643]
  diet_data[svy=="1992 China National Nutrition Survey", nid:=124479]
  diet_data[svy=="United Kingdom National Diet and Nutrition Survey 2008-2011", nid:=120995]
  diet_data[svy=="2002 China National Nutrition and Health Survey", nid:=124479] 
  
  diet_data <- rbind(diet_data, additional_diet, fill=TRUE)
  diet_data[is.na(ihme_data), ihme_data:=1]
  diet_data[nid==33054, diet_2:=4] 
  diet_data[nid==229834, diet_2:=2] 
  diet_data[nid==5241, diet_2 := 4] 
  diet_data[is.na(n), sample_size:=effective_sample_size]
  diet_data[!is.na(n), sample_size:=n]
  vars_to_drop <- c("chn","chn2", "effective_sample_size","n")
  diet_data <- diet_data[, !vars_to_drop, with=FALSE]
  
  diet_data$gbd_cause <- tolower(diet_data$gbd_cause)
  causes <- unique(diet_data$gbd_cause)
  matches <- sapply(metc_codebook$cause, function(i) grep(i, causes, value=TRUE) )
  matches$veg <- c("veg") 

  for( i in 1:length(matches)){
    matches[i]
    metc_codebook[i, metc]
    diet_data[gbd_cause %in% unlist(matches[i]) | metc==metc_codebook[i, metc], c("metc","gbd_cause") := list(metc_codebook[i, metc],metc_codebook[i, gbd_cause])  ]
  }
  
  diet_data[metc==25 & usa==1, unit_1:=2]  
  diet_data <- diet_data[!is.na(metc),]
  diet_data[is.na(year_start) & is.na(year_end) & metc==16, year_start := 1998] 
  diet_data <- diet_data[gbd_cause!="diet_fruit_juice"]   
  vars_to_drop <- c("usa","id","date_created", "file_version_num","file_version_name", "survey_name","survey_version", "svyc")
  diet_data <- diet_data[, !vars_to_drop, with=FALSE]
  diet_data[gbd_cause %in% c("dietpufa", "diet_satfat", "diet_total_fat") & is.na(mean_1unadj), mean_1unadj:=mean_1enadj]  
  diet_data[gbd_cause %in% c("dietpufa", "diet_satfat", "diet_total_fat") & is.na(sd_1unadj), sd_1unadj:=sd_1enadj]
  diet_data[, id_check := paste(ihme_data, nid, location_id, metc, diet_2, age_start, age_end, year_start, year_end, sex, sep = "-")] 
  diet_data[, both_adj:= is.na(mean_1enadj)+is.na(mean_1unadj)]
  india <- diet_data[ svy=="India NSS 2004-2005"]
  diet_data[both_adj ==1 & svy=="India NSS 2004-2005", mean_1enadj:=sum(mean_1enadj, na.rm=TRUE), by=id_check]
  diet_data[both_adj ==1 & svy=="India NSS 2004-2005", sd_1enadj:=sum(sd_1enadj, na.rm=TRUE), by=id_check]
  diet_data[, both_adj:= is.na(mean_1enadj)+is.na(mean_1unadj)]
  diet_data <- diet_data[!(svy=="India NSS 2004-2005" & both_adj==1),]
  diet_data[, both_adj:=NULL]
    #check!
  if(!length(unique(diet_data$id_check))==nrow(diet_data)){
    stop("Warning! Ids are not unique")
  }
  diet_data[, id_check:=NULL]
  
  return(diet_data)
} #end of function  

clean_param_se_vals <- function(diet_data){
    diet_data[, exp_mean:=mean_1unadj]
   diet_data[is.na(exp_mean), exp_mean:=mean_1enadj]  
  (no_mean <- diet_data[is.na(exp_mean)])  
    diet_data[sd_oth==3, sd_1unadj:=NA]  
   need_ss <- unique(diet_data[is.na(sample_size), gbd_cause])
  for(risk in need_ss){
    val <- quantile(diet_data[gbd_cause==risk, sample_size], .10, na.rm=TRUE)
    diet_data[is.na(sample_size) & gbd_cause==risk, sample_size:=val]
  }
  
  # Compute SE: where sd_oth == 1, the value in the sd variables in the dataset are the SD. Divide the SD by the sqrt(samplesize) to get the SE
  diet_data[sd_oth==1, se_1unadj:= sd_1unadj/(sqrt(sample_size))]
  
  # where sd_oth == 2, this means the value reported in the SD column is actually an SE, so we should carry these values over to the SE variables
  diet_data[sd_oth==2, se_1unadj:= sd_1unadj]
  diet_data[sd_oth==3, se_1unadj:= sd_1unadj]
  
  #zinc is in miligrams so have to convert to g
  #also for anything else in mg
  diet_data[unit_1==2, exp_mean:=exp_mean/1000]
  diet_data[unit_1==2, se_1unadj:=sd_1unadj/1000]
  
  #** We want to impute values for SE and SD when they are reported as 0. It is implausible that SE or SD can take on these values if the sample size is > 1. Start by blanking out the SE values for these observations
  diet_data[se_1unadj==0 & sample_size > 1, se_1unadj:=NA]
  diet_data[se_1unadj==0 | se_1unadj < 0.000000001, se_1unadj:=NA]  
  
  #generate overall se variable 
  diet_data[, standard_error:=se_1unadj]
  #** generate standard deviation variable based on the SE computed above. SE should be in the correct units and energy adjusted already, so to generate standard deviation, just multiply SE by sqrt(effective sample size)
  diet_data[, standard_deviation:= standard_error*sqrt(sample_size)]
  return(diet_data)
}

drop_data <- function(diet_data){
    diet_data <- diet_data[!(is.na(exp_mean)),]
   #drop where sex = Total when there are observations for both males & females
  diet_data[, sex_check := paste(gbd_cause, location_id,svy, age_start, age_end, year_start, year_end, sep = "-")] 
  diet_data[sex==1, has_males:= 1, by=sex_check ]
  diet_data[sex==2, has_females:= 1, by=sex_check ]
  diet_data[sex==3, has_both:= 1, by=sex_check ]
  diet_data[has_males==1 & has_females==1 & has_both==1, dropme:=1]   
  diet_data <- diet_data[!(dropme==1 & sex==3),]
  diet_data[, c( "has_males", "has_females", "has_both", "sex_check", "dropme") := NULL, with=TRUE]
  return(diet_data)
  }
load_fao_sales_phvo <- function(version = "2021_1", gbd_round = "gbd2021"){
   modeled_data <- paste0("FILEPATH", gbd_round, "FILEPATH",version, ".csv" )
  return(modeled_data)
}

###########################################################################
################# functions to compile everything #########################
###########################################################################

compile_data <- function(compile_method, compile_version, version_description, gbd_round, make_folder=TRUE, save_full_dataset=TRUE){
  output_folder <- paste0("FILEPATH", gbd_round, "FILEPATH", compile_version)
  if(make_folder){dir.create(output_folder)}
  if(compile_method==1 | compile_method==3){dir.create(paste0(output_folder, "/raw_data_to_upload"))}
  if(compile_method==2 | compile_method==3){dir.create(paste0(output_folder, "/raw_data_to_upload"))}
  
  if(make_folder){
  fileConn<-file(paste0(output_folder, "/version_description.txt"))
  writeLines(version_description, fileConn)
  close(fileConn)
  }
  model_compile_version <- compile_version
   hhbs <- as.data.table(read.dta13("FILEPATH/hhbs_for_split_3.dta"))    
    hhbs[, standard_error:=sqrt(variance)]
  setnames(hhbs, c("mean_value", "sales_data", "year","risk"), c("mean", "cv_sales_data", "year_id","ihme_risk"))
  hhbs[, c("cv_cv_hhbs", "svy","sex_id","sex","age_group_id", "to_split"):= list(1, "HHBS pre split",3,"Both",22,1)]
  vars_to_fill <- c("cv_representative", "cv_sales_data", "cv_cv_urinary_sodium","cv_TFA_ban","cv_fao", "cv_FFQ", "sex_issue","age_issue","year_issue") 
  hhbs[,c(vars_to_fill):=0]
  
  if(compile_method==3){
    diet <- load_intake_data()
    diet <- clean_param_se_vals(diet)
    nrow(diet)
    diet <- drop_data(diet)
    nrow(diet)
    
    some_diet_data <- rbind(modeled_faosales_data, hhbs, fill=TRUE)  
    all_diet_data <- rbind(diet, some_diet_data, fill=TRUE)
       
  }else{
    
    ##################################################################################################
    ##  bring in compiled data (w/o as_split data) and hhbs data since both methods need that data ###
    ##################################################################################################
    
    old_all_diet_data <- as.data.table(read.dta13(sprintf("FILEPATH/GBD_2017_no_as_split.dta", gbd_round)))
    diet_data <- old_all_diet_data[cv_fao==0 & cv_sales_data==0,] 
    diet_data[, note_modeler:=as.character(note_modeler)]
    risk_info <- unique(old_all_diet_data[,.(modelable_entity_id, ihme_risk)])
    to_delete <-names(which(sapply(diet_data, function(x)all(is.na(x) | x==""))))
    diet_data[, c(to_delete):=NULL]
        
    # Cleaning 

    diet_data[sex=="Female", sex_id:=2]
    diet_data[sex=="Male", sex_id:=1]
    diet_data[sex=="Both", sex_id:=3]
    diet_data[, standard_deviation:=NULL]  
    diet_data[, variance:=standard_error^2]
    diet_data[ is.na(svy), svy:=survey_name]
     more_to_delete <- c("rep_2","survey_name","survey_version","country","date_created","file_version_num","file_version_name","unit", "diet_id","flag","orig_uncertainty_type","orig_unit_type","acause", "data_status", "me_risk", "unit_value_as_published", "sales_data", "fao_data")
    diet_data[, c(more_to_delete):=NULL]
    diet_data[, to_split:=0]
    diet_data[, microdata_tabs:=1]   
    #both methods need hhbs and no split compiled data
    some_diet_data <- rbind(diet_data, hhbs, fill=TRUE)  
    some_diet_data[ihme_risk=="diet_grain" | ihme_risk=="diet_grains", ihme_risk:="diet_whole_grains"]
    additional_microdata <- fread(sprintf("FILEPATH/oil_butter_additions.csv", gbd_round))
    setnames(additional_microdata, "var", "ihme_risk")
    setnames(additional_microdata, "ihme_loc_id", "iso3")
    additional_microdata[ihme_risk=="butter_total_1" | ihme_risk=="diet_butter_total", ihme_risk:="diet_butter"]
    additional_microdata[ihme_risk=="oil_total_1" | ihme_risk=="diet_oil_total", ihme_risk:="diet_total_oil"]
    additional_microdata[survey_name=="USA/NATIONAL_HEALTH_AND_NUTRITION_EXAMINATION_SURVEY", svy:="National Health and Nutrition Examination Survey"]
    additional_microdata[survey_name=="GBR/NATIONAL_DIET_NUTRITION_SURVEY", svy:="GBR National Diet Nutrition Survey"]
    to_delete <- c("survey_name", "file_path","survey_module","nclust","nstrata","design_effect","note_modeler")
    additional_microdata[,c(to_delete):=NULL]
    additional_microdata[sex_id==1, sex:="Male"]
    additional_microdata[sex_id==2, sex:="Female"]
    additional_microdata[,variance:=standard_error^2]
    additional_microdata[, parameter_type:="continuous"]
    additional_microdata[, to_split:=0]
    additional_microdata[, year_id:=year_end]
    additional_microdata[, extractor:="herbertm"]
    additional_microdata[svy=="National Health and Nutrition Examination Survey", location_id:=102]
    additional_microdata[svy=="GBR National Diet Nutrition Survey", location_id:=95]
    vars_to_fill <- c( "cv_sales_data", "cv_cv_urinary_sodium","cv_TFA_ban","cv_fao", "cv_FFQ", "sex_issue","age_issue","year_issue", "cv_cv_hhbs")   
    some_diet_data <- rbind(some_diet_data, additional_microdata, fill=TRUE)
    
    
    if(compile_method==2){
      
      #prepp the modeled faosales data to make it compatabile
      modeled_faosales_data <- fread(paste0("FILEPATH",gbd_round,"FILEPATH",model_compile_version, ".csv" ))    
      setnames(modeled_faosales_data, c("gpr_mean", "back_se"), c("mean", "standard_error"))
      modeled_faosales_data[, c("gpr_lower", "gpr_upper", "gbd_cause"):=NULL]
      modeled_faosales_data[cv_sales_data==0 | is.na(cv_sales_data), c("cv_fao", "svy", "to_split", "cv_sales_data"):=list(1,"FAO SUA",1, 0)]
      modeled_faosales_data[cv_sales_data==1, c("cv_fao",  "svy", "to_split"):=list(0, "Euromonitor",1)]
      modeled_faosales_data[cv_sales_data==1, ihme_risk:=gsub("_sales", "", ihme_risk)]
      cvs_to_fill <- c("cv_representative", "cv_cv_hhbs", "cv_cv_urinary_sodium","cv_TFA_ban", "cv_FFQ")   
      modeled_faosales_data[, c(cvs_to_fill):=0]
      modeled_faosales_data[, sex:="Both"]
      modeled_faosales_data[ihme_risk=="diet_total_grains", ihme_risk:="diet_total_grain"]
      
      modeled_faosales_data <- merge(modeled_faosales_data, risk_info, by="ihme_risk", all.x=TRUE)
      modeled_faosales_data[ihme_risk=="diet_whole_grains", modelable_entity_id:=2431]  
      all_diet_data <- rbind(some_diet_data, modeled_faosales_data, fill=TRUE)
      
    }
    if(compile_method==1){
      modeled_faosales_data <- as.data.table(read.dta13("FILEPATH/FAO_item_nutrient_modeled_compiled_unadj_3.dta"))         
      setnames(modeled_faosales_data, c("mean_value", "year","risk", "sales_data"), c("mean", "year_id","ihme_risk","cv_sales_data"))
      modeled_faosales_data[, standard_error:=sqrt(variance)]
      modeled_faosales_data[cv_sales_data==0, c("cv_fao", "nid", "svy", "to_split"):=list(1, 200195, "FAO FBS",1)]
      modeled_faosales_data[cv_sales_data==1, c("cv_fao", "nid", "svy", "to_split"):=list(0, 282698, "Euromonitor",1)]
      modeled_faosales_data[cv_sales_data==1, ihme_risk:=gsub("_sales", "", ihme_risk)]
      modeled_faosales_data[ihme_risk=="diet_hvo", ihme_risk:="diet_transfat"]
      cvs_to_fill <- c("cv_representative", "cv_cv_hhbs", "cv_cv_urinary_sodium","cv_TFA_ban", "cv_FFQ")   
      modeled_faosales_data[, c(cvs_to_fill):=0]
      modeled_faosales_data[, age_group_id:=22]
      modeled_faosales_data[, sex_id:=3]
      modeled_faosales_data[, sex:="Both"]
      nrow(modeled_faosales_data)
      modeled_faosales_data <- merge(modeled_faosales_data, risk_info, by="ihme_risk", all.x=TRUE) 
      nrow(modeled_faosales_data) 
      all_diet_data <- rbind(some_diet_data, modeled_faosales_data, fill=TRUE)
      all_diet_data[, step2_location_year:="recompiled"]
    }
    
    ##############################################################################
    ##### Doing more data cleaning  #####
    ##############################################################################

    all_diet_data[is.na(year_start) & is.na(year_end), c("year_start","year_end"):=list(year_id, year_id)]
    all_diet_data[is.na(year_id), year_id:=year_start]
    all_diet_data[, year:=NULL]
    all_diet_data[age_group_id==22, c("age_start","age_end"):=list(0, 99)]
    all_diet_data[ is.na(age_group_id), age_group_id:=make_age_group_ids(age_start, age_end, nid)]       
    all_diet_data <- last_minute_outliering(all_diet_data)
    
    #keep around a diet_zinc (all zinc data) AND a diet_zinc_age (only age group 5, all-age data)
    zinc_set <- all_diet_data[ihme_risk=="diet_zinc" & age_group_id %in% c(5,22)]  
    zinc_set[, ihme_risk:="diet_zinc_age"]
    zinc_set[age_group_id==5, c("age_group_id", "note_modeler"):=list(22, "Actually age group 5 but coding as 22 for all-age stgpr model")]
    all_diet_data <- rbind(all_diet_data, zinc_set)
    
    if(compile_method==1){
    all_diet_data[ihme_risk=="diet_ssb" & nid==136253, is_outlier:=1] 
    all_diet_data[ihme_risk=="diet_ssb" & nid==141458, is_outlier:=1] 
    all_diet_data[ihme_risk=="diet_transfat" & nid==282698, nid:=160911]   
    all_diet_data[ihme_risk=="diet_zinc_age" & nid %in% c(153674,196406), is_outlier:=1 ] 
    
    }
    
    #variance (impute by risk factor)   #for locs without variance already there

    all_diet_data[, se_adj:=0]
    all_diet_data[is.na(standard_error) | standard_error==0, se_adj:=1]
    se_quantile_by_risk <- all_diet_data[cv_fao==0 & cv_sales_data==0 & se_adj==0, .(ninety_percent_se=quantile(standard_error, probs = 0.90, na.rm=TRUE)), by=ihme_risk ]
     for(risk in se_quantile_by_risk$ihme_risk){
      all_diet_data[se_adj==1 & ihme_risk==risk, standard_error:= se_quantile_by_risk[ihme_risk==risk]$ninety_percent_se]
    }
    all_diet_data[se_adj==1, variance:=standard_error^2]
    
    # need the data to be called "val" for the upload
    setnames(all_diet_data, "mean", "val")
    all_diet_data[,year_end:=NULL]
    all_diet_data[,year_start:=NULL]
    setcolorder(all_diet_data, c("ihme_risk", "modelable_entity_id","nid", "location_id", "location_name", "year_id","age_start","age_end","age_group_id","sex","sex_id","val","standard_error","variance","sample_size","svy","parameter_type","source_type"))
    all_diet_data[, underlying_nid:=nid]
    all_diet_data[,measure:="continuous"]
    all_diet_data[ cv_fao==0 & cv_sales_data==0 & cv_cv_hhbs==0 & cv_cv_urinary_sodium==0 & cv_FFQ==0, cv_dr:=1]
    all_diet_data[is.na(cv_dr), cv_dr:=0]
    
     #################################
    ####### save the data ###########
    #################################
    
    if( save_full_dataset){
    write.csv(all_diet_data, paste0(output_folder, "/all_diet_data_", compile_version,".csv"), row.names = FALSE, na="")
    
      cvs <- c("cv_fao", "cv_cv_hhbs","cv_sales_data","cv_FFQ","cv_cv_urinary_sodium","cv_dr")
      breakdown <- all_diet_data[, .N, by=c("ihme_risk",cvs )]
      setnames(breakdown, "N", "num_datapoints")
      breakdown[, percent_of_risk:= (num_datapoints/sum(num_datapoints))*100, by=ihme_risk]
      write.csv(breakdown, paste0(output_folder, "/summary_of_compiled_data.csv"), row.names = FALSE)
    message(paste0("full dataset saved to: ", output_folder))
        }
    
    risk_w_data <- unique(all_diet_data$ihme_risk)
       for( risk in risk_w_data){
      message(risk)
      sub_data <- all_diet_data[ihme_risk==risk]
      if(compile_method==1 | 2){
        
         sub_data[,seq:=NA]
        risk_specific_file <- paste0(output_folder, "/raw_data_to_upload/",risk,".xlsx")
        openxlsx::write.xlsx(sub_data, risk_specific_file, sheetName = "extraction")
        }
      
    }
    
    return(all_diet_data)
    
  } #end compile method 1 or 2 loop

}
add_age_split_data_crosswalk <- function(compile_version="2021_1", age_split_version="2021_1", gbd_round="gbd2021"){
   output_folder <- paste0("FILEPATH",gbd_round,"FILEPATH", age_split_version)
  ### for this set we pull the full dataset from compiled data and drop all the to_split and append the split data. then save individual files for crosswalks
  all_data_filepath <- paste0("FILEPATH",gbd_round,"FILEPATH", compile_version, "/all_diet_data_", compile_version,".csv")
  full_data <- fread(all_data_filepath)
  orig_data <- full_data[to_split==0]
  age_split_data <- fread(paste0(output_folder, "/as_data_for_compiler_", age_split_version, ".csv"))            
  #clean age split data before rbind
  age_split_data[, standard_error:=new_se]
  age_split_data[, val:=new_exp_mean]
  age_split_data[, age_end:= age_group_years_end]
  age_split_data[, age_start:=age_group_years_start]
  age_split_data[, ihme_risk:=risk]
  age_split_data[ cv_cv_hhbs==0 & cv_sales_data==0, cv_fao:=1]
  age_split_data[ is.na(cv_fao), cv_fao:=0]
  age_split_data[, to_split:=1]
  age_split_data[, mean:=val]
  age_split_data[, metc:=NA]
  age_split_data[, c("cv_FFQ", "cv_dr", "cv_cv_urinary_sodium", "microdata_tabs", "student_lit", "sex_issue", "year_issue", "age_issue"):=0]
  age_split_data <- last_minute_outliering(age_split_data)
  to_drop <- c("new_se", "new_exp_mean", "age_group_years_end","age_group_years_start", "risk", "year_start", "year_end", "mean", "metc")
  age_split_data[, c(to_drop):=NULL]
  new_full_data <- rbind(orig_data, age_split_data, fill=TRUE)
  setnames(new_full_data, "to_split", "was_age_split")
   write.csv(new_full_data, paste0(output_folder, "/all_diet_data_post_as_",age_split_version,".csv"), row.names = FALSE)
  return(new_full_data)
}
update_bundles <- function(save_dir=""){
  new_data <- fread("FILEPATH/data.csv")
  new_data[gbd_cause=="diet_sodium", gbd_cause:="diet_salt"]
  new_data[gbd_cause=="diet_grains", gbd_cause:="diet_whole_grains"]
  gbd_risks <- c("diet_calcium_low", "diet_fruit", "diet_fiber", "diet_milk", "diet_pufa", "diet_fish", "diet_nuts", "diet_legumes", "diet_procmeat", "diet_redmeat", "diet_ssb", "diet_whole_grains", "diet_salt", "diet_zinc", "diet_nuts", "diet_omega_3", "diet_veg", "diet_transfat")
  setnames(new_data, c("gbd_cause", "mean", "units"), c("ihme_risk", "val", "case_definition"))
  vars_to_fill <- c("cv_representative", "cv_sales_data", "cv_cv_urinary_sodium","cv_cv_hhbs","cv_TFA_ban","cv_fao","cv_dr", "cv_FFQ", "sex_issue","age_issue","year_issue", "se_adj", "measure_issue", "to_split")   
  new_data[,c(vars_to_fill):=0]
  new_data[type=="ffq", cv_FFQ:=1]
  new_data[type=="dr", cv_dr:=1]
  new_data[type=="hhbs", cv_cv_hhbs:=1]
  new_data[type=="usodium", cv_cv_urinary_sodium:=1]
  new_data[, year_id:=floor(year_start+(year_end-year_start)/2)]
  new_data[, age_group_id:=make_age_group_ids(age_start, age_end, nid)]           
  new_data[ age_end - age_start > 80, c("age_group_id", "to_split"):=list(22,1)]   
  vars_delete <- c("type", "year_start", "year_end")
  new_data[, c(vars_delete):=NULL]
  new_data[ ,measure:="continuous"]
  new_data[, se_adj:=0]
  new_data[ is.na(standard_error) | standard_error==0, se_adj:=1]
  se_quantile_by_risk <- fread("FILEPATH/se_quantile_by_risk.csv")
  for(risk in se_quantile_by_risk$ihme_risk){
    new_data[se_adj==1 & ihme_risk==risk, standard_error:= se_quantile_by_risk[ihme_risk==risk]$ninety_percent_se]
  }
  new_data[, variance:=standard_error^2]
  
    #now we pull in the outliers we saved, add on the new data and save to folder for upload
  for(m in gbd_risks){
        new_subdata <- new_data[ihme_risk==m]
        if(m=="diet_zinc"){
    other_age_zinc <- as.data.table(read.xlsx("FILEPATH/diet_zinc.xlsx"))
    other_age_zinc <- other_age_zinc[!(age_group_id %in% c(5,22))] #these were already uploaded so we remove them
    other_age_zinc[, note_modeler:="Not age group 5 but keeping in bundle"]
    new_subdata <- rbind(new_subdata, other_age_zinc, fill=TRUE)
    }
  outliers_to_upload <- as.data.table(read.xlsx(paste0("FILEPATH", m, ".xlsx")))
  me <- unique(outliers_to_upload$modelable_entity_id)[1]
  print(me)
  new_subdata[,modelable_entity_id:=me]
  to_upload <- rbind(outliers_to_upload, new_subdata, fill=TRUE)
  openxlsx:: write.xlsx(to_upload, paste0(save_dir, m, ".xlsx"), sheetName="extraction")
  }
}

###########################################################################
###################### execute compile functions ##########################
###########################################################################
full_data <- compile_data(compile_method = 2, compile_version = "2021_1", version_description="Testing", make_folder = FALSE, save_full_dataset = FALSE)
new_data <- add_age_split_data_crosswalk()












