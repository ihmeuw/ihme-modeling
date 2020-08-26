##########################################################################
### Author
### GBD 2019
### Purpose: Prep VAD for stgpr

##########################################################################

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "~/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, openxlsx, ggplot2, reshape2)

source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/validate_input_sheet.R")
source("FILEPATH/get_population.R")
source("FIELPATH/sex_split_functions.R") 
############################################################################################################################################

#old bundle
crosswalk_version <- 88
bundle_id <- 88

########################

df <- as.data.table(read.xlsx("FILEPATH/bundle.xlsx"))

df[,seq:=NA]
df[, year_id:=round(year_start+(year_end-year_start)/2)]
setnames(df, "year_start", "orig_year_start")
setnames(df, "year_end", "orig_year_end")
setnames(df, "mean", "val")
df[, variance:=standard_error^2]
val <- quantile(df$variance, 0.05)
df[variance==0, variance:=val]
df[, is_outlier:=0]
df[, measure:="proportion"]

#mark one outlier
df[location_id==202 & smaller_site_unit==1 & year_id==1992 & age_end==75.99, is_outlier:=1]
df[location_id==114 & smaller_site_unit==1 & year_id==1999, is_outlier:=1]


#assign narrow data to age bins and avoid splitting
df[age_start >=33  & age_end <=40, age_group_id:=12] 
df[age_start >=5  & age_end <=14, age_group_id:=6] 
df[age_start >=74  & age_end <=80, age_group_id:=20] 
df[age_start >=11  & age_end <=17.08, age_group_id:=7] 
df[age_start >=19  & age_end <=26, age_group_id:=9] 
df[age_start >=26  & age_end <=33, age_group_id:=10] 
df[age_start >=40  & age_end <=46, age_group_id:=13] 
#
df[age_start >=0  & age_end <=10, age_group_id:=5]  
df[age_start >=2  & age_end <=12, age_group_id:=6]  
df[age_start >=7  & age_end <=17.10, age_group_id:=7] 
df[age_start >=12  & age_end <=22, age_group_id:=8] 
#
df[age_start >=1  & age_end <=5, age_group_id:=5] 
df[age_start >=5 & age_end <=10, age_group_id:=6]
df[age_start >=10 & age_end <=15, age_group_id:=7]
df[age_start >=15 & age_end <=20, age_group_id:=8]
df[age_start >=20 & age_end <=25, age_group_id:=9]
df[age_start >=25 & age_end <=30, age_group_id:=10]
df[age_start >=30 & age_end <=35, age_group_id:=11]
df[age_start >=35 & age_end <=40, age_group_id:=12]
df[age_start >=40 & age_end <=45, age_group_id:=13]
df[age_start >=45 & age_end <=50, age_group_id:=14]
df[age_start >=50 & age_end <=55, age_group_id:=15]
df[age_start >=55 & age_end <=60, age_group_id:=16]
df[age_start >=60 & age_end <=65, age_group_id:=17]
df[age_start >=65 & age_end <=70, age_group_id:=18]
df[age_start >=70 & age_end <=75, age_group_id:=19]
df[age_start >=75 & age_end <=80, age_group_id:=20]
df[age_start >=80 & age_end <=85, age_group_id:=31]
df[age_start >=85 & age_end <=90, age_group_id:=32]
df[age_start >=90 & age_end <=95, age_group_id:=33]  

#mark the ones that need splitting
df[is.na(age_group_id), age_group_id:=22]

write.xlsx(df, "FILEPATH/bundle_upload.xlsx", sheetName="extraction")
upload_bundle_data(7601, filepath = "FILEPATH/bundle_upload.xlsx", decomp_step = "step4", gbd_round_id = 6)
save_bundle_version(7601, gbd_round_id = 6, decomp_step = "step4") #19220

###########################
# Need to age split the data
###########################

bv_id <- 19220

if(run_age_split){
  
  location_pattern_id <- 1  
  
  draws <- paste0("draw_", 0:999)
  original <- get_bundle_version(bv_id)
  original[!is.na(crosswalk_parent_seq), seq:=NA]
  original[, id := 1:.N]
  original[, was_age_split:=0]
  
  data_to_split <- original[age_group_id==22]
  
  #data_to_split[,age_range:=NULL]
  data_to_split[,year_id:=round((year_start + year_end)/2, 0)]
  data_to_split[sex=="Male", sex_id:=1]
  data_to_split[sex=="Female", sex_id:=2]
  data_to_split[age_end==80, age_end:=100] 
  data_to_split[, cases:=mean*effective_sample_size]
  data_to_split[, age_group_id:=NULL]
  
  ages <- get_age_metadata(12)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_start:=round(age_start)]
  ages[, age_end:=round(age_end)]
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  
  split_dt <- expand_age(small_dt=data_to_split, age_dt = ages)
  
  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)
  
  ## GET PULL LOCATIONS
  location_pattern_id <- 1 
  locations <- location_pattern_id   #if we want to pull the age pattern from one single location
  
  
  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = 2510, age_groups = ages$age_group_id, dstep="step4")  #id for age pattern model 
  age_pattern_model_version <- get_best_model_versions("modelable_entity", 2510, gbd_round_id = 6, decomp_step = "step4")
  age_pattern[,modelable_entity_id:=2510]
  age_pattern[,model_version:=age_pattern_model_version$model_version_id]
  write.csv(age_pattern, "/FILEPATH/VAD_agepattern.csv", row.names = FALSE)
  
  
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id"))
  
  
  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = ages$age_group_id)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))
  
  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  it_split_dt <- age_split_data_proportion(split_dt)
  
  ######################################################################################################
  
  final_dt <- format_data_forfinal(it_split_dt, location_split_id = location_pattern_id, region = F,
                                   original_dt = original)
  
  final_dt[, c("age_start", "age_end") := NULL]
  final_dt[age_group_id==1, age_group_id:=5]
  final_dt <- final_dt[standard_error < 1]   
  
  final_dt <- merge(final_dt, ages, by="age_group_id")
  final_dt[, year_id:=round(year_start+(year_end-year_start)/2)]
  final_dt[, data:=mean]
  final_dt[, variance:=standard_error^2]
  final_dt[sex=="Female", sex_id:=2]
  final_dt[sex=="Male", sex_id:=1]
  
  val <- quantile(final_dt$variance, 0.05)
  final_dt[variance==0, variance:=val]
  
  
  write.csv(final_dt, "FILEPATH/VAD_agesplit_data.csv", row.names = FALSE)
  
  save_dir <- "FILEPATH/age_split_data/"
  
  openxlsx::write.xlsx(final_dt, paste0(save_dir, "/adjusted_data_for_upload.xlsx"), sheetName = "extraction")
  descript <- paste0("ST-GPR ready age split data: sex split cv", crosswalk_version, " adjusted using age trend from mv", age_pattern_model_version$model_version_id)
  
  output_file <- paste0(save_dir, "/adjusted_data_for_upload.xlsx")
  # 
  print(validate_input_sheet(bundle_id, output_file, error_log_path = "/FILEPATH/"))
  # 
  print(save_crosswalk_version(bundle_version=7796, output_file, description = descript))
}


################

bundle_data <- get_bundle_version(19220, fetch="all")
setnames(bundle_data, "seq", "new_bundle_seq")
final_data <- fread("/FILEPATH/VAD_agesplit_data.csv")

final_data2 <- merge(final_data, bundle_data[,c("new_bundle_seq","bundle_325_seq")], by.x="crosswalk_parent_seq",by.y="bundle_325_seq", all.x = TRUE)
final_data2 <- merge(final_data2, bundle_data[,c("new_bundle_seq","bundle_325_seq")], by.x="seq",by.y="bundle_325_seq", all.x = TRUE)

final_data2[, crosswalk_parent_seq:=new_bundle_seq.x]
final_data2[, seq:=new_bundle_seq.y]
final_data2[, new_bundle_seq.x:=NULL]
final_data2[, new_bundle_seq.y:=NULL]

final_data2[, uncertainty_type_value:=95]
final_data2[, measure:="proportion"]
setnames(final_data2, "mean", "val")
setnames(final_data2, "year_start", "orig_year_start")
setnames(final_data2, "year_end", "orig_year_end")

write.xlsx(final_data2, "FILPATH/new_crosswalk_upload.xlsx", sheetName="extraction")

save_crosswalk_version(19220, "FILEPATH/new_crosswalk_upload.xlsx", description="age and sex split data for iterative stgpr modeling")


######### a couple more outliers:

final_data <- as.data.table(read.xlsx("FILEPATH/new_crosswalk_upload.xlsx"))
final_data[location_id==36 & sex_id==1 & age_group_id %in% c(7,8), is_outlier := 1]   
final_data[location_id==38 & age_group_id %in% c(8:14), is_outlier := 1]   

final_data[location_id==180 & age_group_id %in% c(6,7) & year_id==1998 & sex_id==1, is_outlier:=1]
final_data[location_id==180 & age_group_id ==5 & year_id==1994 & sex_id==1 & sample_size==3298, is_outlier := 1]
final_data[location_id==180 & age_group_id ==5 & year_id==1994 & sex_id==1, is_outlier:=1]
final_data[location_id==180 & age_group_id ==5 & year_id==1999 & sex_id==1 & val==1, is_outlier:=1]

final_data[location_id==179 & year_id==1993 & sex_id==1, is_outlier:=1]

final_data[location_id==18 & year_id==1985 & sex_id==1 & age_group_id==5, is_outlier:=1]
final_data[location_id==18 & year_id==1988, is_outlier:=1]
final_data[location_id==177 & year_id==1988, is_outlier:=1]
final_data[location_id==194 & age_group_id==6, is_outlier:=1]
final_data[location_id==215 & age_group_id==5 & sex_id==1, is_outlier:=1]
final_data[location_id==211 & age_group_id==5 & sex_id==1 & year_id==1997, is_outlier:=1]

write.xlsx(final_data, "/FILEPATH/crosswalk_upload.xlsx", sheetName="extraction")
save_crosswalk_version(19220, "/FILEPATH/crosswalk_upload.xlsx", description="age and sex split data for stgpr modeling")
