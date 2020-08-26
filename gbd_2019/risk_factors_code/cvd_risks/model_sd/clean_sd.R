############################################
## Purpose: Clean a dataset for modelling SD
############################################

if(!exists("rm_ctrl")){
  rm(list=objects())
}

j <- "FILEPATH"

date<-gsub("-", "_", Sys.Date())

library(data.table)
library(ggplot2)

################### SCRIPTS #########################################
## paths to repositories
code_root <- "FILEPATH"
central <- "FILEPATH"

source(paste0(code_root, "utility/get_recent.R"))
source(paste0(code_root, "utility/data_tests.R"))
source(paste0(code_root, "utility/get_user_input.R"))

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_model_results.R"))

################### ARGS AND PATHS #########################################

if(!exists("me")){
  me <- get_me_from_user() ## needs to be "sbp" or "chl" or "ldl"
  decomp_step <- get_step_from_user()
}

step_num <- gsub('step', '', decomp_step)

convert_se <- F
only_adults <- T
plot <- F
message("Cleaning ", me, " sd data")

## Data paths
new_data_path <- "FILEPATH"

subnat_reextract_paths <- list(
  "FILEPATH/reextract_subnats_name_1to1map.xlsx",
  "FILEPATH/reextract_subnats_name_multiplemap.xlsx"
)

micro_folder <- paste0("FILEPATH", me, "_post_ubcov/")

## Utility paths
unit_conversion_path <-"FILEPATH/unit_conversion.csv" ## table with conversion factors for things like g/mL to mmol/L
standard_units_path <- "FILEPATH/standard_units.csv"  ## table with the desired measure for each me_name

output <- paste0("FILEPATH", me, "_to_sd/to_sd_decomp", step_num, "_", date, ".csv")
plot_output <- paste0("FILEPATH/", me, "_sd_data_decomp", step_num, "_", date, ".pdf")


################### CONDITIONALS #########################################
if(me=="sbp"){
  me_dis<-"hypertension"
  meid<-2547
}

if(me=="chl"){
  me_dis<-"hypercholesterolemia"
  meid<-2546
}

if(me=="ldl"){
  me_dis<-"hypercholesterolemia"
  meid<-18822
}

################### GET LIT AND CLEAN #########################################
## bring in new data
new_files<-list.files(new_data_path, pattern=".xlsx", full.names=T)
new_files<-new_files[!grepl("\\~\\$", new_files)] ## drops any temp open files

message("Reading in files:")
new_data <- rbindlist(lapply(new_files, function(x){
  message(x)
  extraction <- as.data.table(openxlsx::read.xlsx(x, sheet="literature_full"))[-1,]   ##sy: drop the first row, useless info
  extraction[, source_file:=x]
}), fill=T)

## get new subnational re-extractions
subnat_data <- rbindlist(lapply(subnat_reextract_paths, function(x){
  message(x)
  extraction <- as.data.table(openxlsx::read.xlsx(x))[-1,]   ##sy: drop the first row, useless info
  extraction[, source_file:=x]
}), fill=T)

## Some lines have 2 study names; collapse to one
subnat_data[is.na(study_name), study_name:=study_name_2]
subnat_data[, study_name_2:=NULL]

## drop NIDs that got re-extracted
new_data <- new_data[!nid %in% unique(subnat_data$nid)]
new_data <- rbindlist(list(new_data, subnat_data), use.names=T, fill=T)

## Don't use prevalence measures in new data
new_data <- new_data[me_name==me  |  me_name==me_dis, ]
new_data[, cv_new:=1]

df <- new_data 
df <- df[me_name==me  |  me_name==me_dis,]

## Get microdata
micro<-get_recent(micro_folder)

################### CHECK FORMATTING #########################################
## Convert vars to numeric
nvars <- c("age_start", "age_end", "year_start", "year_end") ## Source/Age/Year
nvars <- c(nvars, grep("mean|lower|upper|error|cases|sample_size", names(df), value=T)) ## Estimates
nvars <- c(nvars, "smaller_site_unit", "group_review") ## Flag's I care about
lapply(nvars, check_class, df=df, class="numeric")

## Drop columns group_review
df <- df[group_review != 0 | is.na(group_review),]

df <- df[, year_id := floor((year_start + year_end)/2)]
df <- df[tolower(sex) == "male", sex_id := 1]
df <- df[tolower(sex) == "female", sex_id := 2]
df <- df[tolower(sex) == "both", sex_id := 3]
df <- df[, unit := tolower(unit)]

################### SUBSET TO ONLY SD #########################################
df <- df[uncert_type=="SD"]
df <- df[!is.na(mean)  & !is.na(error)]
setnames(df, "error", "sd")

################### CONVERT UNITS #########################################
unit_conversion <- fread(unit_conversion_path)
standard_units <- fread(standard_units_path)

## Bring in unit sheet and merge on
df <- merge(df, unit_conversion, by.x=c("me_name", "unit"), by.y=c("var", "unit_from"), all.x=T)

## Convert
vars <- c("mean", "sd")
for (var in vars) df <- df[!is.na(conversion_factor), (var) := get(var) * conversion_factor]
df <- df[!is.na(conversion_factor), unit := unit_to]
df <- df[, c("unit_to", "conversion_factor") := NULL]

## Drop errant row with fpg data
df <- df[!(df$me_name=="dbp"  &  df$unit=="mmol/l"),]

df[me_name==me_dis, unit:="percent"]

## Check that units are consistent with the standard
df_units <- unique(df[, .(me_name, unit)])  
unit_check <- merge(df_units, standard_units, by="me_name", all.x=T)

if (nrow(unit_check[unit != standard_unit])) stop("issue with units")

################### FINAL CLEAN #########################################
df[, cv_lit:=1]

## clean up ages to make age group id, same as age-sex split code
df <- df[age_start > 95, age_start := 95]  
df[, age_end := age_end - age_end %%5 + 4]     
df[, age_start:=age_start-age_start%%5]    ##sy:forcing age start to %%5
df <- df[age_end > 99, age_end := 99]     

## drop age groups that are too large and 'both sexes'
df <- df[(age_end-age_start)==4  & sex_id %in% c(1,2),]

## make age group id
df[, age_group_id := round((age_start/5)+5)]  ##sy:haven't tested this yet
df[age_start>=80, age_group_id:=30]
df[age_start>=85, age_group_id:=31]
df[age_start>=90, age_group_id:=32]
df[age_start>=95, age_group_id:=235]

df <- df[, .(nid, ihme_loc_id, smaller_site_unit, year_id, sex_id, age_group_id, me_name, mean, sample_size, sd)]
df[, cv_lit:=1]
df[, ihme_loc_id:=as.character(ihme_loc_id)]

################### GET ALL MICRODATA FILES #########################################
## get all collapsed microdata and rbind together
micro <- get_recent(micro_folder, pattern = paste0('decomp', step_num))

micro <- micro[most_detailed==1,]
micro[, year_id:=floor((year_start+year_end)/2)]

micro[sex=="Male", sex_id:=1]
micro[sex=="Female", sex_id:=2]

setnames(micro, "var", "me_name")

micro <- micro[, .(ihme_loc_id, age_group_id, nid, year_id, sex_id, mean, standard_error, sample_size, me_name, smaller_site_unit)]

micro[, smaller_site_unit:=as.numeric(smaller_site_unit)]

micro <- micro[me_name %in% grep(me, unique(micro$me_name), value=T)]

################### RESHAPE #########################################
## Reshaping this way because SD didn't get calculated for all rows for some reason during collapse..
if(!paste0(me, "_standard_deviation")  %in% unique(micro$me_name))  stop("Missing SD for SE calculation")

se_df <- micro[me_name==paste0(me, "_standard_deviation"), ]
setnames(se_df, c("mean", "standard_error"), c("sd", "sd_se"))

se_df <- se_df[, .(nid, ihme_loc_id, age_group_id, sex_id, year_id, sd, sd_se, sample_size)]

var_df <- micro[me_name==me,]

if(nrow(var_df) != nrow(se_df))  message("Unequal number of rows for variable and standard errors")

micro <- merge(var_df, se_df, 
             by=c("nid", "ihme_loc_id", "age_group_id", "sex_id", "year_id", "sample_size"), 
             all.x=F, all.y=F)

micro[, sd_se:=NULL]  ## drop for rbind
micro[, standard_error:=NULL]
micro[, cv_lit:=0]

################### MERGE AND PLOT SCATTER #########################################
full <- rbindlist(list(df, micro), use.names = T, fill=F)

## Only use standard locations
stnd_locs<-get_location_metadata(location_set_id=101, gbd_round_id=ROUNDID) 

full <- full[ihme_loc_id %in% stnd_locs$ihme_loc_id, ]

message("Getting locs...")
locs <- get_location_metadata(location_set_id=22, gbd_round_id=6)[, .(location_id, ihme_loc_id, super_region_name, region_name, level)]  
full <- merge(full, locs, by="ihme_loc_id")
print("Done")

## Drop small sample sizes
message(paste("Dropping", nrow(full[sample_size<=10]), "rows for sample size<=10"))
full <- full[sample_size>10]

## Drop where sd is 0, sample size should take care of this
message(paste("Dropping", nrow(full[sd==0]), "rows where sd==0"))
full <- full[sd!=0]

if(me=="chl"){
  message(paste("Dropping", length(full[sd<3]), "rows where sd>3"))
  full <- full[sd<3,]
}

if(only_adults==T){
  ## only keep modeled age groups
  full <- full[age_group_id>=10,]
}

if(plot==T){
  pdf(file=plot_output)
  
  for(sex in c(1,2)){
    message("Making diagnostic plots for sex_id ", sex)
    full.s <- full[sex_id==sex,]
    for(age in unique(full.s$age_group_id)){
      full.s <- full[age_group_id==age, ]
      correl <- cor(full.s$mean, full.s$sd, method="spearman")
      
      p <- ggplot(data=full.s, aes(x=sd))+
            geom_histogram(aes(fill=super_region_name),color="black")+
            ggtitle(paste("Distribution of observed SD, sex_id:", sex))+
            theme_classic()+
            theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
      print(p)
      
      
      p<-ggplot(data=full, aes(x=mean, y=sd, color=super_region_name))+
            geom_point(alpha=0.2)+
            geom_rug(alpha=0.4)+
            ggtitle(paste("Sex_id:", sex, "Spearman correlation:", round(correl, digits=3)))+
            theme_classic()+
            theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
      print(p)
    }
  }
  dev.off()
}

write.csv(full, file=output)
