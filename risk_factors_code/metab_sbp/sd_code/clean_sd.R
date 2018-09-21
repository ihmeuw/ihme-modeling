################################
##author:USERNAME
##date: 4/10/2017
##purpose:    -Clean a dataset for modelling SD
##
#######################################

rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}

date<-gsub("-", "_", Sys.Date())


library(data.table, lib.loc=lib_path)
library(lme4, lib.loc=lib_path)
library(MuMIn, lib.loc=lib_path)
library(boot, lib.loc=lib_path)
library(ggplot2, lib.loc=lib_path)
library(rhdf5, lib.loc=lib_path)
library(arm, lib.loc=lib_path)
library(haven, lib.loc=lib_path)
library(plyr, lib.loc=lib_path)
library(dplyr, lib.loc=lib_path)
library(stringr, lib.loc=lib_path)
library(DBI, lib.loc=lib_path)
library(RMySQL, lib.loc=lib_path)
library(mvtnorm, lib.loc=lib_path)
library(readxl)


################### ARGS AND PATHS #########################################
######################################################

me<-"sbp"
convert_se<-F
message("Cleaning ", me, " sd data")

##USERNAME:data paths
old_data_path<-paste0(j, "FILEPATH", na.strings= "")
new_data_path<-paste0(j, "FILEPATH")
micro_folder<-paste0(j, "FILEPATH", me, "/")

##USERNAME:utility paths
unit_conversion_path <-paste0(j, "FILEPATHv") ##USERNAME:table with conversion factors for things like g/mL to mmol/L
standard_units_path <-paste0(j, "FILEPATH")  ##USERNAME: table with the desired measure for each me_name
central<-paste0(j, "FILEPATH/")



output<-paste0(j, "FILEPATH", me, "_to_sd/to_sd_", date, ".csv")  
plot_output<-paste0(j, "FILEPATH", me, "_sd_data_", date, ".pdf") 






################### CONDITIONALS #########################################
######################################################

if(me=="sbp"){
  me_dis<-"hypertension"
  meid<-2547
}

if(me=="chl"){
  me_dis<-"hypercholesterolemia"
  meid<-2546
}




################### SCRIPTS #########################################
######################################################

source(paste0(j, "FILEPATH"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_model_results.R"))




################### MINOR FUNCTIONS #########################################
######################################################


checknomiss <- function(df, var) {
  if (nrow(df[get(var)==var]) != 0) stop(paste0("missing rows for: ", var)) 
}

##USERNAME: this function preps any numbers that are stored as characters for coercion to numeric. Removes any spaces, commas, or extra decimal pts.
numclean <- function(x) {
  if (class(x) == "character") {
    x <- gsub(" ", "", x)
    x <- gsub("[..]", ".", x)
    x <- gsub("[,]", "", x)
  }
  x <- as.numeric(x)
  return(x)
}





################### GET LIT AND CLEAN #########################################
######################################################




old_data<-read.csv(old_data_path, header=T)
old_data<-as.data.table(old_data)
colorder <- names(df)

old_data[ ,diagnostic_criteria:=as.character(diagnostic_criteria)]
old_data[diagnostic_criteria=="", diagnostic_criteria:=NA]


##USERNAME: bring in new data
new_files<-list.files(new_data_path, pattern=".xlsx", full.names=T)

new_data<-rbindlist(lapply(new_files, function(x){
  as.data.table(read_excel(x, col_names=T, sheet="literature_full"))[-1,]   ##USERNAME: drop the first row, useless info
  
}), fill=T)



##USERNAME:for now, don't use prevalence measures in new data
new_data<-new_data[me_name==me  |  me_name==me_dis, ]
new_data[, cv_new:=1]

df<- rbind(old_data, new_data, fill=T)

df<-df[me_name==me  |  me_name==me_dis,]

##USERNAME: get micro
#micro<-get_recent(micro_folder)





################### CHECK FORMATTING #########################################
######################################################

## Convert vars to numeric
nvars <- c("age_start", "age_end", "year_start", "year_end") ## Source/Age/Year
nvars <- c(nvars, grep("mean|lower|upper|error|cases|sample_size", names(df), value=T)) ## Estimates
nvars <- c(nvars, "smaller_site_unit", "group_review") ## Flag's I care about
df <- df[, (nvars) := lapply(.SD, numclean), .SDcols=nvars] 


## Drop blank rows
df <- df[!is.na(nid) & nid != '',]

## Drop columns group_review
df <- df[group_review != 0 | is.na(group_review),]

## Year: Code id 
df <- df[, year_id := floor((year_start + year_end)/2)]

## Sex: Code id
df <- df[tolower(sex) == "male", sex_id := 1]
df <- df[tolower(sex) == "female", sex_id := 2]
df <- df[tolower(sex) == "both", sex_id := 3]

## Units 
df <- df[, unit := tolower(unit)]




################### SUBSET TO ONLY SD #########################################
######################################################
##USERNAME: only keep SD reports...not currently transforming SE of mean to SD
df<-df[uncert_type=="SD"]
df<-df[!is.na(mean)  & !is.na(error)]
setnames(df, "error", "sd")

if(convert_se==T){
  #not yet implemented
}






################### CONVERT UNITS #########################################
######################################################
unit_conversion<-fread(unit_conversion_path)
standard_units<-fread(standard_units_path)


## Bring in unit sheet and merge on 
df <- merge(df, unit_conversion, by.x=c("me_name", "unit"), by.y=c("var", "unit_from"), all.x=T)  ##USERNAME: here I'm merging the unit conversion df onto the data,

## Convert  ##USERNAME: this conversion only works on means
vars <- c("mean", "sd")
for (var in vars) df <- df[!is.na(conversion_factor), (var) := get(var) * conversion_factor]
df <- df[!is.na(conversion_factor), unit := unit_to]
df <- df[, c("unit_to", "conversion_factor") := NULL]

##USERNAME:there is one row where me_name is dbp and units are mmol/l, looks like it should be fpg, gonna drop this one
df<-df[!(df$me_name=="dbp"  &  df$unit=="mmol/l"),] 

df[me_name==me_dis, unit:="percent"]
## Check that units are consistent with the standard
df_units <- unique(df[, .(me_name, unit)])
unit_check <- merge(df_units, standard_units, by="me_name", all.x=T)

if (nrow(unit_check[unit != standard_unit])) stop("issue with units")




################### FINAL CLEAN #########################################
######################################################


df[, cv_lit:=1]
df[, year_id:=round(year_start+year_end/2)]

##USERNAME:clean up ages to make age group id, same as age-sex split code
df <- df[age_start > 95, age_start := 95]
df[, age_end := age_end - age_end %%5 + 4]
df[, age_start:=age_start-age_start%%5]    ##USERNAME:forcing age start to %%5
df <- df[age_end > 99, age_end := 99]

##USERNAME:drop age groups that are too large and 'both sexes'
df<-df[(age_end-age_start)==4  & sex_id %in% c(1,2),]

##USERNAME:make age group id
df[, age_group_id := round((age_start/5)+5)]  ##USERNAME:haven't tested this yet
df[age_start>=80, age_group_id:=30]
df[age_start>=85, age_group_id:=31]
df[age_start>=90, age_group_id:=32]
df[age_start>=95, age_group_id:=235]




df<-df[, .(nid, ihme_loc_id, smaller_site_unit, year_id, sex_id, age_group_id, me_name, mean, sample_size, sd)]
df[, cv_lit:=1]
df[, ihme_loc_id:=as.character(ihme_loc_id)]



################### GET ALL MICRODATA FILES #########################################
######################################################
##USERNAME: get all collapsed microdata and rbind together


files<-list.files(micro_folder, full.names=T)
files<-files[!grepl("WARNINGS", files)]
  
stack<-list()
for(i in 1:length(files)){
  x<-fread(files[i])
  #if(!"suspect_deff" %in% names(x)){ x[, suspect_deff:=NA]}
  #if(!"admin_2_id" %in% names(x)){ x[, admin_2_id:=NA]}
  stack[[i]]<-x
}
micro<-rbindlist(stack, fill=T)


micro<-micro[most_detailed==1,]
micro[, year_id:=round(year_start+year_end/2)]

micro[sex=="Male", sex_id:=1]
micro[sex=="Female", sex_id:=2]

setnames(micro, "var", "me_name")

micro<-micro[, .(ihme_loc_id, age_group_id, nid, year_id, sex_id, mean, standard_error, sample_size, me_name, smaller_site_unit)]

micro[, smaller_site_unit:=as.numeric(smaller_site_unit)]




################### JANKILY RESHAPE #########################################
######################################################

##USERNAME:I am reshaping this way because SD didn't get calculated for all rows for some reason during collapse..
if(!paste0(me, "_standard_deviation")  %in% unique(micro$me_name))  stop("Missing SD for SE calculation")  ##USERNAME:this will throw an error for BRFSS
##USERNAME:pull out SD and sample size

se_df<-micro[me_name==paste0(me, "_standard_deviation"), ]
setnames(se_df, c("mean", "standard_error"), c("sd", "sd_se"))

se_df<-se_df[, .(nid, ihme_loc_id, age_group_id, sex_id, year_id, sd, sd_se, sample_size)]

var_df<-micro[me_name==me,]


if(nrow(var_df) != nrow(se_df))  message("Unequal number of rows for variable and standard errors")


##USERNAME: for some reason, SD doesn't get made in collapse code for a handful of GBR rows in 2005.  All small sample sizes, these should get dropped later
micro<-merge(var_df, se_df, by=c("nid", "ihme_loc_id", "age_group_id", "sex_id", "year_id", "sample_size"), all.x=F, all.y=F)

micro[, sd_se:=NULL]  ##USERNAME: drop for rbind
micro[, standard_error:=NULL]
micro[, cv_lit:=0]




################### MERGE AND PLOT SCATTER #########################################
######################################################

full<-rbindlist(list(df, micro), use.names = T, fill=F)

message("Getting locs...")
locs <- get_location_metadata(version_id=149)[, .(location_id, ihme_loc_id, super_region_name, region_name, level)]  
full<-merge(full, locs, by="ihme_loc_id")
print("Done")

##USERNAME: drop small sample sizes
message(paste("Dropping", nrow(full[sample_size<=10]), "rows for sample size<=10"))
full<-full[sample_size>10]

##USERNAME: drop where sd is 0--sample size should take care of this
message(paste("Dropping", nrow(full[sd==0]), "rows where sd==0"))
full<-full[sd!=0]



if(me=="chl"){
  message(paste("Dropping", length(full[sd<3]), "rows where sd<3"))
  full<-full[sd<3,]
}

##USERNAME: only keep modeled age groups
full<-full[age_group_id>=10,]


pdf(file=plot_output)
if(T){##USERNAME: don't need these diagnostics again.. should split this script up
  for(sex in c(1,2)){
    message("Making diagnostic plots for sex_id ", sex)
    full.s<-full[sex_id==sex,]
    for(age in unique(full.s$age_group_id)){
      full.s<-full[age_group_id==age, ]
      correl<-cor(full.s$mean, full.s$sd, method="spearman")
      
      p<-ggplot(data=full.s, aes(x=sd))+
        geom_histogram(aes(fill=super_region_name),color="black")+
        #facet_wrap(~age_group_id)+
        ggtitle(paste("Distribution of observed SD, sex_id:", sex))+
        theme_classic()+
        theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
      print(p)
      
      
      p<-ggplot(data=full, aes(x=mean, y=sd, color=super_region_name))+
        geom_point(alpha=0.2)+
        geom_rug(alpha=0.4)+
        ggtitle(paste("Sex_id:", sex, "Spearman correlation:", round(correl, digits=3)))+
        #facet_wrap(~age_group_id)+
        #geom_smooth()+
        theme_classic()+
        theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
      print(p)
    }
  }
}
dev.off()




write.csv(full, file=output)
