# Save water props CSV

rm(list=ls())

# Libraries ########################
library(data.table)
library(readr)
library(fst)

# Shared functions #####################
source("FILEPATH/get_demographics_template.R")

# Variables ###################
# args from launch
args <- commandArgs(trailingOnly = TRUE)
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))

release <- args[1]
gbd <- args[2] #for filepaths
loc <- args[3]
out.dir <- args[4]
out.dir.fst<-args[5]

measure<-18 #measure id, proportion
draw_col<-c(paste0("draw_",0:999))

#get demographics
demo<-get_demographics_template(gbd_team = "epi",release_id = release)
ages<-unique(demo$age_group_id)
rm(demo)

#load in data ###################
#Load in data if I lost the session
data<-read_fst(paste0(out.dir.fst,loc,".fst"))


# Create data files #############
#Prep the data to be in individual data files

#make versions that are M and F with all of the age groups
combo_all<-rbindlist(lapply(ages,function(age){
  print(paste0(age))
  temp_M<-as.data.table(copy(data))
  temp_M[,':='(age_group_id=age,
               sex_id=1,
               measure_id=measure)]

  temp_F<-as.data.table(copy(data))
  temp_F[,':='(age_group_id=age,
               sex_id=2,
               measure_id=measure)]

  temp<-rbind(temp_F,temp_M)

}))

#remove data
rm(data)


#grab column names
imp_bf_col<-c(paste0("total_imp_bf_",0:999))
unimp_bf_col<-c(paste0("total_unimp_bf_",0:999))
piped_bf_col<-c(paste0("total_piped_bf_",0:999))

imp_sc_col<-c(paste0("total_imp_sc_",0:999))
unimp_sc_col<-c(paste0("total_unimp_sc_",0:999))
piped_sc_col<-c(paste0("total_piped_sc_",0:999))

imp_no_treat_col<-c(paste0("total_imp_no_treat_",0:999))
unimp_no_treat_col<-c(paste0("total_unimp_no_treat_",0:999))
piped_no_treat_col<-c(paste0("total_piped_no_treat_",0:999))

b_piped_bf_col<-c(paste0("total_basic_piped_bf_",0:999))
b_piped_sc_col<-c(paste0("total_basic_piped_sc_",0:999))
b_piped_no_treat_col<-c(paste0("total_basic_piped_no_treat_",0:999))

hq_piped_bf_col<-c(paste0("total_hq_piped_bf_",0:999))

## save CSV ##########################################
### improved ##########
imp_bf<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",imp_bf_col)]
setnames(imp_bf,imp_bf_col,draw_col)

imp_sc<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",imp_sc_col)]
setnames(imp_sc,imp_sc_col,draw_col)

imp_no_treat<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",imp_no_treat_col)]
setnames(imp_no_treat,imp_no_treat_col,draw_col)

write_excel_csv(imp_bf,paste0(out.dir,"/improved/bf/",loc,".csv"))
write_excel_csv(imp_sc,paste0(out.dir,"/improved/sc/",loc,".csv"))
write_excel_csv(imp_no_treat,paste0(out.dir,"/improved/no_treat/",loc,".csv"))


### unimproved ##########
unimp_bf<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",unimp_bf_col)]
setnames(unimp_bf,unimp_bf_col,draw_col)

unimp_sc<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",unimp_sc_col)]
setnames(unimp_sc,unimp_sc_col,draw_col)

unimp_no_treat<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",unimp_no_treat_col)]
setnames(unimp_no_treat,unimp_no_treat_col,draw_col)

write_excel_csv(unimp_bf,paste0(out.dir,"/unimproved/bf/",loc,".csv"))
write_excel_csv(unimp_sc,paste0(out.dir,"/unimproved/sc/",loc,".csv"))
write_excel_csv(unimp_no_treat,paste0(out.dir,"/unimproved/no_treat/",loc,".csv"))

### basic piped ##########
b_piped_bf<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",b_piped_bf_col)]
setnames(b_piped_bf,b_piped_bf_col,draw_col)

b_piped_sc<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",b_piped_sc_col)]
setnames(b_piped_sc,b_piped_sc_col,draw_col)

b_piped_no_treat<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",b_piped_no_treat_col)]
setnames(b_piped_no_treat,b_piped_no_treat_col,draw_col)

write_excel_csv(b_piped_bf,paste0(out.dir,"/basic_piped/bf/",loc,".csv"))
write_excel_csv(b_piped_sc,paste0(out.dir,"/basic_piped/sc/",loc,".csv"))
write_excel_csv(b_piped_no_treat,paste0(out.dir,"/basic_piped/no_treat/",loc,".csv"))

### HQ piped ##########
#This one is only the boil/filer and the other treatment types are zeroed
hq_piped_bf<-combo_all[,.SD, .SDcols=c("location_id","year_id","age_group_id","sex_id","measure_id",hq_piped_bf_col)]
setnames(hq_piped_bf,hq_piped_bf_col,draw_col)

hq_piped_sc<-copy(hq_piped_bf)

write_excel_csv(hq_piped_bf,paste0(out.dir,"/hq_piped/bf/",loc,".csv"))
