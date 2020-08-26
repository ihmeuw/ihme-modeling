## Purpose: Clean NRVD lit data and prep for dismod

library(ggplot2)
library(data.table)
library(openxlsx)

################### PATHS AND ARGS #########################################
################################################################
upload_mes<-c("aort", "mitral")
decomp_step <- "2"

input_folder<-"FILEPATH"

output_folder<-"FILEPATH"

code_root<-"FILEPATH"
central<-"FILEPATH"


################### SCRIPTS #########################################
################################################################

source(paste0(code_root, "utility/get_recent.R"))  
source(paste0(code_root, "utility/job_hold.R"))  
source(paste0(code_root, "utility/data_tests.R"))

source(paste0("rm_upload_epi.R"))

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "upload_bundle_data.R"))


################### GET DATA #########################################
################################################################

files<-list.files(input_folder, full.names=T)
files<-files[!grepl("\\~\\$", files)]

nrvd<-rbindlist(lapply(files, function(x){
  temp<-as.data.table(read.xlsx(x, sheet="extraction", na.strings="<NA>"))[-1]
  temp[, source_file:=x]
  return(temp)
}), fill=T)

nrvd<-nrvd[!is.na(me_name)]
nrvd[, "cv*":=NULL]

## clean some study level covs
nrvd[is.na(cv_tx), cv_tx:=3]
nrvd[is.na(cv_only_degen), cv_only_degen:=1]
nrvd[is.na(cv_prop_degen), cv_prop_degen:=0]
nrvd[, cv_preserved_lvef:=NULL] 

num_cols<-c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "age_start", "age_end", "year_start", "year_end", "is_outlier")
lapply(X = num_cols, FUN = check_class, df=nrvd, class="numeric")


## if cases/sample sizes were given, create the mean
nrvd[is.na(mean), mean:=cases/sample_size]

################### ID OUTLIERS #########################################
################################################################

nrvd[me_name=="mitral" & nid==322303, is_outlier:=1]
nrvd[me_name=="mitral" & nid==322273, is_outlier:=1]


nrvd[me_name=="aortic" & nid==322303, is_outlier:=1]

nrvd[measure=="incidence", is_outlier:=1]
nrvd<-nrvd[measure!="incidence"]

nrvd[measure=="cfr" & ihme_loc_id=="GBR_44776"] ##sy: n

outliers<-nrvd[is_outlier==1]
nrvd<-nrvd[is_outlier!=1 | is.na(is_outlier)]


################### COMBINE MODERATE+SEVERE FOR PREV #########################################
################################################################

nrvd[, seq:=1:.N]

cols<-c("age_start", "age_end", "sex", "year_start", "year_end", "location_id", "urbanicity_type", "nid", "me_name", "measure",  "source_file", grep("cv_*", names(nrvd)[!grepl("cv_hemo_stat", names(nrvd))], value=T)) 

## cast on hemodynamic status
to_agg<-nrvd[cv_hemo_stat %in% c("moderate", "severe"), c(cols, "seq", "mean", "cases", "sample_size", "cv_hemo_stat"), with=F]
to_agg.d<-data.table::dcast(to_agg, formula(paste0(paste0(c(cols, "sample_size"), collapse="+"), "~cv_hemo_stat")), value.var="mean", subset=.(measure %in% c("prevalence", "incidence")))

## add proporiton from moderate to prop from severe to combine
to_agg.d[, c("moderate, severe"):=rowSums(.SD, na.rm=F), .SDcols=c("moderate", "severe")]
to_agg.d$aggd<-ifelse(is.na(to_agg.d[["moderate, severe"]]), 0, 1)


meltd<-melt(to_agg.d, id.vars=setdiff(names(to_agg.d), c("moderate", "severe", "moderate, severe")), 
            value.name="mean", variable.name="cv_hemo_stat")
meltd<-meltd[aggd==1,]  
meltd<-meltd[cv_hemo_stat=="moderate, severe",] 
setnames(meltd, c("cv_hemo_stat", "mean"), c("new_hemo", "new_mean"))
meltd[, new_seq:=1:.N]

## merge back on
nrvd.t<-merge(nrvd, meltd, by=setdiff(names(meltd),c("new_hemo", "new_mean", "aggd", "new_seq")), all.x=T, all.y=T)
## drop duplicated rows
nrvd.t<-nrvd.t[!duplicated(new_seq) | is.na(new_seq)]
## replace old estimtes
nrvd.t[!is.na(new_mean), mean:= new_mean]
nrvd.t[!is.na(new_mean), cv_hemo_stat:=new_hemo]

nrvd.t[, c("new_hemo", "new_mean", "new_seq"):=NULL]


nrvd.t[cv_tx %in% c(1,2) & measure=="prevalence", measure:="proportion"]




################### CLEAN CFR #########################################
################################################################

nrvd.t[measure=="cfr" & is.na(cfr_time), cfr_time:=1]
nrvd.t[, cfr_time:=as.numeric(cfr_time)]
nrvd.t[measure=="cfr", mean:=-log(1-mean)/cfr_time]
nrvd.t[measure=="cfr", measure:="mtwith"]
## adjust person-years
nrvd.t[measure=="mtwith"]


################### FIX AGE RANGES #########################################
################################################################
age_fix<-nrvd.t[age_issue==1 & !is.na(mean_age)]

nrvd.t[age_issue==1 & !is.na(mean_age) & !is.na(age_sd), `:=`(og_age_start=age_start, og_age_end=age_end)]
nrvd.t[age_issue==1 & !is.na(mean_age) & !is.na(age_sd), `:=` (age_start=mean_age-2*age_sd, age_end=mean_age+2*age_sd)]


################### USE IDEAL DATA #########################################
################################################################
nrvd.t<-rbind(nrvd.t, outliers, fill=T)
nrvd.t[is.na(cv_hemo_stat), cv_hemo_stat:="moderate, severe"]
nrvd.t[is.na(cv_symptomatic), cv_symptomatic:=3]
nrvd.t[is.na(cv_tx), cv_tx:=3]
nrvd.t[is.na(cv_prop_degen), cv_prop_degen:=0]
nrvd.t[, is_outlier:=as.integer(is_outlier)]
nrvd.t[is.na(is_outlier), is_outlier:=0]
nrvd.t[, `:=` (age_start=as.numeric(age_start), age_end=as.numeric(age_end))]

dismod<-nrvd.t[cv_hemo_stat %in% c("moderate, severe", "moderate", "severe") & cv_tx==3 & cv_symptomatic==3 & cv_prop_degen!=1]


prop_degen<-nrvd.t[cv_prop_degen==1]

survival_data<-nrvd.t[measure=="cfr" & cv_tx %in% c(1,2)]
prop_tx_data<-nrvd.t[measure %in% c("prevalence", "incidence", "proportion") & cv_tx %in% c(1,2)]



################### ADD IN DATA THAT IS MOD OR SEVERE #########################################
################################################################

dismod[cv_hemo_stat=="moderate", form:="moderate"]
dismod[cv_hemo_stat=="severe", form:="severe"]
dismod[aggd==1, form:="Aggregated moderate+severe"]
dismod[is.na(form), form:="Extracted moderate+severe"]
dismod[, seq:=NA]


setnames(dismod, names(dismod), tolower(names(dismod)))

################### UPLOAD EPI DATA #########################################
################################################################
if(T){ 
  for(me in upload_mes){
    b_id<-ifelse(me=="aort", 2987, 2993)
    d_step <- ifelse(decomp_step == "iterative", "iterative", paste0("step", decomp_step))
    upload_folder<-paste0("FILEPATH")
    upload_decomp(b_id, upload_folder, decomp_step = decomp_step)
    upload_bundle_data(bundle_id = 2987, decomp_step = "iterative", filepath = "FILEPATH")
  }
}
