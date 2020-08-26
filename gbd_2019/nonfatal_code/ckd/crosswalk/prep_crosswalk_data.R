#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Crosswalks
# Purpose: Prep microdata sources to estimate coefficients
# for the estimating equation and acr threshold crosswalks 
# outside of DisMod
# Note: Some age/sex groups returned in ubcov tabulations have a SE 
#       that is NA - this is a known issue with the svy package b
# TODO: fix mapping in ubcov and re-extract nids 293226, 133397, 280192 
#--------------------------------------------------------------

# setup -------------------------------------------------------

# Import packages
require(data.table)
require(logitnorm)
require(ggplot2)
require(msm)

# Source functions 
ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
source(paste0(ckd_repo,"FILEPATH/data_processing_functions.R"))
source_shared_functions("get_location_metadata")

set.seed(555)

# Set objects from qsub arguments
args<-commandArgs(trailingOnly = T)
cv_ref_map_path<-args[1]
ubcov_age_agg_file<-args[2]
ubcov_age_spec_file<-args[3]
out_dir<-args[4]
plot_dir<-args[5]
apply_offset<-as.logical(args[6])
offset_pct<-as.numeric(args[7])
mean_col<-as.character(args[8])
se_col<-as.character(args[9])
map_row<-as.numeric(args[10])
mod_type<-as.character(args[11])
print(map_row)
match_vars<-c(args[12:length(args)])

# Set global vars based on arg settings
offset_lab<-ifelse(apply_offset,paste0("_offset_",offset_pct),"_no_offset")

#   -----------------------------------------------------------------------

# process data ------------------------------------------------------------

# Read in cv_reference map
cv_ref_map<-fread(cv_ref_map_path)

# Pull in settings from map 
stg<-cv_ref_map[map_row,stage]
message(stg)
study_level_cov<-unlist(strsplit(cv_ref_map[map_row,cv],split = ","))
message(paste(study_level_cov,collapse = " "))
study_level_cov_ref<-unlist(strsplit(cv_ref_map[map_row,reference],split = ","))
message(paste(study_level_cov_ref,collapse = " "))
study_level_cov_alt<-unlist(strsplit(cv_ref_map[map_row,alt],split = ","))
message(paste(study_level_cov_alt,collapse = " "))
comparison<-paste0(stg,"_",paste0(study_level_cov,collapse="_"),"_",
                   paste0(study_level_cov_alt,collapse="_"),"_to_",
                   paste0(study_level_cov_ref,collapse="_"),"_",mod_type)
message(comparison)
xwalk_type<-cv_ref_map[map_row,xwalk_type]
message(xwalk_type)

# Read in collapsed data file
if (xwalk_type=="age_spec"){
  dt<-fread(ubcov_age_spec_file)
}
if (xwalk_type=="all_age"){
  dt<-fread(ubcov_age_agg_file)
}

# Subset match vars to only those that in the dt 
match_vars<-match_vars[match_vars%in%names(dt)]

# Create a unique dt - duplicates for NIDs 293226, 133397, and 280192 
dt<-unique(dt)

# Don't keep subnational tabulations of the same survey - only use highest level
# tabulation
loc_dt<-get_location_metadata(22)
loc_dt_level<-loc_dt[,.(ihme_loc_id,level)]
dt<-merge(dt,loc_dt_level,by="ihme_loc_id")
dt[,min_level:=min(level),by=c("nid","survey_name")]
dt<-dt[min_level==level]

# Make age end of ubcov tabulations 99 for the spline
dt[age_end==max(age_end),age_end:=99]

# Create crosswalk variables - 
# (1) Equation: pull out equation -- if it doesn't say "mdrd" or "cg" in the var name, 
#     then it was caluclated with CKD-EPI
dt[,equation:=ifelse(grepl("mdrd",var),"mdrd",ifelse(grepl("cg",var),"cg","ckd_epi"))]
# Remove the suffixes 
dt[,var:=gsub("_mdrd","",var)]
dt[,var:=gsub("_cg","",var)]
# (2) Threshold: pull out albuminuria threshold - assign it to 30 when missing
dt[grepl("albuminuria",var),threshold:=tstrsplit(x = var,split="_",keep = 2)]
dt[grepl("albuminuria",var)&is.na(threshold),threshold:="30"]
# Remove the suffixes on var and create a stage variable
dt[,var:=gsub(paste(paste0("_",unique(dt[,na.omit(threshold)])),collapse = "|"),"",var)]
dt[,stage:=factor(var)]
# Drop "var"
dt[,var:=NULL]

# Estimating equation comparisons only apply to data age 18+ (all under 18 sources use dt
# Schqartz)
dt<-dt[age_start>=18]

# There are a few rows that have a standard_error that is NA. This is an known issue with the svy package 
# that returns NA when it is calculating the mean/se of the data. 
# Manually calculate SE for these points ignoring survey design, using the formula 
# se = sqrt((p*1-p)/n) where p is the mean and n is the sample size 
dt[is.na(standard_error),standard_error:=sqrt((mean*(1-mean))/sample_size)]

# Create crosswalk input datasets for each stage 
dt<-dt[stage==stg]

# Only keep rows for specified comparison
dt<-copy(dt[eval(parse(text = paste0(study_level_cov,"%in% c('",study_level_cov_alt,"','",study_level_cov_ref,"')",collapse = "&")))])

# Check that there are no duplicate rows
message("checking for duplicate rows of data")
if(!(identical(unique(dt),dt))) stop("There are duplicate rows in the microdata_dt, please supply a unique dt")

# Subset to a dt where data was extracted using only reference 
message("separating into reference and alterate datasets")
dt_ref<-dt[eval(parse(text = paste0(study_level_cov,"== c('",study_level_cov_ref,"')",collapse = "&")))]
dt_alt<-dt[eval(parse(text = paste0(study_level_cov,"== c('",study_level_cov_alt,"')",collapse = "&")))]

# Set names of comparison columns
comp_cols<-c(mean_col,se_col,study_level_cov)
setnames(dt_ref,comp_cols,paste0(comp_cols,"_ref"))
setnames(dt_alt,comp_cols,paste0(comp_cols,"_alt"))

# Merge ref and alt data by match variables
message(paste("merging reference and alternate dataset. matching on",paste(match_vars,collapse = ", ")))
dt_merge<-merge(dt_ref,dt_alt,by=match_vars)

# If offset is TRUE, add a %age of the median of all non-zero datapoints
if (apply_offset){
  message(paste0("applying offset to data where both num and denom are 0. offset is calculated to be ", offset_pct,"% of the median of all non-zero datapoints.
                 NOTE: THIS IS NOT BEING CALCULATED BY AGE/SEX/ANY OTHER STRATIFYING VARIABLE"))
  offset_val_ref<-median(dt_merge[get(paste0(mean_col,"_ref"))!=0,get(paste0(mean_col,"_ref"))],
                     na.rm = T)*offset_pct
  offset_val_alt<-median(dt_merge[get(paste0(mean_col,"_alt"))!=0,get(paste0(mean_col,"_alt"))],
                         na.rm = T)*offset_pct
  message(paste("offset val for ref def is", offset_val_ref))
  message(paste("offset val for alt def is", offset_val_alt))
  dt_merge[get(paste0(mean_col,"_ref"))==0,offset_flag_ref:=1]
  dt_merge[get(paste0(mean_col,"_alt"))==0,offset_flag_alt:=1]
  
  message("dropping any rows where mean_ref and mean_alt are both 0")
  nrow_drops<-nrow(dt_merge[(offset_flag_ref==1&offset_flag_alt==1)])
  message(paste("this affects",nrow_drops,"rows"))
  dt_merge<-dt_merge[!(offset_flag_ref%in%c(1)&offset_flag_alt%in%c(1))]
  
  dt_merge[offset_flag_alt==1,(paste0(mean_col,"_alt")):=offset_val_ref]
  dt_merge[offset_flag_ref==1,(paste0(mean_col,"_ref")):=offset_val_alt]
  # Recalculate SE after applying the offset
  dt_merge[offset_flag_alt==1,(paste0(se_col,"_alt")):=wilson_se(prop = get(paste0(mean_col,"_alt")),n = sample_size.y)]
  dt_merge[offset_flag_ref==1,(paste0(se_col,"_ref")):=wilson_se(prop = get(paste0(mean_col,"_ref")),n = sample_size.x)]
}

# Transform alternate and ref into appropriate space
if (mod_type=="log_diff"){
  message("Calculating log ratio and log ratio se")
  dt_merge[,ratio:=get(paste0(mean_col,"_alt"))/get(paste0(mean_col,"_ref"))]
  
  # Log transform ratio
  dt_merge[,log_ratio:=log(ratio)]
  
  # Calculate SE of the ratio with the formula presented in crosswalking guide
  dt_merge[,ratio_se:=prop_se_div(m1 = get(paste0(mean_col,"_alt")),
                                  se1 = get(paste0(se_col,"_alt")), 
                                  m2 = get(paste0(mean_col,"_ref")), 
                                  se2 = get(paste0(se_col,"_ref")))]
  
  if (!apply_offset){
    message("dropping rows where ratio and ratio SE are NA or inf b/c offsetting was not implemented")
    # If offset if FALSE drop any place where mean or SE is NA 
    drop_count<-nrow(dt_merge[(is.na(ratio)|is.na(ratio_se)|is.infinite(ratio)|is.na(ratio_se))])
    tot_count<-nrow(dt_merge)
    dt_merge<-dt_merge[!(is.na(ratio)|is.na(ratio_se)|is.infinite(ratio)|is.na(ratio_se))]
    message(paste0("dropped ",drop_count," of ", tot_count," rows"))
  }
  
  # Calculate SE of the xformed ratio using the delta method
  message("applying the delta method to derive standard error of the log-transformed ratio")
  dt_merge[,log_ratio_se:=sapply(1:nrow(dt_merge), function(i){
    ratio_i <- dt_merge[i,ratio]
    ratio_se_i <- dt_merge[i,ratio_se]
    deltamethod(~log(x1),ratio_i,ratio_se_i^2)
  })]
  
  setnames(dt_merge,c("ratio","ratio_se","log_ratio","log_ratio_se"),c("val","val_se","xform_val","xform_val_se"))
}

if (mod_type=="logit_diff"){
  message("Calculating logit difference and logit difference se")
  # Logit the study means
  dt_merge[,logit_alt:=logit(get(paste0(mean_col,"_alt")))]
  dt_merge[,logit_ref:=logit(get(paste0(mean_col,"_ref")))]
  
  # Calcualte to difference
  dt_merge[,logit_difference:=logit_alt-logit_ref]
  
  # Calculate SE both logit means
  message("applying the delta method to derive standard error of the logit-transformed means")
  dt_merge[,logit_alt_se:=sapply(1:nrow(dt_merge), function(i){
    mean_i <- dt_merge[i,get(paste0(mean_col,"_alt"))]
    mean_se_i <- dt_merge[i,get(paste0(se_col,"_alt"))]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]
  dt_merge[,logit_ref_se:=sapply(1:nrow(dt_merge), function(i){
    mean_i <- dt_merge[i,get(paste0(mean_col,"_ref"))]
    mean_se_i <- dt_merge[i,get(paste0(se_col,"_ref"))]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]
  
  # Assume additive variance to calculate SE of logit diff
  dt_merge[,logit_difference_se:=sqrt(logit_alt_se^2+logit_ref_se^2)]
  
  if (!apply_offset){
    message("dropping rows where ratio and ratio SE are NA or inf b/c offsetting was not implemented")
    # If offset if FALSE drop any place where mean or SE is NA 
    drop_count<-nrow(dt_merge[(is.na(logit_difference)|is.na(logit_difference_se)|is.infinite(logit_difference)|is.infinite(logit_difference_se))])
    tot_count<-nrow(dt_merge)
    dt_merge<-dt_merge[!(is.na(logit_difference)|is.na(logit_difference_se)|is.infinite(logit_difference)|is.infinite(logit_difference_se))]
    message(paste0("dropped ",drop_count," of ", tot_count," rows"))
  }
  
  setnames(dt_merge,c("logit_difference","logit_difference_se"),c("xform_val","xform_val_se"))
  
}

# Make dummary variable for comparison
message("creating study-level covariate dummy.")
dt_merge[,(paste0("cv_",study_level_cov,"_",study_level_cov_alt)):=1]

# Remove .x and .y vars 
merge_vars<-grep("\\.x|\\.y",names(dt_merge),value=T)
dt_merge[,(merge_vars):=NULL]

# Write outputs
write.csv(x = dt_merge, file = paste0(out_dir,comparison,offset_lab,".csv"),row.names = F)

# Make age_bin and study_lab vars for plotting
dt_merge[,age_bin:=paste(age_start,"to",age_end)]

# Write plots
pdf(paste0(plot_dir,comparison,"_scatters.pdf"),width=12)
gg<-if(nrow(dt_merge)>0){
  ggplot(data=dt_merge,aes(x=mean_alt,y=mean_ref))+
    geom_point(size=2)+
    facet_wrap(~age_bin,scales="free")+
    geom_abline()+
    theme_bw()+
    xlab(paste0("Mean - ",study_level_cov_alt))+
    ylab(paste0("Mean - ",study_level_cov_ref))+
    ggtitle(paste0(simpleCap(gsub("_"," ",stg)), " ",study_level_cov," crosswalk - ",
                   study_level_cov_alt, " to ", study_level_cov_ref))}
print(gg)
dev.off()  

# Plot ratios
pdf(paste0(plot_dir,comparison,"_ratios_by_age",offset_lab,".pdf"),width=12)
gg<-if(nrow(dt_merge)>0){
  ggplot(data=dt_merge,aes(x=age_start,y=xform_val))+
    geom_jitter(color="blue",alpha=0.1,width = 1)+
    theme_bw()+
    ggtitle(paste0(simpleCap(gsub("_"," ",stg)), " ",study_level_cov," crosswalk - ",
                   study_level_cov_alt, " to ", study_level_cov_ref))}
print(gg)
dev.off()

# Plot mean vs standard error
pdf(paste0(plot_dir,comparison,"_mean_se",offset_lab,".pdf"),width=12)
gg<-ggplot(data=dt_merge,use.names = T,fill=T)+
  aes(x=xform_val,y=xform_val_se)+
  geom_point(color="blue",alpha=0.1)+
  theme_bw()+
  ggtitle(paste0(simpleCap(gsub("_"," ",stg)), " ",study_level_cov," crosswalk - ",
                 study_level_cov_alt, " to ", study_level_cov_ref))
print(gg)
dev.off() 