#--------------------------------------------------------------
# Name: 
# Date: 2019-07-26
# Project: IPV/CSA data processing pipeline
# Purpose: Process and upload crosswalk versions for IPV/CSA bundles
#--------------------------------------------------------------


# setup -------------------------------------------------------------------

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '~/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

# load packages
require(data.table)
require(msm)
require(logitnorm)

# source functions
ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
source_shared_functions(c("get_bundle_version","save_crosswalk_version","get_location_metadata"))

repo_dir <- paste0(j_root,"FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source("FILEPATH/get_crosswalk_version.R")

ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
source_shared_functions(c("get_population"))
source(paste0(ckd_repo,"ckd_data_processing/utility_scripts/add_population_denominator.R"))

# set objects 
bidopen<-319
bidsave<-319
bvid<-16664
cvid <- 12866
acause<-"abuse_ipv_exp"
mrbrtname1 <- "who_ipv_LT_agespline_noprior"
mrbrtname2 <- "who_ipv_lifetime_logit_physsexsev"
ds<-"step4"
gbd_rnd<-6
output_file_name<-paste0("crosswalked_bid_",bidopen, "_bvid_", bvid,"_final.xlsx")
description<-"age split crosswalked with fixed age pattern, new data, ever_partnered crosswalk added, dropped data recovered, 10-9-2019"

# load bundle version data
bdt_orig <- get_crosswalk_version(crosswalk_version_id = cvid, export=FALSE)
bdt <- bdt_orig

# set output path 
bdt_xwalk_path<-paste0(j_root,"FILEPATH")

if(!dir.exists(bdt_xwalk_path)) dir.create(bdt_xwalk_path)

# set name for dt that gets dropped
drop_name<-paste0("dropped_data_cvid_",cvid,".xlsx")

#  -----------------------------------------------------------------------
#edit: make sure cv vars are all populated (!= NA)
bdt[is.na(cv_sexual_ipv),cv_sexual_ipv := 0]
bdt[is.na(cv_phys_ipv),cv_phys_ipv := 0]
bdt[is.na(cv_case_severe),cv_case_severe := 0]

#bdt <- bdt[1:50,]
# apply cross walks -------------------------------------------------------
message(paste0("Applying crosswalk"))

# split into dataset that needs crosswalking and one that doesn't
message(paste("ignoring any data that is marked with group_review = 0"))

toxwalk_dt <- copy(bdt[!(group_review %in% c(0)) & (cv_sexual_ipv==1 | cv_phys_ipv==1 | cv_case_severe==1), ])
noxwalk_dt <- copy(bdt[!(group_review %in% c(0)) & cv_sexual_ipv==0 & cv_phys_ipv==0 & cv_case_severe==0, ])

# edit 10/8/2019: don't have permissions to open former mrbrtname1, remapping:
mrbrtname1 <- paste0("step4_",mrbrtname1)
mrbrtname2 <- paste0("step4_",mrbrtname2)

# pull in mrbrt model
message(paste("reading in MRBRT model and making predictions"))
model_parent_dir<-paste0(j_root,"FILEPATH", mrbrtname1)
mrbrt_mod1<-readRDS(paste0(model_parent_dir,"/model_output.RDS"))
model_parent_dir<-paste0(j_root,"FILEPATH", mrbrtname2)
mrbrt_mod2<-readRDS(paste0(model_parent_dir,"/model_output.RDS"))

# make predictions for the logit difference of non-age-specific crosswalks
toxwalk_dt$d_sex <- toxwalk_dt$cv_sexual_ipv
toxwalk_dt$d_phys <- toxwalk_dt$cv_phys_ipv
toxwalk_dt$d_sev <- toxwalk_dt$cv_case_severe
toxwalk_dt$d_sex <- as.integer(toxwalk_dt$d_sex)

preds <- predict_mr_brt(mrbrt_mod2, toxwalk_dt, write_draws = T)
pred_draws2 <- as.data.table(preds$model_draws)
draw_cols <- paste0("draw_",0:999)
pred_draws2 <- unique(melt(pred_draws2, id.vars = names(pred_draws2)[!names(pred_draws2)%in%draw_cols],
                           variable.name = "draw_num"))
pred_draws2 <- pred_draws2[!is.na(pred_draws2$value),]
pred_draws2$value_physsexsev <- pred_draws2$value
pred_draws2$value <- NULL

toxwalk_dt[,d_curr_age:=(age_start+age_end)/2]
preds <- predict_mr_brt(mrbrt_mod1, toxwalk_dt, write_draws = T)
pred_draws1 <- as.data.table(preds$model_draws)
draw_cols <- paste0("draw_",0:999)
pred_draws1 <- unique(melt(pred_draws1, id.vars = names(pred_draws1)[!names(pred_draws1)%in%draw_cols],
                           variable.name = "draw_num"))
pred_draws1 <- pred_draws1[!is.na(pred_draws1$value),]
pred_draws1$value_agecurr <- pred_draws1$value
pred_draws1$value <- NULL

# adjust data 
message(paste("logit transforming non-0 data points"))
toxwalk_dt[mean!=0,logit_mean:=log((mean)/(1-mean))]

message(paste("applying the delta method to derive standard error of the logit-transformed means"))
toxwalk_dt[mean!=0,logit_standard_error:=sapply(1:nrow(toxwalk_dt[mean!=0]), function(i){
  mean_i <- toxwalk_dt[i,get("mean")]
  mean_se_i <- toxwalk_dt[i,get("standard_error")]
  deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
})]

# merge on mrbrt draws
xwalk_dt <- merge(toxwalk_dt, pred_draws2, by.x = c("d_phys", "d_sev", "d_sex"), 
                  by.y = c("X_d_phys", "X_d_sev", "X_d_sex"), allow.cartesian = T, all=TRUE)

xwalk_dt <- merge(xwalk_dt, pred_draws1, by.x = c("d_curr_age", "draw_num"), 
                  by.y = c("X_d_curr_age", "draw_num"), allow.cartesian = T, all=TRUE)

# adjust data.  
message(paste("adjusting data"))
xwalk_dt[,adj_mean:=logit_mean-value_physsexsev-value_agecurr]

# collapse across draws
message(paste("collapsing across draws"))
xwalk_dt<-xwalk_dt[,.(adj_logit_mean=mean(adj_mean),
                      adj_logit_standard_error=sd(adj_mean),
                      xwalk_val_physsexsev=mean(value_physsexsev),
                      xwalk_val_physsexsev_se=sd(value_physsexsev),
                      xwalk_val_agecurr=mean(value_agecurr),
                      xwalk_val_agecurr_se=sd(value_agecurr)),
                   by = c(names(xwalk_dt)[!names(xwalk_dt)%in%c("sample_size","cases",
                                                                "lower","upper","effective_sample_size",
                                                                "X_intercept","Z_intercept","draw_num",
                                                                "value_physsexsev","value_agecurr", "adj_mean")])]

# inverse logit
message(paste("inverse logit transforming non-0 data points"))
xwalk_dt[,adj_invlogit_mean:=invlogit(adj_logit_mean)]

message(paste("applying the delta method to derive standard error of the inverse logit transformed means"))
xwalk_dt[,adj_invlogit_standard_error:=sapply(1:nrow(xwalk_dt), function(i){
  mean_i <- xwalk_dt[i,get("adj_logit_mean")]
  mean_se_i <- xwalk_dt[i,get("adj_logit_standard_error")]
  deltamethod(~exp(x1)/(1+exp(x1)),mean_i,mean_se_i^2)
})]

# calculate standard error for mean 0 points
xwalk_dt[mean==0, adj_invlogit_mean := 0]
xwalk_dt[mean==0, adj_invlogit_standard_error := sqrt(standard_error^2+xwalk_val_physsexsev_se^2+xwalk_val_agecurr_se^2)]

# clean up - drop extra columns, set names, etc.
remove_cols<-c("logit_mean","logit_standard_error","adj_logit_mean","adj_logit_standard_error",
               "mean","standard_error","xwalk_val_physsexsev","xwalk_val_physsexsev_se",
               "xwalk_val_agecurr","xwalk_val_agecurr_se")
remove_cols<-remove_cols[remove_cols%in%names(xwalk_dt)]
xwalk_dt[,(remove_cols):=NULL]
setnames(xwalk_dt,c("adj_invlogit_mean","adj_invlogit_standard_error"),c("mean","standard_error"))

# clear other uncertainty information
xwalk_dt[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA)]

# change seq to crosswalk_parent_seq where crosswalk parent seq is na 
xwalk_dt$crosswalk_parent_seq[is.na(xwalk_dt$crosswalk_parent_seq)] <- xwalk_dt$seq[is.na(xwalk_dt$crosswalk_parent_seq)]
xwalk_dt[,seq:=NULL]

bdt <- rbindlist(list(noxwalk_dt, xwalk_dt), use.names=T, fill = T)


#   -----------------------------------------------------------------------


# clean up ----------------------------------------------------------------

# drop any data marked with group reveiw 0 
drop_rows<-nrow(bdt[group_review%in%c(0)])
dropped_dt<-copy(bdt[group_review%in%c(0)])
dropped_dt[,drop_reason:="group_review value of 0"]
message(paste("dropping",drop_rows,"of", nrow(bdt),"where group_review is marked 0"))
bdt<-bdt[!(group_review%in%c(0))]

# clear effective sample size where design effect is null
message("clearing effective_sample_size where design effect is NA")
bdt[is.na(design_effect),effective_sample_size:=NA]

# drop any loc ids that can't be uploaded to dismod
message("dropping locations not in the DisMod hierarchy")
dismod_locs<-get_location_metadata(9)[,location_id]
dropped_dt<-rbindlist(
  list(dropped_dt,copy(
    bdt[!(location_id%in%dismod_locs)])[,drop_reason:="loc_id not supported in Dismod modelling"]),
  use.names=T,fill=T)
bdt<-bdt[location_id%in%dismod_locs]

# write data
bdt$specificity[is.na(bdt$specificity)] <- ""
bdt$group_review[is.na(bdt$group_review)] <- ""
bdt$group[is.na(bdt$group)] <- ""
bdt$parent_seq_age[is.na(bdt$parent_seq_age)] <- ""
bdt$parent_seq_sex[is.na(bdt$parent_seq_sex)] <- ""
bdt$crosswalk_parent_seq[is.na(bdt$crosswalk_parent_seq)] <- ""
bdt$origin_seq[is.na(bdt$origin_seq)] <- ""
bdt$seq[is.na(bdt$seq)] <- ""

bdt$standard_error[bdt$standard_error>1] <- NA
bdt$standard_error[bdt$standard_error==0] <- NA
bdt$seq[!is.na(bdt$crosswalk_parent_seq)] <- NA

writexl::write_xlsx(list(extraction=bdt),paste0("FILEPATH",output_file_name))
writexl::write_xlsx(list(extraction=dropped_dt),paste0("FILEPATH",drop_name))

save_result <- save_crosswalk_version(bvid, data_filepath = paste0("FILEPATH",output_file_name),
                       description = description)
save_result

#   -----------------------------------------------------------------------


