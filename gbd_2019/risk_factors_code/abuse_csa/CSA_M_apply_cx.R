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

ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
source_shared_functions(c("get_population"))
source(paste0(ckd_repo,"FILEPATH/add_population_denominator.R"))

# set objects 
bid<-415
bvid<-15695
acause<-"abuse_csa_male"
mrbrtname1 <- "csa_Male_int_contact_perp"
mrbrtname2 <- "csa_male_recall_u15"
ds<-"step4"
gbd_rnd<-6
output_file_name<-paste0("crosswalked_bid_",bid, "_bvid_", bvid,".xlsx")
description<-"crosswalked_new_data_step4"

# load bundle version data
bdt<-get_bundle_version(bundle_version_id = bvid,export = F,transform = T)
bdt$cv_contact_noncontact[is.na(bdt$cv_contact_noncontact)] <- 0
bdt$cv_not_represent[is.na(bdt$cv_not_represent)] <- 0
bdt$cv_perp3[is.na(bdt$cv_perp3)] <- 0
bdt$cv_questionaire[is.na(bdt$cv_questionaire)] <- 0
bdt$cv_subnational[is.na(bdt$cv_subnational)] <- 0
bdt$cv_students[is.na(bdt$cv_students)] <- 0
bdt$cv_recall_under_age_15[is.na(bdt$cv_recall_under_age_15)] <- 0
bdt$cv_intercourse[is.na(bdt$cv_intercourse)] <- 0
bdt$cv_recall_over_age_15[is.na(bdt$cv_recall_over_age_15)] <- 0

# set output path 
bdt_xwalk_path<-'FILEPATH'
if(!dir.exists(bdt_xwalk_path)) dir.create(bdt_xwalk_path)

# set name for dt that gets dropped
drop_name<-paste0("dropped_data_bvid_",bvid,".xlsx")

#  -----------------------------------------------------------------------

#bdt <- bdt[1:50,]
# apply cross walks -------------------------------------------------------
message(paste0("Applying crosswalk"))

# split into both-sex and sex-specific data sets
message(paste("ignoring any data that is marked with group_review = 0"))
toxwalk_dt <- copy(bdt[!(group_review %in% c(0)) & (cv_intercourse==1 | cv_contact_noncontact==1 | cv_perp3==1 | cv_recall_under_age_15==1 | cv_recall_over_age_15==1), ])
noxwalk_dt <- copy(bdt[!(group_review %in% c(0)) & (cv_intercourse==0 & cv_contact_noncontact==0 & cv_perp3==0 & cv_recall_under_age_15==0 & cv_recall_over_age_15==0), ])

# pull in mrbrt model
message(paste("reading in MRBRT model and making predictions"))
model_parent_dir<-paste0(j_root,"FILEPATH", mrbrtname1)
mrbrt_mod1<-readRDS(paste0(model_parent_dir,"/model_output.RDS"))
model_parent_dir<-paste0(j_root,"FILEPATH", mrbrtname2)
mrbrt_mod2<-readRDS(paste0(model_parent_dir,"/model_output.RDS"))

# make predictions for the logit difference of non-age-specific crosswalks
# rename to match MR-BRT var names blahhh
toxwalk_dt$cv_recall_under_age15 <- toxwalk_dt$cv_recall_under_age_15
toxwalk_dt$cv_recall_over_age15 <- toxwalk_dt$cv_recall_over_age_15
toxwalk_dt$d_intercourse <- toxwalk_dt$cv_intercourse
toxwalk_dt$d_contact_noncontact <- toxwalk_dt$cv_contact_noncontact
toxwalk_dt$d_perp3 <- toxwalk_dt$cv_perp3

preds <- predict_mr_brt(mrbrt_mod2, toxwalk_dt, write_draws = T)
pred_draws2 <- as.data.table(preds$model_draws)
draw_cols <- paste0("draw_",0:999)
pred_draws2 <- unique(melt(pred_draws2, id.vars = names(pred_draws2)[!names(pred_draws2)%in%draw_cols],
                           variable.name = "draw_num"))
pred_draws2 <- pred_draws2[!is.na(pred_draws2$value),]
pred_draws2$value1 <- pred_draws2$value
pred_draws2$value <- NULL

preds <- predict_mr_brt(mrbrt_mod1, toxwalk_dt, write_draws = T)
pred_draws1 <- as.data.table(preds$model_draws)
draw_cols <- paste0("draw_",0:999)
pred_draws1 <- unique(melt(pred_draws1, id.vars = names(pred_draws1)[!names(pred_draws1)%in%draw_cols],
                           variable.name = "draw_num"))
pred_draws1 <- pred_draws1[!is.na(pred_draws1$value),]
pred_draws1$value2 <- pred_draws1$value
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
xwalk_dt <- merge(toxwalk_dt, pred_draws2, by.x = c("cv_recall_under_age15", "cv_recall_over_age15"), 
                  by.y = c("X_cv_recall_under_age15", "X_cv_recall_over_age15"), allow.cartesian = T, all=TRUE)

xwalk_dt <- merge(xwalk_dt, pred_draws1, by.x = c("d_intercourse", "d_contact_noncontact", "d_perp3", "draw_num"), 
                  by.y = c("X_d_intercourse", "X_d_contact_noncontact", "X_d_perp3", "draw_num"), allow.cartesian = T, all=TRUE)

# adjust data.
message(paste("adjusting data"))
xwalk_dt[,adj_mean:=logit_mean-value1-value2]

# collapse across draws
message(paste("collapsing across draws"))
xwalk_dt<-xwalk_dt[,.(adj_logit_mean=mean(adj_mean),
                      adj_logit_standard_error=sd(adj_mean),
                      xwalk_val_1=mean(value1),
                      xwalk_val_1_se=sd(value1),
                      xwalk_val_2=mean(value2),
                      xwalk_val_2_se=sd(value2)),
                   by = c(names(xwalk_dt)[!names(xwalk_dt)%in%c("sample_size","cases",
                                                                "lower","upper","effective_sample_size",
                                                                "X_intercept","Z_intercept","draw_num",
                                                                "value1","value2", "adj_mean")])]

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
xwalk_dt[mean==0, adj_invlogit_standard_error := sqrt(standard_error^2+xwalk_val_1_se^2+xwalk_val_2_se^2)]

# clean up - drop extra columns, set names, etc.
remove_cols<-c("logit_mean","logit_standard_error","adj_logit_mean","adj_logit_standard_error",
               "mean","standard_error","xwalk_val_1","xwalk_val_1_se",
               "xwalk_val_2","xwalk_val_2_se")
remove_cols<-remove_cols[remove_cols%in%names(xwalk_dt)]
xwalk_dt[,(remove_cols):=NULL]
setnames(xwalk_dt,c("adj_invlogit_mean","adj_invlogit_standard_error"),c("mean","standard_error"))

# clear other uncertainty information
xwalk_dt[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA)]

# change seq to crosswalk_parent_seq where crosswalk parent seq is na 
xwalk_dt$crosswalk_parent_seq <- xwalk_dt$seq
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

dim(bdt[is.na(bdt$mean),])
bdt <- bdt[!is.na(bdt$mean),]
dim(bdt[(is.na(bdt$lower) & is.na(bdt$upper) & is.na(bdt$standard_error) & 
           is.na(bdt$cases) & is.na(bdt$sample_size) & is.na(bdt$effective_sample_size))])
bdt <- bdt[!(is.na(bdt$lower) & is.na(bdt$upper) & is.na(bdt$standard_error) & 
               is.na(bdt$cases) & is.na(bdt$sample_size) & is.na(bdt$effective_sample_size)),]

bdt$d_intercourse <- NULL
bdt$d_contact_noncontact <- NULL
bdt$d_perp3 <- NULL
bdt$cv_recall_under_age15 <- NULL
bdt$cv_recall_over_age15 <- NULL
bdt$Z_intercept.x <- NULL
bdt$Z_intercept.y	<- NULL


writexl::write_xlsx(list(extraction=bdt),paste0(bdt_xwalk_path, output_file_name))
writexl::write_xlsx(list(extraction=dropped_dt),paste0(bdt_xwalk_path, drop_name))

save_xwalk_result <- save_crosswalk_version(bvid, data_filepath = paste0(bdt_xwalk_path, output_file_name),
                       description = description)
print(save_xwalk_result)
#   -----------------------------------------------------------------------

headmin
