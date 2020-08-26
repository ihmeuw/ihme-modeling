#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - sex splits
# Purpose: Run MRBRT on prepped CKD sex split data 
# Note: Decided not to include super region as a cov in 
# MRBRT model b/c the only super region with significantly
# different effect was Latin America and the Caribbean
# but the only countries w/ Both sex data in this SR were 
# Brazil and Haiti for stage 3 so not worth splitting out 
#--------------------------------------------------------------

# setup -------------------------------------------------------

# Import packages
require(data.table)
require(ggplot2)
require(msm)
require(logitnorm)

# source MRBRT functions
repo_dir <- paste0("FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

# source shared functions
source(paste0("FILEPATH/function_lib.R"))
source_shared_functions("get_location_metadata")

# Set seed
set.seed(555)

# Set objects from qsub arguments
args<-commandArgs(trailingOnly = T)
bid<-as.numeric(args[1])
acause<-as.character(args[2])
mrbrt_dir<-as.character(args[3])
apply_trimming<-as.logical(args[4])
trim_pct<-as.numeric(args[5])
apply_offset<-as.logical(args[6])
offset_pct<-as.numeric(args[7])
mod_type<-as.character(args[8])
bvid<-as.numeric(args[9])

# Set global vars based on arg settings
offset_lab<-ifelse("FILEPATH")
epi_path<-paste0("FILEPATH")
#   -----------------------------------------------------------------------

# read in prepped data for specified stage
dt<-fread("FILEPATH")

# drop rows where xwalk vals are NA
dt<-dt[!is.na(xform_val)]
dt<-dt[!is.na(xform_val_se)]

# create study id var
dt[is.na(underlying_nid),sid:=paste0(nid,"_",location_id,"_",year_start)]
dt[!is.na(underlying_nid),sid:=paste0(underlying_nid,"_",nid,"_",location_id,"_",year_start)]

# add super region as a covariate:

# get location info 
locs<-get_location_metadata(22)

# merge on super region name and id
data<-merge(data,locs[,.(location_id,super_region_name)],by="location_id")

# format super region name - remove commas, dashes, spaces, and capitals
dt[,super_region_name:=tolower(super_region_name)]
dt[,super_region_name:=gsub(",|-| ","_",super_region_name)]

# create vector of sr names for use later 
sr_dummies<-unique(dt[,super_region_name])

# cast wide to make super region into a dummy var
dt[,t:=1]
lhs<-paste0(names(dt)[!names(dt)%in%c("super_region_name","super_region_id","t")],collapse = "+")
cast_formula<-paste0(lhs,"~super_region_name")
dt<-dcast(dt,as.formula(cast_formula))
dt[,(sr_dummies):=lapply(.SD,function(x) ifelse(is.na(x),0,x)),
   .SDcols=sr_dummies]

# use cov_info function to format as MRBRT covariate
cov_list<-lapply(sr_dummies,function(x) cov_info(x,"X"))

results<-run_mr_brt(
  output_dir = "FILEPATH",
  model_label = paste0("sex_split_by_sr_bid_",bid,"_bvid_",bvid,offset_lab,"_trim_",trim_pct),
  data = dt,
  method=ifelse(apply_trimming,"trim_maxL","ReML"),
  trim_pct = ifelse(apply_trimming,trim_pct,NULL),
  covs = cov_list,
  remove_x_intercept = T,
  study_id = "sid",
  mean_var = "xform_val",
  se_var = "xform_val_se",
  overwrite_previous = TRUE)

pred_dt<-unique(dt[,..sr_dummies])
preds<-predict_mr_brt(model_object = results,newdata = pred_dt)

# Write predictions to model directory
write.csv(preds$model_summaries,paste0(preds$working_dir,"model_predictions.csv"),row.names = F)

# Create objects for plotting 
trim_percentage<-(trim_pct)*100
model_dt <- as.data.table(preds$model_summaries)
data_dt <- as.data.table(results$train_data)
data_dt[,study_lab:=paste0(nid," ",location_name," ",year_start," ",year_end)]
data_dt[,xform_val_lower:=xform_val-1.96*xform_val_se]
data_dt[,xform_val_upper:=xform_val+1.96*xform_val_se]
data_dt[w == 0, excluded := 0][w > 0, excluded := 1]

# Cut off lower and upper vals where they go outside the x-axis limits
data_dt[xform_val_lower<=-4,xform_val_lower:=-3.95]
data_dt[xform_val_upper>=4,xform_val_upper:=3.95]

# pull out gamma 0 label
gamma_0_lab<-paste0("Gamma 0 val: ", unique(as.data.table(results$model_coefs)[!is.na(gamma_soln),gamma_soln]))

# rename x covariates
xcov_names <- names(model_dt)[grepl("^X", names(model_dt))]
setnames(model_dt, xcov_names, gsub("^X_", "", xcov_names))
model_dt <- merge(model_dt, unique(dt[, c(sr_dummies), with = F]), by = sr_dummies)

pdf("FILEPATH"),
    width=12,height=10)
for(sr in sr_dummies){
  es_mean <- round(model_dt[get(sr) == 1, Y_mean],3)
  es_lower <- round(model_dt[get(sr) == 1, Y_mean_lo],3)
  es_upper <- round(model_dt[get(sr) == 1, Y_mean_hi],3)
  es_se<-unique(as.data.table(results$model_coefs)[x_cov==sr,beta_var])
  graph_dt <- copy(data_dt[get(sr) == 1])

  if (mod_type=="log_diff"){
    overall_effect_exp<-round(1/exp(es_mean),3)
    overall_effect_se_exp<-deltamethod(~exp(x1),mean = es_mean,cov = es_se)
    overall_effect_lower_exp<-round(overall_effect_exp-1.96*overall_effect_se_exp,3)
    overall_effect_upper_exp<-round(overall_effect_exp+1.96*overall_effect_se_exp,3)
    
    effect_size_lab<-paste0("Effect size: ",round(overall_effect,3), " (",overall_effect_lower," to ",overall_effect_upper,")",
                            ", Inverted to compare w/ DisMod: ",overall_effect_exp," (",overall_effect_lower_exp," to ",overall_effect_upper_exp,")")
  }else{
    effect_size_lab<-paste0("Effect size: ",round(es_mean,3), " (",es_lower," to ",es_upper,")")
  }
  
  gg <- ggplot() +
    geom_point(data = graph_dt, aes(x = xform_val, y = study_lab, color = as.factor(w))) +
    geom_errorbarh(data = graph_dt, aes(y = study_lab, xmin = xform_val_lower, xmax = xform_val_upper, color = as.factor(w)),height=0) +
    geom_vline(xintercept = es_mean, linetype = "dashed", color = "darkorchid") +
    geom_vline(xintercept = 0) +
    geom_rect(data = graph_dt[1,], xmin = es_lower, xmax = es_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
    labs(x = "Effect Size", y = "",title = paste0("Sex ratios - ",sr),subtitle = effect_size_lab,caption = gamma_0_lab) +
    xlim(-4, 4) +
    scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
    theme_bw()+
    theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
          axis.text.y = element_text(size = 10,hjust = 0))
  print(gg)
}
dev.off()
