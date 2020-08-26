#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Crosswalks
# Purpose: Run MRBRT on prepped CKD crosswalk data
#--------------------------------------------------------------

# setup -------------------------------------------------------

# Import packages
require(data.table)
require(ggplot2)

# source MRBRT functions
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0("FILEPATH","/function_lib.R"))

# Set seed
set.seed(555)

# Set objects from qsub arguments
args<-commandArgs(trailingOnly = T)
cv_ref_map_path<-as.character(args[1])
out_dir<-as.character(args[2])
mrbrt_dir<-as.character(args[3])
apply_trimming<-as.logical(args[4])
trim_pct<-as.numeric(args[5])
n_knots_age_midpoint<-as.numeric(args[6])
n_knots_age_sex<-as.numeric(args[7])
tail_priors<-as.logical(args[8])
deg_age_midpoint<-as.numeric(args[9])
deg_age_sex<-as.numeric(args[10])
offset_pct<-as.numeric(args[11])
map_row<-as.numeric(args[12])
apply_offset<-as.logical(args[13])
mod_type<-as.character(args[14])

# Set global vars based on arg settings
offset_lab<-ifelse(apply_offset,paste0("_offset_",offset_pct),"_no_offset")

#   -----------------------------------------------------------------------

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

# read in prepped data for specified stage
dt<-fread(paste0("FILEPATH",comparison,offset_lab,".csv"))

# calculate any desired cov columns
dt[,age_midpoint:=(age_start+age_end)/2]
dt[,age_sex:=age_midpoint*sex_id]

# drop rows where val or SE are NA
dt<-dt[!is.na(xform_val_se)]
dt<-dt[!is.na(xform_val)]

# construct args for cov_info
if (tail_priors){
  bspline_var_age_midpoint<-paste("1e-5",
                                  paste(rep("inf",n_knots_age_midpoint-1),collapse = ","),
                                  "1e-5",sep = ",")
  bspline_var_age_sex<-paste("1e-5",
                             paste(rep("inf",n_knots_age_sex-1),collapse = ","),
                             "1e-5",sep = ",")
}else{
  bspline_var_age_midpoint<-paste(rep("inf",n_knots_age_midpoint+1),collapse = ",")
  bspline_var_age_sex<-paste(rep("inf",n_knots_age_sex+1),collapse = ",")
}

# Add spline on age and FE on sex_id 
covs1<-list(cov_info(covariate = "age_midpoint",
                     design_matrix = "X",
                     degree = deg_age_midpoint,
                     bspline_gprior_mean = paste(rep("0",n_knots_age_midpoint+1),collapse = ","),
                     bspline_gprior_var = bspline_var_age_midpoint,
                     n_i_knots = n_knots_age_midpoint),
            cov_info(covariate = "age_sex",
                     design_matrix = "X",
                     bspline_gprior_mean = paste(rep("0",n_knots_age_sex+1),collapse = ","),
                     bspline_gprior_var = bspline_var_age_sex,
                     degree = deg_age_sex,
                     n_i_knots = n_knots_age_sex),
            cov_info(covariate = "sex_id",
                     design_matrix = "X"))

results<-run_mr_brt(
  output_dir = "FILEPATH",
  model_label = paste0("crosswalk_",comparison,offset_lab,"_trim_",trim_pct),
  data = dt,
  method=ifelse(apply_trimming,"trim_maxL","ReML"),
  trim_pct = ifelse(apply_trimming,trim_pct,NULL),
  study_id = "nid",
  mean_var = "xform_val",
  se_var = "xform_val_se",
  covs = covs1,
  overwrite_previous = TRUE)

# save model as an .rds file
saveRDS(results,file="FILEPATH")

# Predict ratio for each age/sex combination
new_dat<-data.table(age_midpoint=rep(seq(18,99,1),2),
                    sex_id=c(rep(1,length(seq(18,99,1))),
                             rep(2,length(seq(18,99,1)))))
new_dat[,age_sex:=age_midpoint*sex_id]

preds<-predict_mr_brt(model_object = results,newdata = new_dat)

# Write predictions to model directory
write.csv(preds$model_summaries,paste0(preds$working_dir,"model_predictions.csv"),row.names = F)

# Plot
trim_percentage<-as.numeric(trim_pct)*100
data_dt <- as.data.table(results$train_data)
data_dt[,lb:=xform_val-1.96*xform_val_se]
data_dt[,ub:=xform_val+1.96*xform_val_se]
model_dt <- as.data.table(preds$model_summaries)
model_dt[X_sex_id == 1, sex := "Male"][X_sex_id == 2, sex := "Female"]
setnames(model_dt,"X_sex_id","sex_id")
data_dt[w == 0, excluded := 0][w > 0, excluded := 1]
gamma_0_lab<-paste0("Gamma 0 val: ", unique(as.data.table(results$model_coefs)[,gamma_soln]))
gg <- ggplot() +
  geom_point(data = data_dt, aes(x = age_midpoint, y = xform_val, color = as.factor(excluded),
                                 size = 1/xform_val_se),
              alpha=0.25) +
  geom_line(data = model_dt, aes(x = X_age_midpoint, y = Y_mean), color="#117777") +
  geom_ribbon(data = model_dt, aes(x = X_age_midpoint, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
  scale_fill_manual(guide = F, values = c("#117777")) +
  labs(x = "Age",y=ifelse(mod_type=="log_diff","Log Ratio","Logit Difference"),caption = gamma_0_lab) +
  facet_wrap(~sex_id)+
  ggtitle(paste(comparison," - trim ",trim_percentage,"%")) +
  theme_bw() +
  theme(text = element_text(size = 15, color = "black")) +
  scale_color_manual(name = "", values = c("#AA4455","#4477AA"),labels = c("Included", "Trimmed"))
ggsave(plot=gg,filename = paste0(preds$working_dir,comparison,offset_lab,"_plot.pdf"),
       width = 14, height = 10)
