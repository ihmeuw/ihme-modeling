#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation - Crosswalks
# Purpose: Run MRBRT on prepped CKD crosswalk data -  NOT SEX SPECIFIC
#--------------------------------------------------------------

user <- Sys.info()["user"]
share_path <- "FILEPATH"
code_general <- "FILEPATH"

# Import packages
require(data.table)
require(ggplot2)
require(msm)
require(logitnorm)

# source MRBRT functions
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(code_general, "/function_lib.R"))

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
offset_lab <- "FILENAME"
epi_path <- "FILEPATH"
#   -----------------------------------------------------------------------

# read in prepped data for specified stage
dt<-fread("FILEPATH")

# drop rows where xwalk vals are NA
dt<-dt[!is.na(xform_val)]
dt<-dt[!is.na(xform_val_se)]

# create study id var
dt[is.na(underlying_nid),sid:=paste0(nid,"_",location_id,"_",year_start)]
dt[!is.na(underlying_nid),sid:=paste0(underlying_nid,"_",nid,"_",location_id,"_",year_start)]

results<-run_mr_brt(
  output_dir = ""FILEPATH,
  model_label = paste0("sex_split_bid_",bid,"_bvid_",bvid,offset_lab,"_trim_",trim_pct),
  data = dt,
  method=ifelse(apply_trimming,"trim_maxL","ReML"),
  trim_pct = ifelse(apply_trimming,trim_pct,NULL),
  study_id = "sid",
  mean_var = "xform_val",
  se_var = "xform_val_se",
  overwrite_previous = TRUE)

# save model as an .rds file
saveRDS(results,file="FILEPATH")

preds<-predict_mr_brt(model_object = results,newdata = data.table(X_intercept=1))

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
data_dt[xform_val_lower<=-2.5,xform_val_lower:=-2.49]
data_dt[xform_val_upper>=2.5,xform_val_upper:=2.49]

# Pull out effect size values for labels
overall_effect<-unique(model_dt[,Y_mean])
overall_effect_lower<-round(unique(model_dt[,Y_mean_lo]),3)
overall_effect_upper<-round(unique(model_dt[,Y_mean_hi]),3)
overall_effect_se<-unique(as.data.table(results$model_coefs)[,beta_var])

if (mod_type=="log_diff"){
  overall_effect_exp<-round(1/exp(overall_effect),3)
  overall_efffect_se_exp<-deltamethod(~exp(x1),mean = overall_effect,cov = overall_effect_se)
  overall_effect_lower_exp<-round(overall_effect_exp-1.96*overall_efffect_se_exp,3)
  overall_effect_upper_exp<-round(overall_effect_exp+1.96*overall_efffect_se_exp,3)
  
  effect_size_lab<-paste0("Effect size: ",round(overall_effect,3), " (",overall_effect_lower," to ",overall_effect_upper,")",
                          ", Inverted to compare w/ DisMod: ",overall_effect_exp," (",overall_effect_lower_exp," to ",overall_effect_upper_exp,")")
}else{
  effect_size_lab<-paste0("Effect size: ",round(overall_effect,3), " (",overall_effect_lower," to ",overall_effect_upper,")")
}

# pull out gamma 0 label
gamma_0_lab<-paste0("Gamma 0 val: ", unique(as.data.table(results$model_coefs)[!is.na(gamma_soln),gamma_soln]))

gg <- ggplot() +
  geom_point(data = data_dt, aes(x = xform_val, y = study_lab, color = as.factor(excluded))) +
  geom_errorbarh(data = data_dt, aes(y = study_lab, xmin = xform_val_lower, xmax = xform_val_upper, color = as.factor(excluded)),height=0) +
  geom_vline(xintercept = overall_effect, linetype = "dashed", color = "darkorchid") +
  geom_vline(xintercept = 0) +
  geom_rect(data = model_dt[1,], xmin = overall_effect_lower, xmax = overall_effect_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, 
            fill = "darkorchid") +
  labs(x = "Effect Size", y = "",title = paste0("Sex split model, bundle id ",bid),subtitle = effect_size_lab,caption = gamma_0_lab) +
  scale_x_continuous(limits=c(-2.5, 2.5),expand=c(0,0)) +
  scale_color_manual(name = "", values = c( "0" = "red","1" = "midnightblue"), 
                     labels = c("0" = "Trimmed", "1" = "Included")) +
  theme_bw()+
  theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
        legend.text = element_text(size=12),axis.text.y = element_text(size = 8),
        plot.title = element_text(hjust = 0.5,size=12),plot.subtitle = element_text(hjust = 0.5,size=10))
ggsave(plot = gg, filename = paste0(preds$working_dir,"bid_",bid,"_bvid_",bvid,offset_lab,"_trim_",trim_pct,"_plot.pdf"),
       width=12,height=10)
