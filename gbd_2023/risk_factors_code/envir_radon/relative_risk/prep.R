#-------------------Header------------------------------------------------
# Purpose: Prep radon extractions for MR-BRT

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
  } else {
  j_root <- "J:"
  h_root <- "H:"
  }

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# load the functions
repo_dir <- "FILEPATH"
source(file.path(repo_dir,"mr_brt_functions.R"))

#------------------Directories--------------------------------------------------


version <- 8 

in_dir <- "/FILEPATH"
out_dir <- file.path("FILEPATH/model",version)
results_dir <- file.path("FILEPATH/model",version)

dir.create(out_dir,recursive=T)
dir.create(results_dir,recursive=T)

#------------------Format input data/scale RRs----------------------------------

dt <- read.xlsx(file.path(in_dir,"CC_envir_radon_kcausey_Apr_19_2019.xlsm"),
                sheet="extraction", startRow=4, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(dt) <- read.xlsx(file.path(in_dir,"CC_envir_radon_kcausey_Apr_19_2019.xlsm"),
                       sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names

dt <- dt[is.na(exclude)]

# for version 4, we will keep only the studies that Kate used in GBD2019 for the "linear" version

dt <- dt[custom_select_rows %in% c("categorical","continuous_only","spline","categorical_only")]

# calculate scaling factor for different units
dt[,exp_unit:=cohort_exp_unit_rr]
dt[is.na(exp_unit),exp_unit:=cc_exp_unit_rr]

dt[,exp_scale_factor:=1]
dt[exp_unit=="hBq/m^3", exp_scale_factor:=100]
dt[exp_unit=="pCi/L", exp_scale_factor:= 37] #http://www.icrpaedia.org/index.php/Radon:_Units_of_Measure

# For continouous effect size, determine the exposure level/range of the effect
dt[,exp_level_dr := cohort_exp_level_dr]
dt[is.na(exp_level_dr), exp_level_dr := cc_exp_level_dr]

# For categorical determine the exposure level/range of the effect

# first choice is to use the study-given mean/median of the group
# otherwise we will use the midpoint. When there is no lower value given, replace with 0. When there is no upper value given, replace with 2 times the minimum of the upper category. (somewhat arbitrary decision)
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr) & is.na(custom_unexp_level_lower), custom_unexp_level_lower:=0]
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr) & is.na(custom_exp_level_upper), custom_exp_level_upper:=2*custom_exp_level_lower]

dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(cc_unexp_level_rr),cc_unexp_level_rr:=(custom_unexp_level_lower+custom_unexp_level_upper)/2]
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(cc_exp_level_rr),cc_exp_level_rr:=(custom_exp_level_lower+custom_exp_level_upper)/2]

dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr), exp_level_dr:=cc_exp_level_rr-cc_unexp_level_rr] 

# Check
dt[,.(custom_select_rows,cc_exposed_def,cc_exp_level_rr,cc_unexposed_def,cc_unexp_level_rr,exp_level_dr,exp_unit,exp_scale_factor)]

# Scale effect size to the given range
dt[,scaled_effect_mean:=effect_size^(100/(exp_level_dr*exp_scale_factor))]
dt[,scaled_effect_lower:= lower ^ (100/(exp_level_dr*exp_scale_factor))]
dt[,scaled_effect_upper:= upper ^ (100/(exp_level_dr*exp_scale_factor))]

dt[,sample_size := cohort_sample_size_total]
dt[is.na(sample_size), sample_size := cc_cases+cc_control]

# calculate exposure value for each row for spline
dt[,exposure:=(cc_exp_level_rr+cc_unexp_level_rr)/2*exp_scale_factor]
# for continuous studies plot at median
dt[is.na(exposure),exposure:=custom_exp_50th*exp_scale_factor]

# Check
dt[,.(custom_select_rows,cc_exposed_def,cc_exp_level_rr,cc_unexposed_def,cc_unexp_level_rr,exposure,exp_level_dr,exp_unit,exp_scale_factor)]

# isolate to needed columns
dt <- dt[,c("nid","location_name","design","outcome_type","effect_size","lower","upper","CI_uncertainty_type_value","exp_unit",
            "exp_level_dr","exp_scale_factor","scaled_effect_mean","scaled_effect_lower","scaled_effect_upper","sample_size",
            "custom_select_rows",grep("cv_",names(dt),value = T),"exposure"),
         with=F]

# replace cv_selection_bias with 2 when missing
dt[is.na(cv_selection_bias),cv_selection_bias:=2]

# calculate log effect size and log se for MR-BRT
dt[,log_effect_size:=log(scaled_effect_mean)]
dt[,log_effect_size_se:= (log(scaled_effect_upper)-log(scaled_effect_lower))/
     (qnorm(0.5+(CI_uncertainty_type_value/200))*2)]

# predict unknown SE based on sample size
dt[,scaled_n:=1/sqrt(sample_size)]

SE_mod <- lm(data=dt,
             log_effect_size_se ~ 0 + scaled_n)
dt[,log_effect_size_se_predicted:=predict.lm(SE_mod,newdata = dt)]
dt

pred_dt <- data.table(sample_size = seq(50,1000000,10))
pred_dt[,scaled_n := 1/sqrt(sample_size)]
pred_dt[, log_effect_size_se:= predict.lm(SE_mod,
                                          newdata=pred_dt)]

pdf(file.path(out_dir,"impute_se.pdf"))

# plot to check fit for imputing SE
ggplot(data=dt,aes(x=sample_size,y=log_effect_size_se))+
  geom_point(aes(color=custom_select_rows))+
  geom_line(data=pred_dt,
            aes(x=sample_size,y=log_effect_size_se))+
  geom_vline(xintercept=dt[is.na(log_effect_size_se),sample_size],color="grey")+
  scale_x_log10()+
  scale_y_continuous(limits=c(0,2.5))+
  scale_color_manual(values=c("categorical_only"= "#af8dc3","categorical"= "#af8dc3","continuous"="#7fbf7b","continuous_only"="#7fbf7b","spline"="#f48b8b"))+
  theme_bw()

dev.off()

# replace missing SE with predicted
dt[is.na(log_effect_size_se),log_effect_size_se:=log_effect_size_se_predicted]


#------------------Format covariates--------------------------------------------
# Format covariates according to protocol given by MSCM for the new MR-BRT

# remove covariates that only have 1 level
dt[,cv_exposure_self_report:=NULL]
dt[,cv_subpopulation:=NULL]
dt[,cv_outcome_selfreport:=NULL]
dt[,cv_outcome_unblinded:=NULL]
dt[,cv_reverse_causation:=NULL]
dt[,cv_confounding_nonrandom:=NULL]

# create dummy covariates for cv_selection_bias because it has 3 levels (the rest have 2 levels only)

dt[,cv_selection_bias_1:=ifelse(cv_selection_bias==1,1,0)]
dt[,cv_selection_bias_2:=ifelse(cv_selection_bias==2,1,0)]
dt[,cv_selection_bias:=NULL] # remove unnecessary variable


# weight the sample sizes in accordance with the guidance for the new MRBRT
# for studies with >1 observations, we want to weight the SEs to represent smaller sample sizes
# this is so each individual study is weighted less
for (n in dt$nid){
  if (nrow(dt[nid==n])>1){
    n_observations <- nrow(dt[nid==n])
    dt[nid==n,log_se_weighted:=(log_effect_size_se*sqrt(n_observations))]
  }
}

dt[is.na(log_se_weighted),log_se_weighted:=log_effect_size_se]

#------------------Save input data----------------------------------------------
write.csv(dt,file.path(out_dir,"input_data.csv"),row.names = F)

