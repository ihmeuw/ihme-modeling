#####
####Code to collapse microdata
#####
rm(list = ls())

#install.packages('SDMTools')
#install.packages('alr3')
#install.packages('metafor')
#libraries
library(stats)
library(SDMTools)
library(ggplot2)
library (data.table)
library(dplyr)
library(metafor)
library(msm)
library(readxl)
library (alr3)
library(openxlsx)
library(stringr)

root <- ifelse(Sys.info()[1]=="Windows", drive, drive)
path<-paste0(root, "FILEPATH")

#read in file
ratio<-read.csv(paste0(root,"FILEPATH"),stringsAsFactors = FALSE)
ratio$age<-ratio$age_start
ratio<-as.data.frame(ratio)
ratio$variable<-as.factor(ratio$variable)

pdf(paste0("FILEPATH"))
for (j in unique(ratio$variable)){
  temp<-ratio[ratio$variable==j,]
p<-ggplot(temp, aes(x=dm_ref, y=value, color=region_name))+geom_point()+xlab('reference')+ylab('alt')+geom_abline(slope=1,intercept=0)+theme_bw()+ggtitle (paste0('scatterplot of prevalences between dm defined as \n',j,' and fpg>126 mg/dl or tx'))
q<-ggplot(temp, aes(x=ratio_alt_ref))+geom_histogram()+geom_vline(xintercept=1)+theme_bw()+ggtitle(paste0('distribution of ratio (',j,'/ fpg>126 mg/dl or tx)'))
print(p)
print(q)
}
dev.off()


ratio2 <-ratio

#MR_BRT CW test

# data for meta-regression model

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean and standard error variables

t<-read.csv("FILEPATH", stringsAsFactors = F)

dat_original<-t
dat_original<-dat_original[(dat_original$case_diagnostics !='cw_fpg_2_dm|no tx'),]
dat_original[, c("pre", "alt")] <- do.call("rbind", strsplit(dat_original$case_diagnostics, split = "cw_"))

#recode PPG to OGTT
dat_original$alt[dat_original$alt=='fpg_126_ogtt_200_ppg_200_notx'|dat_original$alt=='fpg_126_ppg_200_notx' ]<-'fpg_126_ogtt_200_notx'
dat_original$alt[dat_original$alt=='fpg_126_ogtt_200_ppg_200_tx' ]<-'fpg_126_ogtt_200_tx'
dat_original$alt[dat_original$alt=='fpg_126_ppg_220_notx']<-'fpg_126_ogtt_220_notx'
dat_original$alt[dat_original$alt=='fpg_140_ppg_200_notx' ]<-'fpg_140_ogtt_200_notx'
dat_original$alt[dat_original$alt=='fpg_140_ppg_200_tx' ]<-'fpg_140_ogtt_200_tx'
dat_original$alt[dat_original$alt=='ppg_180_tx' ]<-'ogtt_180_tx'

dat_original<- dat_original %>%
 mutate(
    fpg_126_tx=ifelse(case_diagnostics=="cw_fpg_126_tx",1,0)
 )

#test
dat_original$age[dat_original$age<30]<-20
dat_original$age[dat_original$age>=30 & dat_original$age<40  ]<-30
dat_original$age[dat_original$age>=40 & dat_original$age<50  ]<-40
dat_original$age[dat_original$age>=50 & dat_original$age<60  ]<-50
dat_original$age[dat_original$age>=60 & dat_original$age<70  ]<-60
dat_original$age[dat_original$age>=70  ]<-70

reference_var <- "fpg_126_tx"
reference_value <- 1
mean_var <- "mean"
se_var <- "se"

ratio2<-read.csv("FILEPATH")
# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be calculated as alternative/reference
dat_metareg <-as.data.frame(ratio2)
ratio_var<-'ratio_alt_ref'
ratio_se_var<-'se_ratio_alt_ref'
nid<-'nid'
study_id<-'study_id'
alt<-'variable'
age<-'age'
year_start<-'year_start'
sex<-'sex'
region_name<-'region_name'
prev_ref_var <- "dm_ref"
se_prev_ref_var <- "se_ref"
prev_alt_var <- "value"
se_prev_alt_var <- "se_mean"
#cov_names<-'age'

#####
orig_vars <- c(mean_var, se_var, reference_var)
metareg_vars <- c(ratio_var, ratio_se_var,nid,alt,age,year_start,sex,region_name,prev_ref_var, se_prev_ref_var, prev_alt_var, se_prev_alt_var)

# fit the MR-BRT model
repo_dir <-"FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

output_dir = "FILEPATH"
trim_percent<-.1
test<-"" #'TEST: exclude Central Latin America'

dat_metareg<-dat_metareg[dat_metareg$region_name !='Central Latin America',]
df_total = data.frame()
#i<-'fpg_115_tx'
for (i in unique(dat_metareg$variable)){

  model_label = paste0(dat_metareg$alt,'_logit')

  #dat_original.1<-dat_original[dat_original$alt | dat_original$alt=='fpg_126_tx',]
  #dat_metareg.1<-dat_metareg[dat_metareg$variable==i,]

  #LOGIT
  tmp_orig <- as.data.frame(dat_metareg) %>%
    .[, orig_vars] %>%
    #setnames(orig_vars, c("mean", "se", "ref")) %>%
    mutate(ref = if_else(ref == reference_value, 1, 0, 0))

  tmp_metareg <- as.data.frame(dat_metareg.1) %>%
    .[, metareg_vars] %>%
    setnames(metareg_vars, c("ratio", "ratio_se","nid",'alt','age','year_start','sex','region_name','prev_ref','se_prev_ref','prev_alt','se_prev_alt'))


  # logit transform the original data
  # -- SEs transformed using the delta method
  tmp_orig$mean_logit <- log(tmp_orig$mean / (1-tmp_orig$mean))
  tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
    mean_i <- tmp_orig[i, "mean"]
    se_i <- tmp_orig[i, "se"]
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })

  # logit transform the meta-regression data
  # -- alternative
  tmp_metareg$prev_logit_alt <- log(tmp_metareg$prev_alt / (1-tmp_metareg$prev_alt))
  tmp_metareg$se_prev_logit_alt <- sapply(1:nrow(tmp_metareg), function(i) {
    prev_i <- tmp_metareg[i, "prev_alt"]
    prev_se_i <- tmp_metareg[i, "se_prev_alt"]
    deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
  })

  # -- reference
  tmp_metareg$prev_logit_ref <- log(tmp_metareg$prev_ref / (1-tmp_metareg$prev_ref))
  tmp_metareg$se_prev_logit_ref <- sapply(1:nrow(tmp_metareg), function(i) {
    prev_i <- tmp_metareg[i, "prev_ref"]
    prev_se_i <- tmp_metareg[i, "se_prev_ref"]
    deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
  })


  tmp_metareg$diff_logit <- tmp_metareg$prev_logit_alt - tmp_metareg$prev_logit_ref
  tmp_metareg$se_diff_logit <- sqrt(tmp_metareg$se_prev_logit_alt^2 + tmp_metareg$se_prev_logit_ref^2)


  # fit the MR-BRT model
  tmp_metareg<-read.csv("FILEPATH")
  source(paste0("FILEPATH",mr_brt_functions.R))

      fit1 <- run_mr_brt(
      output_dir = "FILEPATH",
      model_label = paste0('dm_logit'),
      data = tmp_metareg,
      mean_var = "diff_logit",
      se_var = "se_diff_logit",
      overwrite_previous = TRUE,
    study_id =c("study_id"),
    method='trim_maxL',
    trim_pct=.10
    )


  preds <- predict_mr_brt(fit1, newdata = tmp_orig)$model_summaries
  tmp_orig$adj_logit <- preds$Y_mean
  tmp_orig$se_adj_logit <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

  tmp_orig2 <- tmp_orig %>%
    mutate(
      prev_logit_adjusted_tmp = mean_logit - adj_logit,
      se_prev_logit_adjusted_tmp = sqrt(se_logit^2 + se_adj_logit^2),
      prev_logit_adjusted = ifelse(ref == 1, mean_logit, prev_logit_adjusted_tmp),
      se_prev_logit_adjusted = ifelse(ref == 1, se_logit, se_prev_logit_adjusted_tmp),
      prev_adjusted = exp(prev_logit_adjusted)/(1+exp(prev_logit_adjusted)),
      lo_logit_adjusted = prev_logit_adjusted - 1.96 * se_prev_logit_adjusted,
      hi_logit_adjusted = prev_logit_adjusted + 1.96 * se_prev_logit_adjusted,
      lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)),
      hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted))
    )


  tmp_orig2$se_adjusted <- sapply(1:nrow(tmp_orig2), function(i) {
    mean_i <- tmp_orig2[i, "prev_logit_adjusted"]
    mean_se_i <- tmp_orig2[i, "se_prev_logit_adjusted"]
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
  })


  # 'final_data' is the original extracted data plus the new variables
  final_data <- cbind(
    dat_original.1,
    as.data.frame(tmp_orig2)[, c("ref", "prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
  )


  final_data$ref_type<-ifelse(final_data$ref==1,'Reference','Alternate')
  final_data$mean_unadjust<-final_data$mean
  #graph<-final_data
  graph<-final_data[final_data$ref==0,]
  df <- data.frame(graph)
  df_total <- rbind(df_total,df)

  #diagnostic forest plot
  pdf(paste0(output_dir,'/', model_label,'/plot_adjust_v_orig_data.pdf'))
  p<-ggplot(fit1$train_data,aes(x=obs_id, y=diff_logit, size=1/(se_diff_logit*se_diff_logit), color=as.factor(w)))+geom_point()+coord_flip()+geom_hline(yintercept=preds$Y_mean)+geom_hline(yintercept=preds$Y_mean_lo,lty=2)+geom_hline(yintercept=preds$Y_mean_hi,lty=2)
  p<-p+ggtitle(paste0('Microdata: Relationship between logit difference and SE relative to adjustment factor and \n uncertainty for  ', i, ' and trim percent=', trim_percent))
  print(p)
  dev.off()


  pdf(paste0(output_dir,'/', model_label,'/mean_vs_adjusted_',i,'.pdf'))
  p<-ggplot(graph, aes(x=mean_unadjust, y=prev_adjusted))+geom_point()+geom_abline(slope=1, intercept=0)+theme_bw()+ggtitle (paste0('MR_BRT adjustment for ', i,' \n adjustment to ref (fpg>126 mg/dl (7mmol/L) + tx) with trim ', trim_percent))
  #p<-p+geom_point(aes(x=se, y=se_adjusted), color='red')

  print(p)
  dev.off()

  #########PLOTTING MR BRT
  bundle<-i
  cv<-i
  trim_percent<-.1
  plot_mr_brt_custom <- function(model_object, continuous_vars = NULL, dose_vars = NULL, print_cmd = FALSE) {

    library("FILEPATH", lib.loc = "FILEPATH")

    wd <- model_object[["working_dir"]]

    contvars <- paste(continuous_vars, collapse = " ")
    dosevars <- paste(dose_vars, collapse = " ")

    if (!file.exists(paste0(wd, "model_coefs.csv"))) {
      stop(paste0("No model outputs found at '", wd, "'"))
    }

    contvars_string <- ifelse(
      !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
    dosevars_string <- ifelse(
      !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )

    cmd <- paste(
      c("export PATH=FILEPATH",
        "source FILEPATH",
        paste(
          "python FILEPATH/mr_brt_visualize.py",
          "--mr_dir", wd, "--plot_note Bundle:", bundle, ", Crosswalk:", cv, ", Trim:", trim_percent, ", ", Sys.Date(),
          contvars_string,
          dosevars_string
        )
      ), collapse = " && "
    )

    print(cmd)
    cat("To generate plots, run the following command in a qlogin session:")
    cat("\n\n", cmd, "\n\n")
    cat("Outputs will be available in:", wd)

    path <- paste0(wd, "tmp.txt")
    readr::write_file(paste("/bin/bash -c", cmd), path)

    #Job specifications
    username <- Sys.getenv("USER")
    m_mem_free <- "-l m_mem_free=2G"
    fthread <- "-l fthread=2"
    runtime_flag <- "-l h_rt=00:05:00"
    jdrive_flag <- "-l archive"
    queue_flag <- "-q all.q"
    shell_script <- path
    script <- path
    errors_flag <- paste0("-e FILEPATH", Sys.getenv("USER"), "FILEPATH")
    outputs_flag <- paste0("-o FILEPATH", Sys.getenv("USER"), "FILEPATH")
    job_text <- strsplit(wd, "/")
    job_name <- paste0("-N funnel_",job_text[[1]][7] )

    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P project_name", outputs_flag, errors_flag, shell_script, script)

    system(job)

  }

  plot_mr_brt_custom(fit1)
}


pdf(paste0(output_dir,'/', 'all_mean_vs_adjusted.pdf'))
for (z in unique(df_total$alt)){
  temp<-df_total[df_total$alt==z,]

p<-ggplot(temp, aes(x=mean_unadjust, y=prev_adjusted))+geom_point()+geom_abline(slope=1, intercept=0)+theme_bw()+ggtitle (paste0('MR_BRT adjustment for ', z,' \n adjustment to ref (fpg>126 mg/dl (7mmol/L) + tx) with trim ', trim_percent))
#p<-p+geom_point(aes(x=se, y=se_adjusted), color='red')

print(p)
}
dev.off()

