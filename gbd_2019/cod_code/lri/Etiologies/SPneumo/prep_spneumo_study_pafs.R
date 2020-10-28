## Clean data, calculate study-level Strep pneumo PAF in the absence of vaccine ##
## the values from this analysis will be run through an age-integrating meta-regression
## (BradMod) to get this value by age. ##
########################################################################################################
## Prep ##
########################################################################################################

library(metafor)
library(msm)
library(matrixStats)
library(plyr)
library(ggplot2)
library(reshape2)
library(boot)
source("filepath/get_covariate_estimates.R")
source("filepath/mr_brt_functions.R")
source("filepath/plot_mr_brt_function.R")
source("filepath/run_mr_brt_function.R")

## Do we want to fill VE_VT for missing studies?
fill_vt <- FALSE

# Which vaccine type to use?
vtype <- "PCV7"

# IHME location ids
  locs <- read.csv("filepath")

# IHME age group information
  age_info <- read.csv("filepath")

# Pull PCV coverage #
  pcv_cov_pull <- data.frame(get_covariate_estimates(covariate_id=210, location_id="all", year_id=1990:2019, decomp_step="step4"))
    setnames(pcv_cov_pull, c("mean_value","lower_value","upper_value"), c("pcv_cov","pcv_cov_lower","pcv_cov_upper"))
  pcv_cov <- merge(pcv_cov_pull, locs[,c("location_id","super_region_name","region_name")], by="location_id")

########################################################################################################
## Create a file for vaccine serotype coverage ##
  ## Mostly used to calculate final PAF but used a little in this script ##
########################################################################################################
# This file is from Johnson et al. and gives the estimated fraction of all S. pneumo serotypes covered by
# each of the PCV types (7, 10, 13)
  sero_cov <- read.csv("filepath")

# Regions are matched as closely
# as possible to what was used in GBD 2017 
  pcv_cov$region <- with(pcv_cov, ifelse(region_name=="Oceania","Oceania",
                                 ifelse(region_name=="High-income North America","North America",
                                 ifelse(region_name=="Western Europe","Europe",
                                 ifelse(region_name=="Central Europe", "Europe",
                                 ifelse(region_name=="Eastern Europe","Europe",
                                 ifelse(region_name=="Australasia","North America",
                                 ifelse(region_name=="High-income Asia Pacific","Asia",
                                 ifelse(super_region_name=="Sub-Saharan Africa","Africa",
                                 ifelse(super_region_name=="North Africa and Middle East","Africa",
                                 ifelse(super_region_name=="Southeast Asia, East Asia, and Oceania","Asia",
                                 ifelse(super_region_name=="South Asia","Asia",
                                 ifelse(super_region_name=="Central Europe, Eastern Europe, and Central Asia","Asia","LAC"))
                                 )))))))))))

  pcv_cov <- join(pcv_cov, sero_cov, by=c("region"))

# Create a composite indicator for pcv_vt_cov by combinding products of variances
  pcv_cov$pcv_vt_cov <- pcv_cov$pcv_cov * pcv_cov$covmean
  pcv_cov$pcv_cov_std <- (pcv_cov$pcv_cov_upper - pcv_cov$pcv_cov_lower) / 2 / qnorm(0.975)
  pcv_cov$vtcov_std <- (pcv_cov$covupper - pcv_cov$covlower) / 2 / qnorm(0.975)

  pcv_cov$pcv_vt_cov_std <- with(pcv_cov, sqrt(vtcov_std^2 * pcv_cov^2 + pcv_cov_std^2 * covmean^2 + vtcov_std^2 * pcv_cov_std^2))

## Save this intermediate product for later
  write.csv(pcv_cov, "filepath", row.names=F)

###############################################################################################################
  # This is the PCV efficacy data ##
###############################################################################################################
  pcv_data <- read.csv("filepath")
  pcv_data <- subset(pcv_data, is_outlier==0)

  ## Assumption is that the vaccine efficacy against any invasive disease can be used if
  ## there isn't a value for vt invasive disease ##
  pcv_data$ve_invasive <- ifelse(is.na(pcv_data$ve_vt_invasive), pcv_data$ve_all_invasive, pcv_data$ve_vt_invasive)
  pcv_data$ve_invasive_lower <- ifelse(is.na(pcv_data$ve_vt_invasive_lower), pcv_data$ve_all_invasive_lower, pcv_data$ve_vt_invasive_lower)
  pcv_data$ve_invasive_upper <- ifelse(is.na(pcv_data$ve_vt_invasive_upper), pcv_data$ve_all_invasive_upper, pcv_data$ve_vt_invasive_upper)
  pcv_data$ve_invasive_std <- (pcv_data$ve_invasive_upper / 100 - pcv_data$ve_invasive_lower / 100) / qnorm(0.975) / 2

  # make in log space, find log se using delta method
    pcv_data$ln_vi <- log(1-pcv_data$ve_invasive/100)
    pcv_data$ln_vi_se <- sapply(1:nrow(pcv_data), function(i) {
      ratio_i <- 1 - pcv_data[i, "ve_invasive"] / 100
      ratio_se_i <- pcv_data[i, "ve_invasive_std"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })

    pcv_data$ve_pneumonia_std <- (pcv_data$ve_pneumonia_upper / 100 - pcv_data$ve_pneumonia_lower / 100) / qnorm(0.975) / 2
    pcv_data$ln_ve <- log(1-pcv_data$ve_pneumonia/100)
    pcv_data$ln_ve_se <- sapply(1:nrow(pcv_data), function(i) {
      ratio_i <- 1 - pcv_data[i, "ve_pneumonia"] / 100
      ratio_se_i <- pcv_data[i, "ve_pneumonia_std"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })


  ## to fill in missing ve_invasive estimates,
  ## use a meta-analysis with an age-stratification ##
  pcv_data$age_cat <- ifelse(pcv_data$age_end <5, "Under-5", "Over-5")
  rma_vi <- rma(ve_invasive ~ age_cat, sei=ve_invasive_std, data=pcv_data[pcv_data$study_type=="RCT",])
  rma_vi <- rma(ln_vi ~ age_cat, sei=ln_vi_se, data=pcv_data[pcv_data$study_type=="RCT",])
  summary(rma_vi)
  forest(rma_vi)

  p <- predict(rma_vi)
  ve_pred <- data.frame(pred_ve_vt = p$pred, pred_ve_vt_sd = p$se, pred_ve_vt_lower = p$ci.lb, pred_ve_vt_upper = p$ci.ub)
  ve_pred <- ve_pred[1:2,]
  ve_pred$age_cat <- c("Over-5","Under-5")
  pcv_data <- join(pcv_data, ve_pred, by="age_cat")

  if(fill_vt==TRUE) {
    pcv_data$ve_invasive <- ifelse(is.na(pcv_data$ve_invasive), 1-exp(pcv_data$pred_ve_vt), pcv_data$ve_invasive)
    pcv_data$ve_invasive_upper <- ifelse(is.na(pcv_data$ve_invasive_upper), 1-exp(pcv_data$pred_ve_vt_upper), pcv_data$ve_invasive_upper)
    pcv_data$ln_vi <- ifelse(is.na(pcv_data$ln_vi), pcv_data$pred_ve_vt, pcv_data$ln_vi)
    pcv_data$ln_vi_se <- ifelse(is.na(pcv_data$ln_vi_se), pcv_data$pred_ve_vt_sd, pcv_data$ln_vi_se)
  }

  # Data are only usable if they include estimates against Pneumonia and Invasive Disease
    pcv_data <- subset(pcv_data, !is.na(ve_invasive))
    pcv_data <- subset(pcv_data, !is.na(ve_pneumonia))

  ## Determine the vaccine serotype coverage in the study ##
    # This helps merge
      pcv_data$vtype <- ifelse(pcv_data$vtype=="PCV23","PCV13", as.character(pcv_data$vtype))

    pcv_data$year_id <- floor((pcv_data$year_start + pcv_data$year_end) / 2)
    pcv_data <- join(pcv_data, 
                     pcv_cov[,c("location_id","year_id","region","pcv_cov","pcv_cov_lower","pcv_cov_upper","pcv_vt_cov","pcv_vt_cov_std","vtype")], 
                     by=c("location_id","year_id","vtype"))

  # If the study reported vaccine serotype coverage, use that
    pcv_data$study_coverage <- pcv_data$vaccineserotype/100
    pcv_data$study_coverage_std <- (pcv_data$upper_serotype - pcv_data$lower_serotype) / 2 / qnorm(0.975) / 100

    pcv_data$study_coverage <- ifelse(is.na(pcv_data$study_coverage), pcv_data$pcv_vt_cov, pcv_data$study_coverage)
    pcv_data$study_coverage_std <- ifelse(is.na(pcv_data$study_coverage_std), pcv_data$pcv_vt_cov_std, pcv_data$study_coverage_std)

  # For studies that only reported All Invasive S pneumo, the vaccine serotype coverage should be 1.
    pcv_data$study_coverage[is.na(pcv_data$ve_vt_invasive) & !is.na(pcv_data$ve_all_invasive)] <- 1
    pcv_data$study_coverage_std[is.na(pcv_data$ve_vt_invasive) & !is.na(pcv_data$ve_all_invasive)] <- 0

## Starting in GBD 2016, we used an adjustment for the efficacy in invasive disease 
## (majority of input data) compared to against pneumococcal pneumonia. This is informed
## by a single study (Bonten et al 2013) and after some experimentation, decided on using 
## a uniform distribution for this adjustment.

  uni_adj <- data.frame(age_group_id=22)
  for(i in 1:1000){
    uni_adj[,paste0("adjustment_",i)] <- runif(n=1, min=0.3, max=1)
  }

## Import Hib final PAF summaries ##
  hib_paf <- read.csv("filepath")

  pcv_data <- join(pcv_data, hib_paf, by=c("location_id","year_id"))

## Need to create some logit variables for vaccine coverage and Hib PAF ##
## Let's use the GBD estimates for vaccine coverage for this.

  pcv_data$logit_hib <- logit(pcv_data$hib_paf)
  pcv_data$logit_hib_se <- sapply(1:nrow(pcv_data), function(i) {
    ratio_i <- pcv_data[i, "hib_paf"]
    ratio_se_i <- pcv_data[i, "hib_paf_std"]
    deltamethod(~log(x1 / (1 - x1)), ratio_i, ratio_se_i^2)
  })

  # Set logit study vaccine coverage
  pcv_data$logit_cov <- logit(pcv_data$study_coverage)
  pcv_data$logit_cov_se <- sapply(1:nrow(pcv_data), function(i) {
    ratio_i <- pcv_data[i, "study_coverage"]
    ratio_se_i <- pcv_data[i, "study_coverage_std"]
    deltamethod(~log(x1 / (1 - x1)), ratio_i, ratio_se_i^2)
  })

  # See the ballpark number
  pcv_data$paf_ballpark <- with(pcv_data, ve_pneumonia * (1-hib_paf) / ve_invasive / study_coverage / 0.65)

  write.csv(pcv_data, "filepath", row.names=F)

## Now all we need to do is, by draw, calculate the study-level PAF
## for S pneumoniae in the absence of the vaccine, accounting for imperfect
## efficacy against pneumonia, serotype coverage, and Hib PAF! 

  pcv_df <- data.frame(nid=pcv_data$nid)
  pcv_df_log <- data.frame(nid=pcv_data$nid)
  for(i in 1:1000){
    ve_p <- 1-exp(rnorm(n=length(pcv_data$nid), mean=pcv_data$ln_ve, sd=pcv_data$ln_ve_se))
    vi <- 1-exp(rnorm(n=length(pcv_data$nid), mean=pcv_data$ln_vi, sd=pcv_data$ln_vi_se))
    cov <- rnorm(n=length(pcv_data$nid), mean=pcv_data$study_coverage, sd=pcv_data$study_coverage_std)
    cov <- inv.logit(rnorm(n=length(pcv_data$nid), mean=pcv_data$logit_cov, sd=pcv_data$logit_cov_se))
    cov[is.na(cov)] <- 1

    hib <- inv.logit(rnorm(n=length(pcv_data$nid), mean=pcv_data$logit_hib, sd=pcv_data$logit_hib_se))
    # account for the fact that there is no Hib attributed in ages older than 5
    hib <- ifelse(pcv_data$age_start >= 5, 0, hib)

    # This study actually reports the VE against S pneumoniae pneumonia
    adj <- uni_adj[,paste0("adjustment_",i)]
    adj <- ifelse(pcv_data$nid==139192, 1, adj)

    paf <- ve_p * (1-hib) / vi / cov / adj
    paf <- ifelse(paf > 1, 0.995, paf)

    pcv_df[,paste0("draw_",i)] <- paf
    pcv_df_log[,paste0("draw_",i)] <- log(1-paf)
  }

  pcv_data$mean_paf <- rowMeans(pcv_df[,2:1001])
  pcv_data$std_paf <- apply(pcv_df[,2:1001], 1, sd)
  pcv_data$paf_lower <- apply(pcv_df[,2:1001], 1, function(x) quantile(x, 0.025, na.rm=T))
  pcv_data$paf_upper <- apply(pcv_df[,2:1001], 1, function(x) quantile(x, 0.975, na.rm=T))

  pcv_data$mean_log_paf <- rowMeans(pcv_df_log[,2:1001])
  pcv_data$std_log_paf <- apply(pcv_df_log[,2:1001], 1, sd)
  rma_paf <- rma(mean_log_paf, sei=std_log_paf, data=pcv_data, method="DL", slab=first)
  forest(rma_paf)

## Create some variables required for BradMod ##
  pcv_data$meas_value <- rowMeans(exp(pcv_df_log[,2:1001]))
  pcv_data$meas_stdev <- apply(exp(pcv_df_log[,2:1001]), 1, sd)
  pcv_data$time_lower <- pcv_data$year_start
  pcv_data$time_upper <- pcv_data$year_end
  pcv_data$age_lower <- pcv_data$age_start
  pcv_data$age_upper <- pcv_data$age_end
  pcv_data$super <- pcv_data$super_region_name
  pcv_data$subreg <- pcv_data$ihme_loc_id
  pcv_data$integrand <- "incidence"
  pcv_data$x_sex <- 0
  pcv_data$x_ones <- 0
  pcv_data$x_befaft <- ifelse(pcv_data$study_type=="RCT",0,1)

## Sanity check
  pcv_data$paf_rough <- with(pcv_data, ve_pneumonia * (1 - hib_paf) / ve_invasive / study_coverage / 0.7)

###########################################################################################
## This file is the input nito "winrun_lri_adjusted.do" to get an age
## curve from BradMod. ##
## Save ##
  pcv_data <- subset(pcv_data, !is.na(meas_value))
  pcv_data$meas_stdev <- ifelse(pcv_data$meas_stdev==0, 0.001, pcv_data$meas_stdev)
  if(fill_vt==T){
    pcv_data$x_filled <- ifelse(is.na(pcv_data$ve_invasive_lower),1,0)
    write.csv(pcv_data, "filepath", row.names=F)
  } else {
    write.csv(pcv_data, "filepath", row.names=F)
  }

############################################################################################
  ## Prep a file that can be run in MR-BRT ##
############################################################################################

pcv_data$age <- floor((pcv_data$age_end + pcv_data$age_start) / 2)
pcv_data$before_after <- pcv_data$x_befaft

mrbrt <- pcv_data[,c("nid","age","meas_value","meas_stdev","mean_paf","std_paf","mean_log_paf",
                     "std_log_paf","before_after","age_start","age_end","ihme_loc_id","study_type","vtype",
                     "ve_pneumonia","ve_invasive","year_start","pcv_cov","pcv_vt_cov","hib_paf")]
  write.csv(mrbrt, "filepath", row.names=F)

## First, we must crosswalk the study type ##

# Try to increase matches, get one in adults (change AUS and NLD to W. Europe)
mrbrt$match_loc <- mrbrt$ihme_loc_id
mrbrt$match_loc <- ifelse(mrbrt$ihme_loc_id == "NLD", "WEU", 
                          ifelse(mrbrt$match_loc == "AUS", "WEU", 
                                 ifelse(mrbrt$match_loc == "JPN", "WEU", as.character(mrbrt$match_loc))))

mrbrt$age_cut <- cut(mrbrt$age, c(-1,5,20,60,99))
df_ref <- subset(mrbrt, before_after == 0)
df_alt <- subset(mrbrt, before_after == 1)
  setnames(df_alt, c("meas_value", "meas_stdev"), c("alt_meas_value","alt_meas_stdev"))

df_xw <- join(df_ref, df_alt[,c("match_loc","age_cut","alt_meas_value","alt_meas_stdev")], by=c("match_loc","age_cut"))
  df_xw <- subset(df_xw, !is.na(alt_meas_value))
df_xw$ratio <- df_xw$meas_value / df_xw$alt_meas_value
df_xw$log_ratio <- log(df_xw$ratio)
df_xw$se <- sqrt(df_xw$meas_value^2 / df_xw$alt_meas_value^2 * (df_xw$meas_stdev^2/df_xw$alt_meas_value^2 + df_xw$alt_meas_stdev^2/df_xw$meas_value^2))
df_xw$log_ratio_se <- sapply(1:nrow(df_xw), function(i) {
  ratio_i <- df_xw[i, "ratio"]
  ratio_se_i <- df_xw[i, "se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})



## Run this in MR-BRT ##
fit1 <- run_mr_brt(
  output_dir = "filepath",
  model_label = "pneumo_study_type",
  data = df_xw[df_xw$ratio < 2,],
  mean_var = "log_ratio",
  se_var = "log_ratio_se",
  trim_pct = 0.1,
  method = "trim_maxL",
  study_id = "nid",
  overwrite_previous = TRUE
)

df_pred <- data.frame(intercept = 1)
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
preds$se <- (preds$Y_mean_hi - preds$Y_mean) / qnorm(0.975)

mrbrt$original_meas <- mrbrt$meas_value

# Convert the standard deviation to log space
mrbrt$log_meas_se <- sapply(1:nrow(mrbrt), function(i) {
    ratio_i <- mrbrt[i, "meas_value"]
    ratio_se_i <- mrbrt[i, "meas_stdev"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

mrbrt$xw_meas_value <- ifelse(mrbrt$before_after == 1, mrbrt$meas_value * exp(preds$Y_mean), mrbrt$meas_value)
mrbrt$log_meas_se <- ifelse(mrbrt$before_after == 1, sqrt(mrbrt$log_meas_se^2 + preds$se^2), mrbrt$log_meas_se)

mod_data <- fit1$train_data
mod_data$outlier <- ceiling(abs(mod_data$w - 1))
mod_data$row_num <- 1:length(mod_data$w)
mod_data$label <- paste0(mod_data$ihme_loc_id, "_", mod_data$age_cut,"_", mod_data$nid,"_",mod_data$row_num)
f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=log_ratio + log_ratio_se*1.96, ymin=log_ratio - log_ratio_se*1.96)) + 
  geom_point(aes(y=log_ratio, x=label)) + geom_errorbar(aes(x=label), width=0) +
  theme_bw() + xlab("") + coord_flip() + ggtitle(paste0("Before-after comparison (",round(preds$Y_mean,3),")")) +
  geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") + scale_y_continuous(limits = c(-20, 20)) +
  geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
print(f)

##########################################################
## Run the age curve in MR-BRT ##

mrbrt$log_value <- log(mrbrt$xw_meas_value)
mrbrt$age <- (mrbrt$age_end + mrbrt$age_start) / 2
mrbrt$age <- mrbrt$age_end

fit1 <- run_mr_brt(
  output_dir = "filepath",
  model_label = "pneumo_age_paf",
  data = mrbrt[mrbrt$mean_paf > 0.2,],
  mean_var = "log_value",
  se_var = "log_meas_se",
  # covs = list(cov_info("age","X", degree = 3, n_i_knots=4, l_linear=TRUE, r_linear=TRUE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf"),
  #             cov_info("before_after","X")),
  covs = list(cov_info("age","X", degree = 3, i_knots="2,5,40,60", l_linear=TRUE, r_linear=TRUE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf")),
  trim_pct = 0.1,
  method = "trim_maxL",
  #study_id = "nid",
  overwrite_previous = TRUE
)

df_pred <- data.frame(intercept = 1, age = seq(0,100,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

preds$age <- preds$X_age
preds$before_after <- preds$X_before_after

# Calculate se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

mod_data <- fit1$train_data

mod_data <- fit1$train_data
mod_data$outlier <- ceiling(abs(mod_data$w - 1))
preds$pred_mean <- 1 - exp(preds$Y_mean)
preds$linear_lower <- 1 - exp(preds$Y_mean_lo)
preds$linear_upper <- 1 - exp(preds$Y_mean_hi)

preds$before_after <- 0

p <- ggplot(preds) + geom_line(aes(x=X_age, y=pred_mean, col=factor(before_after))) +
  geom_ribbon(aes(x=X_age, ymin=linear_lower, ymax= linear_upper, fill=factor(before_after)), alpha=0.25) + 
  ggtitle("Strep pneumo attributable fraction") + theme_bw() +
  xlab("Age mid") + ylab("Mean attributable fraction") + geom_hline(yintercept=0) + guides(fill=F) +
  geom_point(data=mod_data, aes(x=age, y=1-xw_meas_value, col=factor(before_after), shape=factor(outlier)), size=2, alpha=0.75) +
  scale_color_manual("Study type", values = c("#30A9DE","#E53A40"), label=c("RCT","Before-after")) + 
  scale_fill_manual("Study type", values = c("#30A9DE","#E53A40"), label=c("RCT","Before-after")) +
  scale_shape_manual("Outlier", values = c(19, 4)) + scale_y_continuous(limits = c(-1, 1))
print(p)

## Compare with BradMod ##
bmod <- read.csv("filepath")
bmod$age_start <- bmod$age_lower
bmod <- join(bmod[,c("lower","upper","mean","age_start")], age_info[age_info$age_pull==1,c("age_group_id","order","age_start","age_end")], by="age_start")
bmod$age <- (bmod$age_start + bmod$age_end) / 2

ggplot() + geom_line(data=subset(preds, before_after==0), aes(x=X_age, y=pred_mean), col="blue") + 
  geom_ribbon(data=subset(preds, before_after==0), aes(x=X_age, ymin=linear_lower, ymax= linear_upper), alpha=0.25, fill="blue") +
  geom_line(data=bmod, aes(x=age, y=mean), col="purple") + geom_ribbon(data=bmod, aes(x=age, ymin=lower, ymax= upper), alpha=0.25, fill="purple") + theme_bw()


######################################################
## Save the predicted PAFs by age ##
######################################################

write.csv(preds, "filepath", row.names=F)

#############################################################################################
  ## Make a few data plots ##
pcv_data$age_mid <- (pcv_data$age_end + pcv_data$age_start) / 2
ggplot(pcv_data, aes(x=age_mid, y=mean_paf, ymin=paf_lower, ymax=paf_upper)) + geom_point() + geom_errorbar() + stat_smooth(method="loess") +
  scale_y_continuous(limits=c(-5,1.25))
ggplot(pcv_data, aes(x=age_mid, y=mean_paf, ymin=paf_lower, ymax=paf_upper)) + geom_point() + geom_errorbar() + stat_smooth(method="loess") +
  scale_y_continuous(limits=c(-5,1.25)) + scale_x_continuous(limits=c(0,10))

## Compare input data with those used in GBD 2017 ##
used_data <- read.csv("filepath")
used_data$used_mean <- used_data$meas_value

pcv_data$author <- paste0(pcv_data$first,"_",pcv_data$iso3,pcv_data$time_upper)
comp <- join(pcv_data, used_data[,c("author","used_mean","age_lower")], by=c("age_lower","author"))
ggplot(comp, aes(x=1-used_mean, y=1-meas_value)) + geom_point() + theme_bw() + geom_abline(intercept=0, slope=1)
