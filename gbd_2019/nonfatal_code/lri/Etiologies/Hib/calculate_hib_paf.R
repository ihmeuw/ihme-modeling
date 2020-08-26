## Calculate attributable fraction of Hib ##

########################################################################################################
## Prep ##
########################################################################################################

library(metafor)
library(matrixStats)
library(plyr)
library(ggplot2)
library(reshape2)
source("/filepath/get_covariate_estimates.R")
source("/filepath/get_covariate_estimates.R")
source("/filepath/mr_brt_functions.R")
source("/filepath/plot_mr_brt_function.R")
source("/filepath/run_mr_brt_function.R")

hib_vac_data <- read.csv("filepath")

  hib_vac_data$ve_invasive <- hib_vac_data$ve_invasive
  hib_vac_data$ve_invasive_lower <- hib_vac_data$lower_invasive
  hib_vac_data$ve_invasive_upper <- hib_vac_data$upper_invasive
  hib_vac_data$ve_invasive_se <- with(hib_vac_data, (ve_invasive_upper - ve_invasive_lower)/2/qnorm(0.975))

  hib_vac_data$ve_pneumonia <- hib_vac_data$Vaccine.Efficacy
  hib_vac_data$ve_pneumonia_lower <- hib_vac_data$Lower.95.
  hib_vac_data$ve_pneumonia_upper <- hib_vac_data$Upper.95.
  hib_vac_data$ve_pneumonia_se <- with(hib_vac_data, (ve_pneumonia_upper - ve_pneumonia_lower)/2/qnorm(0.975))

# make in log space
  hib_vac_data$ln_vi <- log(1-hib_vac_data$ve_invasive/100)
  hib_vac_data$ln_vi_upper <- log(1-hib_vac_data$ve_invasive_upper/100)
  hib_vac_data$ln_vi_se <- (hib_vac_data$ln_vi - hib_vac_data$ln_vi_upper)/qnorm(0.975)

  hib_vac_data$ln_ve <- log(1-hib_vac_data$ve_pneumonia/100)
  hib_vac_data$ln_ve_upper <- log(1-hib_vac_data$ve_pneumonia_upper/100)
  hib_vac_data$ln_ve_se <- (hib_vac_data$ln_ve - hib_vac_data$ln_ve_upper)/qnorm(0.975)

#########################################################################################################
## Part 1: Meta-analysis of Hib RCT studies to get a PAF in the absence of vaccine ##
  #################################################################################

## keep if studies are RCT
  rct <- subset(hib_vac_data, Study.type=="RCT" & is_outlier==0)

## The idea is to run a meta-analysis for studies that report
## the reduction in invasive Hib disease and for
## reduction in all-cause pneumonia. The ratio of these two
## values is the attributable fraction for Hib in the
## absence of the vaccine.

  paf_table <- rct[,c("first","iso3","ve_invasive","ve_invasive_se","ve_pneumonia","ve_pneumonia_se","ln_vi","ln_vi_se","ln_ve","ln_ve_se")]
  pneumo_draws <- c()
  invasive_draws <- c()
  for(i in 1:1000){
    draw_pneumo <- rnorm(n=length(rct$source), mean=rct$ve_pneumonia, sd=rct$ve_pneumonia_se)
    draw_hib <- rnorm(n=length(rct$source), mean=rct$ve_invasive, sd=rct$ve_invasive_se)
    draw <- draw_pneumo / draw_hib
    draw <- ifelse(draw > 1, 0.995, draw)
    paf_table[,paste0("draw_",i)] <- draw
  }
## Do this in log-space since the VE estimates are log-normal

  paf_table$mean <- rowMeans(paf_table[,11:1010])
  paf_table$std <- apply(paf_table[,11:1010], 1, sd)
  stds <- rowSds(as.matrix(paf_table[,11:1010]))

  paf_table[,c("first","iso3","ve_invasive","ve_invasive_se","ve_pneumonia","ve_pneumonia_se","mean","std")]

## Check the uncertainty using variance of ratios ##
  paf_table$se_ratio <- with(paf_table,
                             sqrt(ve_pneumonia^2 / ve_invasive^2 *
                                    (ve_pneumonia_se^2 / ve_pneumonia^2 + ve_invasive_se^2/ve_invasive^2)))
  meta <- rma(mean, sei=se_ratio, data=paf_table, method="DL")
  summary(meta)
  forest(meta, slab=paf_table$iso3)

## Run meta-analysis ##
  meta <- rma(mean, sei=std, data=paf_table, method="DL")
  summary(meta)
  forest(meta, slab=paf_table$iso3)

## Test this in MR-BRT ##
  fit1 <- run_mr_brt(
    output_dir = "filepath",
    model_label = "hib_paf",
    data = paf_table[!is.na(paf_table$mean),c("mean","std","iso3")],
    mean_var = "mean",
    se_var = "std",
    overwrite_previous = TRUE
  )

  df_pred <- data.frame(intercept = 1)
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries

  # Calculate se
  preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)

  mod_data <- fit1$train_data
  ## Create essentially a forest plot
  f <- ggplot(mod_data, aes(ymax=mean + std*1.96, ymin=mean - std*1.96)) + geom_point(aes(y=mean, x=iso3)) + 
    geom_errorbar(aes(x=iso3), width=0) +
    theme_bw() + ylab("PAF") + xlab("") + coord_flip() + 
    ggtitle(paste0("Modeled Attributable Fraction: ", round(preds$Y_mean,2), " (", round(preds$Y_mean_lo,2),"-",round(preds$Y_mean_hi,2),")")) +
    geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
    geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$iso3)+1), alpha=0.1, fill="purple")
  print(f)

## Calculate Hib paf draws ##
  hib_paf <- data.frame(modelable_entity="lri_hib", rei_id=189)
  for(i in 1:1000){
    hib_paf[,paste0("draw_",i)] <- rnorm(n=1, mean=preds$Y_mean, sd=preds$se)
  }

## We have Hib PAF in the absence of vaccine ##
  write.csv(hib_paf, "filepath", row.names=F)

## Check for values outside of range (negative or positive)
  draws <- hib_paf[,3:1002]
  range(draws)
  hist(as.numeric(draws))
  quantile(draws, 0.025)

################################################################################################################
  ## Part 2: Use Hib vaccine coverage to determine the Hib PAF in the presence of vaccine ##
################################################################################################################

## Previous GBD rounds used a meta-analysis (Swingler et al https://www.ncbi.nlm.nih.gov/pubmed/17443509)
  ## to get an estimate of the vaccine efficacy against invasive Hib disease.
  ## Three problems with this. The first is that they didn't use one of our RCTs (Gessner 2005).
  ## The second is that previous GBDs didn't incorporate uncertainty
  ## The third is that we have a better estimate of this value.
  ## Need to track the differences between previous approach (0.8, no uncertainty),
  ## incorporation of Swingler uncertainty (0.46-0.97),
  ## and estimation using our data.

### use the value produced by the meta-analysis of Hib data ###
  efficacy_default <- 0.91

  sys_efficacy <- data.frame(mean=0.8, lower=0.46, upper=0.97, ln_mean=log(1-0.8), ln_upper=log(1-0.46))
  sys_efficacy$ln_se <- (sys_efficacy$ln_upper - sys_efficacy$ln_mean)/qnorm(0.975)

  meta_efficacy <- data.frame(mean = 0.91, lower=0.77, upper=0.96, ln_mean=-2.3835, ln_se = 0.4611)

  meta.invasive <- rma(ln_vi, sei=ln_vi_se, data=rct, method="DL")
  summary(meta.invasive)
  ## Check that value makes sense
    1-exp(meta.invasive$b)
    rct[,c("ve_invasive")]

## Calculate Hib VE invasive draws ##
  hib_invasive <- data.frame(type=c("single_value","sys_review","meta_analysis"))
  for(i in 1:1000){
    hib_invasive[2,paste0("draw_",i)] <- 1-exp(rnorm(n=1, mean=sys_efficacy$ln_mean, sd=sys_efficacy$ln_se))
    hib_invasive[3,paste0("draw_",i)] <- 1-exp(rnorm(n=1, mean=meta.invasive$b, sd=meta.invasive$se))
    hib_invasive[1,paste0("draw_",i)] <- efficacy_default
  }

# Make a histogram to compare
  hib_melt <- melt(hib_invasive[2:3,])
  ggplot(hib_melt, aes(x=value, fill=type)) + geom_histogram(col="black") + theme_bw() + geom_vline(xintercept=0.8, lwd=2, col="darkblue") +
    geom_vline(xintercept=1-exp(meta.invasive$b), col="darkred", lwd=2)

## Pull in the Hib vaccine coverage for every GBD location and year ##
  hib_cov <- get_covariate_estimates(covariate_id=47, location_id="all", year_id=1990:2019, decomp_step="step4")

## Create draws for vaccine coverage ##
  library(boot)
# logit transform the vaccine coverage
  hib_cov$logit_mean <- logit(hib_cov$mean_value)
  hib_cov$logit_upper <- logit(hib_cov$upper_value)
  hib_cov$logit_se <- (hib_cov$logit_upper - hib_cov$logit_mean) / qnorm(0.975)

  hib_c <- hib_cov
  for(i in 1:1000){
    draw <- inv.logit(rnorm(n=length(hib_cov$mean_value), mean=hib_cov$logit_mean, sd=hib_cov$logit_se))
    draw <- ifelse(draw=="NaN",0,draw)
    hib_c[,paste0("coverage_",i)] <- draw
  }

## Save this for later ##
  write.csv(hib_c, "filepath", row.names=F)

## To be able to test this ##
  is_test <- FALSE
  if(is_test==TRUE){
    est_df <- data.frame(subset(hib_c, year_id==2017))
  } else{
    est_df <- data.frame(hib_c)
  }

##  everything is prepped ##
## Now, calculate the Hib PAF in the presence of vaccine ##
  est_df$mean_coverage <- est_df$mean_value
  hib_df <- est_df[,c("location_id","location_name","year_id","sex_id","age_group_id","mean_coverage")]

## The loop creates the values from GBD 2017 but also calculates alternatives, summarizes, and saves for comparison ##
# Equation is PAFBase * (1-Cov*VE) / (1 - PAFBase * Cov * VE)

  paf_sys <- data.frame(location_name=hib_df$location_name)
  paf_meta <- data.frame(location_name=hib_df$location_name)

  for(i in 1:1000){
    p <- hib_paf[,paste0("draw_",i)]
    cov <- est_df[,paste0("coverage_",i)]
    ve <- hib_invasive[,paste0("draw_",i)]

    gbd <-  p * (1 - cov * ve[1]) / (1 - p * cov * ve[1])
    hib_df[,paste0("draw_",i)] <- gbd

    if(is_test==TRUE){
      sys <- p * (1 - cov * ve[2]) / (1 - p * cov * ve[2])
      meta <- p * (1 - cov * ve[3]) / (1 - p * cov * ve[3])
      paf_sys[,paste0("draw_",i)] <- sys
      paf_meta[,paste0("draw_",i)] <- meta
    }
  }

  hib_df$mean_paf <- rowMeans(hib_df[,7:1006])
  hib_df$std_paf <- apply(hib_df[,7:1006], 1, sd)
  hib_df$paf_lower <- apply(hib_df[,7:1006], 1, function(x) quantile(x, 0.025))
  hib_df$paf_upper <- apply(hib_df[,7:1006], 1, function(x) quantile(x, 0.975))

  if(is_test==TRUE){
    hib_df$mean_sys <- rowMeans(paf_sys[,2:1001])
    hib_df$sys_lower <- apply(paf_sys[,2:1001], 1, function(x) quantile(x, 0.025))
    hib_df$sys_upper <- apply(paf_sys[,2:1001], 1, function(x) quantile(x, 0.975))
    hib_df$mean_meta <- rowMeans(paf_meta[,2:1001])
    hib_df$meta_lower <- apply(paf_meta[,2:1001], 1, function(x) quantile(x, 0.025))
    hib_df$meta_upper <- apply(paf_meta[,2:1001], 1, function(x) quantile(x, 0.975))
  }

## Tada!! ##
  write.csv(hib_df, "filepath", row.names=F)

## Summarize Hib PAFs for use in pcv analysis ##
  hib_trunc <- hib_df[,c("location_id","year_id","mean_paf","std_paf","paf_lower","paf_upper")]
  colnames(hib_trunc) <- c("location_id","year_id","hib_paf","hib_paf_std","hib_paf_lower","hib_paf_upper")
  write.csv(hib_trunc, "filepath", row.names=F)

################################################################################################################
## Great, now produce some diagnostic plots! ##
ggplot(data=hib_df, aes(x=mean_coverage, y=mean_paf, ymin=paf_lower, ymax=paf_upper)) + geom_errorbar(width=0) + geom_point() +
  theme_bw() + ggtitle("Approach used in GBD 2017") + stat_smooth(method="loess", col="purple")
ggplot(data=hib_df, aes(x=mean_coverage, y=mean_sys, ymin=sys_lower, ymax=sys_upper)) + geom_errorbar(width=0) + geom_point() +
  theme_bw() + ggtitle("Approach using Systematic Review Uncertainty") + stat_smooth(method="loess", col="darkblue")
ggplot(data=hib_df, aes(x=mean_coverage, y=mean_meta, ymin=meta_lower, ymax=meta_upper)) + geom_errorbar(width=0) + geom_point() +
  theme_bw() + ggtitle("Approach using Meta-analysis Uncertainty") + stat_smooth(method="loess", col="darkred")
ggplot(hib_df, aes(x=mean_coverage)) + geom_errorbar(aes(ymin=paf_lower, ymax=paf_upper), col="purple", alpha=0.25) +
  geom_errorbar(aes(ymin=sys_lower, ymax=sys_upper), col="darkblue", alpha=0.25) +
  geom_errorbar(aes(ymin=meta_lower, ymax=meta_upper), col="darkred", alpha=0.25) + theme_bw()

diag_df <- melt(hib_df[,c("mean_coverage","mean_paf","mean_sys",
                          "mean_meta")], id.vars="mean_coverage")
ggplot(diag_df, aes(x=mean_coverage, y=value, col=variable)) + geom_line() + theme_bw()

diag_df <- melt(data.frame(mean_coverage=hib_df$mean_coverage, range_paf = hib_df$paf_upper - hib_df$paf_lower,
                           range_sys = hib_df$sys_upper - hib_df$sys_lower,
                           range_meta = hib_df$meta_upper - hib_df$meta_lower), id.vars="mean_coverage")
ggplot(diag_df, aes(x=mean_coverage, y=value, col=variable)) + geom_line() + theme_bw()
