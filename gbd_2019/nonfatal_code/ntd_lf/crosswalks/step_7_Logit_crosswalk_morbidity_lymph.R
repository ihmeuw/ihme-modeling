#LF MR-BRT test
# 
#
# Example of how to do a crosswalk when the outcome variable is 
# bounded by zero (log-linear crosswalk), and the adjustment factor should differ by levels 
# of a covariate
# 
# The data come from estimates of prevalence/incidence of knee osteoarthritis
#   when using two different sets of diagnostic criteria
#
# When the ratio is reference/alternative it is interpreted as: 
#   "when you have a data point using the alternative definition, 
#    here's what to multiply it by to make it equivalent to the reference definition"
# 
# Note that we model the ratio as alternative/reference
# -- In the code, it's expressed as a difference on the log scale because:
#    log(a/b) = log(a) - log(b)
#
# We also adjust the standard error to account for the fact the adjusted data aren't real
# -- We account for the usual uncertainty that results from taking the sum of 
#    two random variables, as well as the uncertainty conferred between-study heterogeneity
#    (i.e. incorporating uncertainty from tau in the mixed effect meta-regression)
# 
user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH", user, "FILEPATH")
library(dplyr)
library(data.table)
library(metafor, lib.loc = path)
library(msm, lib.loc = path)
library(ggplot2)
install.packages("tidyverse", lib=path)
library(tidyverse)
library(gtools)
rm(list = ls())


#####
# USER SETTINGS

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

#dat_original will be LF age and sex split model inputs 

dat_original <-read.csv("FILEPATH") 

#need to recode variables for dx and hydrocele

dat_original$lymph_mean<-dat_original$mean
dat_original$lymph_ss<-dat_original$sample_size
dat_original$lymph_cases<-dat_original$cases

#imput cases or sample size if missing

dat_original$mean<-NA
dat_original$sample_size<-NA
dat_original$cases<-NA

dt<-as.data.table(dat_original)


dt<-dt[, prev_mf := ifelse(is.na(prev_mf), np_mf/pop_mf, prev_mf)]
dt<-dt[, prev_ict := ifelse(is.na(prev_ict),np_ict/pop_ict, prev_ict)]


dt<-dt[, pop_mf := ifelse(is.na(pop_mf), prev_mf/np_mf, pop_mf)]
dt<-dt[, pop_ict := ifelse(is.na(pop_ict),prev_ict/np_ict,pop_ict)]

dt<-dt[, mean := ifelse(!is.na(prev_mf), prev_mf, prev_ict)]

dt<-dt[, sample := ifelse(!is.na(pop_mf), pop_mf,pop_ict)]
dt<-dt[, cases := ifelse(!is.na(np_mf), np_mf,np_ict)]

dt<-dt[, diagnostic := ifelse((!is.na(prev_mf)& is.na(prev_ict)), "mf","ICT")]

dat_original<-dt 
reference_var <- "ref"
reference_value <- 1
mean_var <- "mean"
se_var <- "se"
#need to calculate an SE for the observations

dat_original$se<-sqrt((dat_original$mean-(1-dat_original$mean)/dat_original$sample_size))
#create ref variable
dat_original$ref<-1

# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as alternative/reference
dat_metareg <- read.csv("FILEPATH") 

#####
dat_metareg<-dat_metareg %>% 
  mutate(ratio=mean_2/mean_1)

#drop records that have zero ratio
dat_metareg<-dat_metareg[dat_metareg$ratio!=0,]
#outlier records for which the ratio is not <1
dat_metareg<-dat_metareg[dat_metareg$ratio<1,]
#drop records where both tests are zero
dat_metareg<-dat_metareg[!(dat_metareg$cases_1==0 & dat_metareg$cases_2 ==0), ]
#drop where records are NA
dat_metareg<-na.omit(dat_metareg)


#logit transformation and SE for comparisons 

dat_metareg$se_logit_mean_1 <- sapply(1:nrow(dat_metareg), function(i) {
  ratio_i <- dat_metareg[i, "mean_1"]
  ratio_se_i <- dat_metareg[i, "se_1"]
  deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
})

dat_metareg$se_logit_mean_2 <- sapply(1:nrow(dat_metareg), function(a) {
  ratio_a <- dat_metareg[a, "mean_2"]
  ratio_se_a <- dat_metareg[a, "se_2"]
  deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
})

#calculate logit difference :MEAN1 - REFERENCE, MEAN2= ALTERNATIVE
#### 
dat_metareg <- dat_metareg %>%
  mutate(
    logit_mean_1 = logit(mean_1),
    logit_mean_2 = logit(mean_2),
    diff_logit = logit_mean_2 - logit_mean_1,
    se_diff_logit = sqrt(se_logit_mean_1^2 + se_logit_mean_2^2)
  )



ratio_var <- "diff_logit"
ratio_se_var <- "se_diff_logit"


# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var)
metareg_vars <- c(ratio_var, ratio_se_var)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref")) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("diff_logit", "se_diff_logit"))


# log transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})


tmp_metareg$intercept <- 1


# fit the MR-BRT model
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))
source(paste0(repo_dir, "FILEPATH"))

#covs1 <- list()
#for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

fit1 <- run_mr_brt(
  output_dir = paste0("FILEPATH", Sys.info()["user"], "/"),
  model_label = "ADDRESS",
  data = tmp_metareg,
  mean_var = "diff_logit",
  se_var = "se_diff_logit",

  overwrite_previous = TRUE
)



check_for_outputs(fit1)

# this creates a ratio prediction for each observation in the original data
df_pred <- as.data.frame(tmp_orig) 
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

tmp_preds <- preds %>%
  mutate(
    pred = Y_mean,
    # don't need to incorporate tau as with metafor; MR-BRT already included it in the uncertainty
    pred_se = (Y_mean_hi - Y_mean_lo) / 3.92  ) %>%
  select(pred, pred_se)



# new variance of the adjusted data point is just the sum of variances 
# because the adjustment is a sum rather than a product in log space
tmp_orig2 <- cbind(tmp_orig, tmp_preds) %>%
  mutate(
    mean_logit_tmp = mean_logit - pred, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_logit_tmp = se_logit^2 + pred_se^2, # adjust the variance
    se_logit_tmp = sqrt(var_logit_tmp)
  )

# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
  mutate(
    mean_logit_adjusted = if_else(ref == 1, mean_logit, mean_logit_tmp),
    se_logit_adjusted = if_else(ref == 1, se_logit, se_logit_tmp),
    lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
    hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
    mean_adjusted = inv.logit(mean_logit_adjusted),
    lo_adjusted = inv.logit(lo_logit_adjusted),
    hi_adjusted = inv.logit(hi_logit_adjusted) )


tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "mean_logit_adjusted"]
  ratio_se_i <- tmp_orig3[i, "se_logit_adjusted"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})



# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original, 
  tmp_orig3[, c("mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)




final_data$ref<-as.factor(final_data$ref)
#standard plots 
plot_mr_brt(fit1)

#plot observed v. crosswalked data
# Same, but with different colors and add regression lines
ggplot(final_data, aes(x=mean, y=mean_adjusted, color=ref)) +
  geom_point(shape=1) +
  scale_colour_hue(l=50) 



pred_df<-as.data.frame(pred1$model_summaries)
write.csv(final_data,"FILEPATH")


