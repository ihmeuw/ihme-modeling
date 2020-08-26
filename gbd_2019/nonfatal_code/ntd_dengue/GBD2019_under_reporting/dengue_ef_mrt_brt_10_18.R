
#USE MR-BRT to cross-walk under-reporting
# 
# #
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
path <- paste0("FILEPATH", user, "/rlibs/")
library(dplyr)
library(data.table)
#install.packages("metafor", lib=path)
#install.packages("msm", lib=path)
library(metafor, lib.loc = path)
library(msm, lib.loc = path)
#install.packages("ggplot2",lib=path)
library(ggplot2)
install.packages("tidyverse", lib=path)
library(tidyverse)
rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
#####
# USER SETTINGS

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

#read in age-split incidence data
dat_original<-read.csv("FILEPATH")
reference_var <- "ref"
reference_value <- 1
mean_var <- "mean"
se_var <- "se"
cov_names <- c("m2_cat1","m2_cat2","m2_cat3","m2_cat4","hq_cat") # can be a vector of names
#cov_names<-c("mean_2")


#with ST/GPR input dataset
dat_original$se<-sqrt(dat_original$variance)

#identify ref variable 
#in MRT BRT dataset mean_1 is the active screening, Mean_2 is the passive detection
#all data inputs are passive detection
dat_original$ref<-0

#ST/GPR inputs coded as data = mean 
dat_original$mean<-dat_original$data

dat_original<-as.data.table(dat_original)

#create categorical covariates based on median values
dat_original<-dat_original%>%
  mutate(sdi_cat=if_else(sdi_mean>.5224,1,0))%>%
  mutate(hq_cat=if_else(haqi_mean>53,1,0))%>%
  mutate(deng_prob=ifelse(dengue_prob_mean>0.85,1,0))
  
dat_original$mean_2<-dat_original$mean

dat_original<-dat_original%>%
  mutate(m2_cat1=if_else((0.001<=mean_2 & mean_2<.002),1,0))%>%
  mutate(m2_cat2=if_else((0.002<=mean_2 & mean_2<.003),1,0))%>%
  mutate(m2_cat3=if_else((0.003<=mean_2 & mean_2<.004),1,0))%>%
  mutate(m2_cat4=if_else((0.004<=mean_2),1,0))


#do not correct for under reporting for Cape Verde - 2009 data already very high
#dat_original<-dat_original%>%
 # mutate(ref=if_else(location_id==203,1,0))


#do not inflate original incidence greater than .1, generates implausibly high values >1
dat_original<-dat_original%>%
 mutate(ref=if_else(mean>.09,1,0))

table(dat_original$ref)

# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as alternative/reference
dat_metareg <- read.csv("FILEPATH") 
ratio_var <- "ratio"
ratio_se_var <- "ratio_se"

#drop group_review =0

dat_metareg_b<-dat_metareg[dat_metareg$group_review==1,]

#drop missing data on se, etc 
dat_metareg_b<-dat_metareg_b[!is.na(dat_metareg_b$se_1),]

#merge covariates on to file

covs_input<-read.csv("FILEPATH")

dat_metareg2<-merge(dat_metareg_b,covs_input,by=c("location_id","year_start"))



#####calculate the ratio (this is  passive (alt) to active (ref))
dat_metareg2<-dat_metareg2 %>% 
  mutate(ratio=mean_2/mean_1)

#drop ratio that is around 1

summary(dat_metareg2$ratio)


plot(dat_metareg2$ihme_loc_id, dat_metareg2$ratio)

#plot input data by location
dat_metareg2$ln_ratio<-log(dat_metareg2$ratio)

ggplot(dat_metareg2, aes(x=ihme_loc_id, y=ratio)) +
  geom_point(color='red', size=3) + theme_bw()+
   xlab("Location") + ylab("Ratio") +ggtitle("Passive/Active Ratio") 

#make scatter plot of covariate values


ggplot(dat_metareg_b, aes(x=sdi_mean, y=ln_ratio)) +
  geom_point(color='red', size=3) +
  ylim(-4, 0)+ xlab("Mean SDI") + ylab("ln(Ratio)") +ggtitle("SDI v. ln(Ratio)") 

ggplot(dat_metareg2, aes(x=mean_2, y=ln_ratio)) +
  geom_point(color='red', size=3) +
  ylim(-4, 0)+ xlab("Mean incidence") + ylab("ln(Ratio)") +ggtitle("mean_incidence v. ln(Ratio)") 

dat_metareg2$log_inc1<-log(dat_metareg2$mean_1)
#inc mean_2 = passive
dat_metareg2$log_inc2<-log(dat_metareg2$mean_2)

#ratio against passive incidence
plot(dat_metareg2$ln_ratio,dat_metareg2$mean_2)



#categorize SDI and HAQI

summary(dat_metareg2$sdi_mean)
summary(dat_metareg2$haqi_mean)
summary(dat_metareg2$dengue_prob_mean)

#use median values of covariate for these studies

dat_metareg2b<-dat_metareg2%>%
  mutate(sdi_cat=if_else(sdi_mean>.5224,1,0))%>%
  mutate(hq_cat=if_else(haqi_mean>53,1,0))%>%
  mutate(deng_prob=ifelse(dengue_prob_mean>0.85,1,0))
 

dat_metareg3<-dat_metareg2b%>%
  mutate(m2_cat1=if_else((0.001<=mean_2 & mean_2<.002),1,0))%>%
  mutate(m2_cat2=if_else((0.002<=mean_2 & mean_2<.003),1,0))%>%
  mutate(m2_cat3=if_else((0.003<=mean_2 & mean_2<.004),1,0))%>%
  mutate(m2_cat4=if_else((0.004<=mean_2),1,0))

#calculate se:
attach(dat_metareg3)
dat_metareg3$ratio_se<-sqrt((mean_2^2/mean_1^2)*((se_1^2/mean_1^2)+(se_2^2/mean_2^2)))
detach()

#drop null ratio se
dat_metareg3<-dat_metareg3[!is.na(dat_metareg3$ratio_se),]

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
metareg_vars <- c(ratio_var, ratio_se_var, cov_names)
extra_vars <- c("nid")
metareg_vars2 <- c(metareg_vars, extra_vars)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg3) %>%
  .[, metareg_vars2] %>%
  setnames(metareg_vars, c("ratio", "ratio_se", cov_names))


# log transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_log <- log(tmp_orig$mean)
tmp_orig$se_log <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

# log transform the meta-regression data
tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
tmp_metareg$ratio_se_log <- sapply(1:nrow(tmp_metareg), function(i) {
  ratio_i <- tmp_metareg[i, "ratio"]
  ratio_se_i <- tmp_metareg[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


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

covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

#run with trim
fit1 <- run_mr_brt(
  output_dir = paste0("/homes/", Sys.info()["user"], "/"), # user home directory
  model_label = "dengue_ef_xwalk__rev_wk2",
  data = tmp_metareg,
  mean_var = "ratio_log",
  se_var = "ratio_se_log",
 # trim_pct=.1,
 # method="trim_maxL",
  study_id="nid",
  covs = list(cov_info(cov_names, "X")),
  #covs = list(
  #cov_info(
   #covariate = "mean_2", 
  #  design_matrix = "X", 
  # degree = 2,
  #  n_i_knots = 2, 
   # bspline_gprior_mean = "0,0,0", 
   #bspline_gprior_var = "Inf,Inf,Inf",
  #  r_linear = TRUE,
   # l_linear = TRUE
  #)),
 overwrite_previous = TRUE)
#)






check_for_outputs(fit1)
plot_mr_brt(fit1)


df_pred <- as.data.frame(tmp_orig[, cov_names]) # cov_names from above
names(df_pred) <- cov_names
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
    mean_log_tmp = mean_log - pred, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_log_tmp = se_log^2 + pred_se^2, # adjust the variance
    se_log_tmp = sqrt(var_log_tmp)
  )

# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
  mutate(
    mean_log_adjusted = if_else(ref == 1, mean_log, mean_log_tmp),
    se_log_adjusted = if_else(ref == 1, se_log, se_log_tmp),
    lo_log_adjusted = mean_log_adjusted - 1.96 * se_log_adjusted,
    hi_log_adjusted = mean_log_adjusted + 1.96 * se_log_adjusted,
    mean_adjusted = exp(mean_log_adjusted),
    lo_adjusted = exp(lo_log_adjusted),
    hi_adjusted = exp(hi_log_adjusted) )

tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "mean_log_adjusted"]
  ratio_se_i <- tmp_orig3[i, "se_log_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})


# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original, 
  tmp_orig3[, c("mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data$ref<-as.factor(final_data$ref)

#update with adjusted cases for sex and age_splitting

#output final data

final_data$new_cases<-final_data$mean_adjusted*final_data$sample_size
final_data$data<-final_data$mean_adjusted
final_data$variance<-final_data$se_adjusted^2
final_data$orig_mean<-final_data$mean

#for zero records, recalculate variance (can't be missing for use in st/gpr)
final_data2<-final_data%>%
  mutate(variance=if_else(is.na(variance),0,variance))

#drop zero values under assumption they are implausible, not modeled in st/gpr
final_data2<-final_data2[final_data2$data!=0,]

#distribution of new data
summary(final_data2$data)

final_data2$new_r<-final_data2$mean_adjusted/final_data2$mean
summary(final_data2$new_r)


plot(final_data2$mean,final_data2$new_r)

final_data2$val<-final_data2$mean_adjusted



write.csv(final_data2,"FILEPATH")


#plot observed v. crosswalked data
# Same, but with different colors and add regression lines


ggplot(final_data2, aes(x=mean, y=mean_adjusted, color=ref)) +
  geom_point(shape=1) + theme_bw()+
  scale_colour_hue(l=50) + 
  xlim(0, 1.5)+
  ylim(0, 1.5)+ xlab("Reported Incidence") + ylab("Adjusted Incidence") +ggtitle("Reported v. Adjusted Incidence")



test_ind<-final_data2[final_data2$location_id==163,]
summary(test_ind$new_r)
summary(test_ind$data)

test_high<-final_data2[final_data2$mean_adjusted>1,]
summary(test_high$mean)
summary(test_high$new_r)

source(sprintf("FILEPATH",prefix))

locs_ids<-get_location_metadata(gbd_round_id=6, location_set_id=22)



final_data3<-merge(final_data2, locs_ids, by="location_id")
final_data3$reg_f <- as.factor(final_data3$region_id)


##plots of countries
#Peru, age group 9, males

peru<-final_data3[final_data3$location_id==123 & final_data3$age_group_id==9 & final_data3$sex_id==1,]
peru$inc<-peru$mean_adjusted
peru$type<-"adjusted"

peru2<-peru
peru2$inc<-peru2$orig_mean
peru2$type<-"original"


#bind
peru_plot<-rbind(peru,peru2)


ggplot(peru_plot, aes(x=year_id, y=inc, color=type)) +
  geom_point(size=2) + theme_bw()+
  scale_colour_hue(l=50) + 
  #xlim(0, .4)+
  #ylim(0, .4)+ 
  xlab("Year") + ylab("Incidence")
