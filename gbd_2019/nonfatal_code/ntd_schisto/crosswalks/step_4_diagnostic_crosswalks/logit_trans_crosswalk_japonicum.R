
rm(list=ls())

library(readxl)
library(dplyr)
library(data.table)
library(msm)
library(msm, lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")

readxl::excel_sheets("FILEPATH")
df <- readxl::read_xlsx("FILEPATH", sheet = 'japonicum')


#calculating the ratio and se in logit space

library(boot)

df$se_logit_mean_1 <- sapply(1:nrow(df), function(i) {
  ratio_i <- df[i, "mean_1"]
  ratio_se_i <- df[i, "se_1"]
  deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
})

df$se_logit_mean_2 <- sapply(1:nrow(df), function(a) {
  ratio_a <- df[a, "mean_2"]
  ratio_se_a <- df[a, "se_2"]
  deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
})

df <- df %>%
  mutate(
    logit_mean_1 = logit(mean_1),
    logit_mean_2 = logit(mean_2),
    diff_logit = logit_mean_1 - logit_mean_2,
    se_diff_logit = sqrt(se_logit_mean_1^2 + se_logit_mean_2^2)
  )

#subsetting for ratio, ratio_Se, and comparison
df2 <- select(df, ratio = diff_logit, ratio_se = se_diff_logit, comparison, NID)

#ratio = diff_logit
#ratio_se = se_diff_logit

#adding two more columns, that split the comparison column into alt and ref
df2[, c("alt", "ref")] <- do.call("rbind", strsplit(df2$comparison, split = "_"))

#saving all case definitions in a vector
case_defs <- unique(c(df2$alt, df2$ref))
#saving all alternative case defintions in a vector 
nonGBD_case_defs <- case_defs[!case_defs == "iha"]

for (i in nonGBD_case_defs) df2[, i] <- ifelse(df2$alt == i, 1, ifelse(df2$ref == i, -1, 0))

write.csv(df2, "FILEPATH", row.names = FALSE)


#####
readxl::excel_sheets(paste0(crosswalks_dir, "FILEPATH"))
dat_original <- readxl::read_xlsx((paste0(crosswalks_dir, "FILEPATH")), sheet = 'extraction')

#dropping any missing cases
dat_original<- subset(dat_original, sample_size!=cases)


colnames(dat_original)[colnames(dat_original)=="mean"] <- "pre_adjusted_mean"
colnames(dat_original)[colnames(dat_original)=="standard_error"] <- "pre_adjusted_standard_error"

dat_original$pre_adjusted_mean[dat_original$cases==0] <- 0

#subsetting by specie
dat_original_japon<- subset(dat_original, case_name=="S japonicum")

dat_original_japon$case_diagnostics <- as.character(dat_original_japon$case_diagnostics)

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" | dat_original_japon$case_diagnostics=="fecal smear" | dat_original_japon$case_diagnostics=="sedimentation"] <- "Kato-Katz"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="IHA"] <- "iha"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="nylon silk method"] <- "hatch"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="ELISA"] <- "elisa"

dat_original_japon<- subset(dat_original_japon, dat_original_japon$case_diagnostics != "NA")

dat_original_japon$num_samples <- as.double(dat_original_japon$num_samples)

#renaming values under case_diagnostics

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==1] <- "kk1"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==2] <- "kk2"


dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==3] <- "kk3"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$num_samples==4] <- "kk3"

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz"] <- "kk1"


unique(dat_original_japon$case_diagnostics)

length(which(dat_original_japon$case_diagnostics == "kk1")) 
length(which(dat_original_japon$case_diagnostics == "kk2")) 
length(which(dat_original_japon$case_diagnostics == "kk3")) 
length(which(dat_original_japon$case_diagnostics == "iha")) 
length(which(dat_original_japon$case_diagnostics == "hatch")) 
length(which(dat_original_japon$case_diagnostics == "elisa")) 

dat_original_japon <- dat_original_japon %>%
  mutate(
    elisa = ifelse(case_diagnostics == "elisa", 1, 0),
    hatch = ifelse(case_diagnostics == "hatch", 1, 0),
    kk1 = ifelse(case_diagnostics == "kk1", 1, 0),
    kk2 = ifelse(case_diagnostics == "kk2", 1, 0),
    kk3 = ifelse(case_diagnostics == "kk3", 1, 0)
      )

reference_var <- "case_diagnostics"
reference_value <- "iha"
mean_var <- "pre_adjusted_mean"
se_var <- "pre_adjusted_standard_error"
cov_names <- c("elisa","kk1","kk2", "kk3", "hatch") # can be a vector of names


dat_metareg <- df2
ratio_var <- "ratio"
ratio_se_var <- "ratio_se"


# create datasets with standardized variable names (the extra vars may change based on meta-reg inputs)
#meta-reg dataset
metareg_vars <- c(ratio_var, ratio_se_var)
extra_vars <- c("kk1", "kk2", "kk3", "hatch", "elisa", "ref", "alt", "NID")
metareg_vars2 <- c(metareg_vars, extra_vars)
#excludes iha as that is the reference 

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars2] %>%
  setnames(metareg_vars, c("ratio", "ratio_se"))

#original dataset
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

tmp_orig <- as.data.frame(dat_original_japon) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

######################
# logit transform the original data
# -- SEs transformed using the delta method 

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
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

cov_list <- lapply(nonGBD_case_defs, function(x) cov_info(x, "X"))

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "crosswalk_network_schisto_logit_japonicum",
  data = tmp_metareg,
  mean_var = "ratio",
  se_var = "ratio_se",
  covs = cov_list,
  remove_x_intercept = TRUE,
  overwrite_previous = TRUE, 
  study_id = "NID"
)

check_for_outputs(fit1)

# this creates a ratio prediction for each observation in the original data
df_pred <- as.data.frame(tmp_orig[, cov_names])
names(df_pred) <- cov_names
pred1 <- predict_mr_brt(fit1, newdata = df_pred) 
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

plot_mr_brt(fit1)
plot_mr_brt(pred1)

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
    #prediciton (alt-ref), 
    var_logit_tmp = se_logit^2 + pred_se^2, # adjust the variance
    #variance is additive 
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


##to calculate standard errors for mean = 0 and mean =1
tmp_orig3$pred_se_invlogit <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "pred"]
  ratio_se_i <- tmp_orig3[i, "pred_se"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})

tmp_orig3$se_adjusted_zero <- sqrt(tmp_orig3$se^2 + tmp_orig3$pred_se_invlogit^2)


#apply this se_adjusted_zero not only to mean = 0 but also mean =1
tmp_orig3$se_adjusted[tmp_orig3$mean_adjusted==0] <- tmp_orig3$se_adjusted_zero
tmp_orig3$se_adjusted[tmp_orig3$mean_adjusted==1] <- tmp_orig3$se_adjusted_zero

#calculate high and low CIs for those problematic datapoints
tmp_orig3$high_adjusted_zero_one<- (tmp_orig3$mean_adjusted + (1.96*tmp_orig3$se_adjusted))
tmp_orig3$low_adjusted_one<- (tmp_orig3$mean_adjusted - (1.96*tmp_orig3$se_adjusted))

#apply this high_adjusted_zer_one to mean = 0 and mean =1
tmp_orig3$hi_adjusted[tmp_orig3$mean_adjusted==0] <- tmp_orig3$high_adjusted_zero_one
tmp_orig3$hi_adjusted[tmp_orig3$mean_adjusted==1] <- 1

#apply this low_adjusted_one to mean 1
tmp_orig3$lo_adjusted[tmp_orig3$mean_adjusted==1] <- tmp_orig3$low_adjusted_one

#applying celings to higher and lower CIs as there are a few that are >1 and <0 cause of above calculations
tmp_orig3$hi_adjusted[tmp_orig3$hi_adjusted>1] <- 1
tmp_orig3$lo_adjusted[tmp_orig3$lo_adjusted<0] <- 0

# 'final_data' is the original extracted data plus the new variables
final_data_japon <- cbind(
  dat_original_japon, 
  tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)


drops <- c("elisa","hatch", "kk1", "kk2", "kk3", "ref")
final_data_japon<- final_data_japon[ , !(names(final_data_japon) %in% drops)]

#renaming the adjusted mean and se to variables that wil be accepted by DISMOD
colnames(final_data_japon)[colnames(final_data_japon)=="mean_adjusted"] <- "mean"
colnames(final_data_japon)[colnames(final_data_japon)=="se_adjusted"] <- "standard_error"

write.csv(final_data_japon, file="final_data_logit_japon.csv")

#final_data<- read.csv("final_test_data_logit_japon.csv")
#plots - final_data 
library(ggplot2)

#this worked:
ggplot(final_data_japon, aes(x=pre_adjusted_mean, y=mean, color=case_diagnostics)) +
  geom_point(size=1.0) + 
  labs(color="Diagnostics", x="Unadjusted prevalence", y="Adjusted prevalence")+
  scale_color_brewer(palette="Dark2")

#to plot the logit differences
readxl::excel_sheets("FILEPATH")
model_coef <- readxl::read_xlsx("FILEPATH")


#this worked
p<- ggplot(model_coef, aes(x=Diagnostic, y=beta_soln, color=Diagnostic)) + 
  geom_line()+
  geom_point()+
  geom_errorbar(aes(ymin=lower_ci, ymax=upper_ci), width=.2,
                position=position_dodge(0.05)) 


p+labs(title="Predicted Logit Difference in reference to CCA", x="Diagnostic", y = "Predicted Logit Difference")




# Box plot
bp + scale_fill_manual(breaks = c("2", "1", "0.5"), 
                       values=c("red", "blue", "green"))
# Scatter plot
sp + scale_color_manual(breaks = c("cca", "kk1", "kk2", "kk3", "sed", "fec", "elisa", "pcr"),
                        values=c("red", "blue", "black", "green", "orange", "yellow", "pink", "purple"))

sp + scale_fill_discrete(breaks=c("cca", "kk1", "kk2", "kk3", "sed", "fec", "elisa", "pcr"))


p<- ggplot(df2, aes(x=dose, y=len, group=supp, color=supp)) + 
  geom_line() +
  geom_point()+
  geom_errorbar(aes(ymin=len-sd, ymax=len+sd), width=.2,
                position=position_dodge(0.05))

library(ggplot2)
box <- ggplot(model_coef, aes(x=x_cov, y=beta_var, fill=x_cov)) + 
  geom_boxplot()
box

mean<- model_coef$beta_soln
lower<- model_coef$lower_ci
upper<- model_coef$upper_ci

plot(mean,
     ylim=range(-1.5, 1.5),
     size=1, pch=1, xlab="Measurements", ylab="Mean +/- SD",
     main="Scatter plot with std.dev error bars")



arrows(x, avg-sdev, x, avg+sdev, length=0.05, angle=90, code=3)




sp<- ggplot(final_data, aes(x=mean_unadjusted, y=mean_adjusted, color=case_diagnostics)) +
  geom_point(size=1.0) 

sp + scale_color_brewer(palette="Dark2")

sp + legend(x=1,y=1,legend="Diagnostics")


sp + labs(color = "Diagnostics")

sp<- ggplot(final_data, aes(x=mean_unadjusted, y=mean_adjusted, color=case_diagnostics)) +
  geom_point(size=1.0) + 
  labs(color="Diagnostics", x="Unadjusted prevalence", y="Adjusted prevalence")+
  scale_color_brewer(palette="Dark2")

