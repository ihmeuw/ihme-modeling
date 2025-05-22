################################################################################
##      TITLE     asthma crosswalks GBD 2023
##      PURPOSE   updated asthma xwalk 
################################################################################


#load packages
library(writexl)
library(knitr)
library(kableExtra)
library(purrr)
library(openxlsx)
library(tidyr)
library(zoo)
library(stringr)
library(readr)
library(ggplot2)
library(data.table)
library(dplyr)
library(reticulate)
reticulate::use_python("/filepath/")
mr <- import("mrtool")
cw <- import("crosswalk")
invisible(sapply(list.files("/filepath/r/", full.names = TRUE), source)) 


#read-in input data, if running in MR, then post age split
df_at <- read.csv("/filepath/data.csv")

#remove any both sex, group_review = 0
df_at <- df_at %>%
  filter(sex != "Both",
         group_review == 1)

#separate clinical from scilit, only for MR
sci_lit_asthma <- df_prexwalk[is.na(clinical_version_id) | clinical_version_id == ""]

#case definitions will be matched on column, "case_diagnostics", use cv_xxx columns to fill in missing 
#check if any are missing 

case_def_counts <- table(df_at$case_diagnostics)
print(case_def_counts)

df_at <- df_at %>%
  filter(case_diagnostics != "CLAIMS")

#prep data for matching
loc <- get_location_metadata(location_set_id = 9, release_id = 16)
loc_min <- loc %>%
  dplyr::select(location_id, location_name, region_id, region_name, super_region_name, super_region_id)

prevalence <- df_at %>%
  filter(measure == "prevalence",
         is_outlier == 0,
         mean != 0)

incidence <- df_at %>%
  filter(measure == "incidence",
         is_outlier == 0,
         mean != 0)

prevalence <- as.data.table(prevalence)
incidence <- as.data.table(incidence)

#save outliers and 0 
prev_0 <- df_at %>%
  filter(measure == "prevalence",
         mean == 0) #1707 entries

prev_outlier <- df_at %>%
  filter(is_outlier == 1) # 9 outliers

inc_0 <- df_at %>%
  filter(measure == "incidence",
         mean == 0) # 9 entries

inc_outlier <- df_at %>%
  filter(is_outlier == 1) # 9 outliers

#generate id columns to match after removing zeros and outliers
df <- prevalence %>% mutate(
  midage_xwalk=(age_end+age_start)/2,
  age_cat=cut(midage_xwalk, breaks = seq(0, 120, by= 10), right = FALSE),
  midyear_xwalk=(year_start+year_end)/2,
  year_cat=cut(midyear_xwalk, breaks = seq(1900, 2025, by=5), right=FALSE),
  id= paste(age_cat, year_cat, sex, location_id, sep="-"), #id var
  orig_row=1:nrow(.))  


df_inc <- incidence %>% mutate(
  midage_xwalk=(age_end+age_start)/2,
  age_cat=cut(midage_xwalk, breaks = seq(0, 120, by= 10), right = FALSE),
  midyear_xwalk=(year_start+year_end)/2,
  year_cat=cut(midyear_xwalk, breaks = seq(1900, 2025, by=5), right=FALSE),
  id= paste(age_cat, year_cat, sex, location_id, sep="-"), #id var
  orig_row=1:nrow(.)) 

#match
alt_defs <-unique(df[case_diagnostics!="WHEEZE_AND_DIAGNOSIS"]$case_diagnostics)
gold_standard <- unique(df[case_diagnostics %like% "WHEEZE_AND_DIAGNOSIS",]$case_diagnostics)

method_var<- "case_diagnostics"
gold_def<- "WHEEZE_AND_DIAGNOSIS"
keep_vars<- c("orig_row", "id", "mean", "standard_error", "nid")

df <- as.data.frame(df)

df_matched_all_defs <- do.call("rbind", lapply(unique(df$id), function(i) {
  dat_i<- filter(df, id==i) %>% mutate(dorm=get(method_var))
  keep_vars<- c("dorm", keep_vars)
  row_ids<- expand.grid(idx1=1:nrow(dat_i), idx2=1:nrow(dat_i))
  do.call("rbind", lapply(1:nrow(row_ids), function(j){
    dat_j <- row_ids[j, ]
    dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, keep_vars]
    dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, keep_vars]
    filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
  })) %>% mutate(id=i) %>% select(-idx1, -idx2)
}))

df_inc <- as.data.frame(df_inc)

df_matched_inc <- do.call("rbind", lapply(unique(df_inc$id), function(i) {
  dat_i<- filter(df_inc, id==i) %>% mutate(dorm=get(method_var))
  keep_vars<- c("dorm", keep_vars)
  row_ids<- expand.grid(idx1=1:nrow(dat_i), idx2=1:nrow(dat_i))
  do.call("rbind", lapply(1:nrow(row_ids), function(j){
    dat_j <- row_ids[j, ]
    dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, keep_vars]
    dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, keep_vars]
    filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
  })) %>% mutate(id=i) %>% select(-idx1, -idx2)
}))

#remove duplicates(only required for network---------------------------------------
df_matched_all_defs <- as.data.table(df_matched_all_defs)
df_matched_inc <- as.data.table(df_matched_inc)
#this creates a new column with combo of orig ids, and uses this to de-duplicate
df_matched_all_defs[, combined_ids:= paste(pmin(orig_row_alt, orig_row_ref), pmax(orig_row_alt, orig_row_ref)), by = .I]
df_matched_inc[, combined_ids:= paste(pmin(orig_row_alt, orig_row_ref), pmax(orig_row_alt, orig_row_ref)), by = .I]

df_matched_dedup <- df_matched_all_defs %>% filter(!duplicated(combined_ids))
df_matched_dedup_inc <- df_matched_inc %>% filter(!duplicated(combined_ids))

#this shows number of pairs per each ref alt
df_matched_dedup[, countcombo := .N, by = .(dorm_alt, dorm_ref)]
df_matched_dedup_inc[, countcombo := .N, by = .(dorm_alt, dorm_ref)]

refcombo_dedup <- df_matched_dedup %>% select(dorm_alt, dorm_ref, countcombo) %>% distinct()
refcombo_inc <- df_matched_dedup_inc %>% select(dorm_alt, dorm_ref, countcombo) %>% distinct()

df_tologit <-  copy(df_matched_dedup)
df_tologit <- as.data.table(df_tologit)


#transform linear to logit for reference and alternative values
#alternative 
df_tologit[, c("alt_mean2", "alt_se2")]<-as.data.frame(
  cw$utils$linear_to_logit(
    mean = array(df_tologit$mean_alt),
    sd = array(df_tologit$standard_error_alt))
)

#reference
df_tologit[, c("ref_mean2", "ref_se2")] <- as.data.frame(
  cw$utils$linear_to_logit(
    mean = array(df_tologit$mean_ref),
    sd = array(df_tologit$standard_error_ref)))

#get the logit different 
df_tologit<-  df_tologit %>%
  mutate(
    ydiff_log_mean = alt_mean2 - ref_mean2,
    ydiff_log_se = sqrt(alt_se2^2 + ref_se2^2)
  )

df_tologit$id <- as.integer(as.factor(df_tologit$id))
colnames(df_tologit)

# format data for meta-regression; pass in data.frame and variable names
dat1 <- cw$CWData(
  df = df_tologit,
  obs = "ydiff_log_mean",   # matched differences in log space
  obs_se = "ydiff_log_se",  # SE of matched differences in log space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",   # var for the reference def/method
  covs = list(),            # list of (potential) covariate column names
  study_id = "id"          # var for random intercepts; i.e. (1|study_id)
)

fit1 <- cw$CWModel(
  cwdata = dat1,           # result of CWData() function call
  obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
  cov_models = list(       # specify covariate details
    cw$CovModel("intercept") ),
  gold_dorm = "WHEEZE_AND_DIAGNOSIS",# level of 'ref_dorms' that's the gold standard
  order_prior = list(c("SELF_REPORT_CURRENT", "SELF_REPORT_EVER"))
)

fit1$fit()  

df_tmp <- fit1$create_result_df() 
def<-unique(df_tmp$dorms)

xwalk_output <- "/ihme/homes/andnakam/"
#save .pkl file
py_save_object(object = dat1, filename = file.path(xwalk_output, "asthma_at_dat1.pkl"), pickle = "dill")
py_save_object(object = fit1, filename = file.path(xwalk_output, "asthma_at_fit1.pkl"), pickle = "dill")

write.csv(df_tmp, paste0(xwalk_output,"/betas_version_final_2_2_25.csv"))

#funnel plot
plots <- import("crosswalk.plots")
df_tmp<-
  defs_network<-unique(df_tmp$dorms)
defs_network<-defs_network[defs_network!="WHEEZE_AND_DIAGNOSIS"]

for (i in defs_network) {
  plots$funnel_plot(
    cwmodel = fit1, 
    cwdata = dat1,
    continuous_variables = list(),
    obs_method = i,
    plot_note = paste0("network xwalk :WHEEZE_AND_DIAGNOSIS_vs",i),
    plots_dir = paste0(output_path,"/"), 
    file_name = paste0(i,"_funnel_plot"),
    write_file = TRUE
  )
  
}

#apply coefficients
df2 = df
df2 <- as.data.table(df2)

pred_tmp <- fit1$adjust_orig_vals(
  df = df2 ,
  orig_dorms = "case_diagnostics",
  orig_vals_mean = "mean", # mean in un-transformed space
  orig_vals_se = "standard_error"      # SE in un-transformed space
)

head(pred_tmp)

#add the adjusted values back to the original dataset
df2[, c(
  "meanvar_adjusted", "sdvar_adjusted", "pred_logit", 
  "pred_se_logit", "data_id")] <- pred_tmp

###incidence 

df_inc_tologit <-  copy(df_matched_dedup_inc)
df_inc_tologit <- as.data.table(df_inc_tologit)


#transform linear to logit for reference and alternative values
#alternative 
df_inc_tologit[, c("alt_mean2", "alt_se2")]<-as.data.frame(
  cw$utils$linear_to_logit(
    mean = array(df_inc_tologit$mean_alt),
    sd = array(df_inc_tologit$standard_error_alt))
)

#reference
df_inc_tologit[, c("ref_mean2", "ref_se2")] <- as.data.frame(
  cw$utils$linear_to_logit(
    mean = array(df_inc_tologit$mean_ref),
    sd = array(df_inc_tologit$standard_error_ref)))

#get the logit different 
df_inc_tologit<-  df_inc_tologit %>%
  mutate(
    ydiff_log_mean = alt_mean2 - ref_mean2,
    ydiff_log_se = sqrt(alt_se2^2 + ref_se2^2)
  )

df_inc_tologit$id <- as.integer(as.factor(df_inc_tologit$id)) # ...housekeeping
colnames(df_inc_tologit)

# format data for meta-regression; pass in data.frame and variable names
dat1_inc <- cw$CWData(
  df = df_inc_tologit,
  obs = "ydiff_log_mean",   # matched differences in log space
  obs_se = "ydiff_log_se",  # SE of matched differences in log space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",   # var for the reference def/method
  covs = list(),            # list of (potential) covariate column names
  study_id = "id"          # var for random intercepts; i.e. (1|study_id)
)

fit1_inc <- cw$CWModel(
  cwdata = dat1_inc,           # result of CWData() function call
  obs_type = "diff_logit",   # must be "diff_logit" or "diff_log"
  cov_models = list(       # specify covariate details
    cw$CovModel("intercept")),
  gold_dorm = "WHEEZE_AND_DIAGNOSIS",# level of 'ref_dorms' that's the gold standard
)

fit1_inc$fit()  

df_tmp_inc <- fit1_inc$create_result_df()


#apply the model fit
df_inc <- df_inc %>%
  mutate(case_diagnostics = ifelse(case_diagnostics == "SELF_REPORT_CURRENT", "WHEEZE_AND_DIAGNOSIS", case_diagnostics))

df2_inc = df_inc
df2_inc <- as.data.table(df2_inc)

pred_tmp_inc <- fit1_inc$adjust_orig_vals(
  df = df2_inc ,
  orig_dorms = "case_diagnostics",
  orig_vals_mean = "mean", # mean in un-transformed space
  orig_vals_se = "standard_error"      # SE in un-transformed space
)

head(pred_tmp)
df2_inc <- copy(incidence)
#add the adjusted values back to the original dataset
df2_inc[, c(
  "meanvar_adjusted", "sdvar_adjusted", "pred_logit", 
  "pred_se_logit", "data_id")] <- pred_tmp_inc

#read-in non incidence or prevalence bundle data
asthma_at <- get_bundle_version(49577)

#103 remission, mtwith, relrisk, mtstandard, mtexcess also in bundle. Only remission sources have been updated within the last 5+ years
asthma_at_no_prev_inc <- asthma_at %>% 
  filter(measure != "prevalence" & measure != "incidence")

#format to save crosswalk version
df2_inc <- df2_inc %>%
  mutate(crosswalk_parent_seq = if_else(pred_logit == 0, "", as.character(seq)))

df2_inc$crosswalk_parent_seq <- as.numeric(df2_inc$crosswalk_parent_seq)

df2_inc <- df2_inc %>%
  rename(
    mean_original = mean,
    standard_error_original = standard_error
  )

df2_inc <- df2_inc %>%
  rename(
    mean = meanvar_adjusted,
    standard_error = sdvar_adjusted,
    lower_original = lower,
    upper_original = upper
  )

df2_inc <- df2_inc %>%
  mutate(lower = as.numeric(NA), upper = as.numeric(NA))

df2 <- df2 %>%
  mutate(crosswalk_parent_seq = if_else(pred_logit == 0, "", as.character(seq)))

df2$crosswalk_parent_seq <- as.numeric(df2$crosswalk_parent_seq)

df2 <- df2 %>%
  rename(
    mean_original = mean,
    standard_error_original = standard_error
  )

df2 <- df2 %>%
  rename(
    mean = meanvar_adjusted,
    standard_error = sdvar_adjusted,
    lower_original = lower,
    upper_original = upper
  )

df2 <- df2 %>%
  mutate(lower = as.numeric(NA), upper = as.numeric(NA))

# Plot for all definitions combined
p_all_prev <- ggplot(df2, aes(x=mean, y=meanvar_adjusted, color=case_diagnostics)) +
  geom_point(size=0.6) +
  geom_abline(intercept = 0, slope = 1, color = "black", linetype = "solid", size=0.6) +  # x=y line
  theme_light() +
  labs(title="Asthma - Network Crosswalk for Dismod-AT",
       subtitle="All definitions",
       x="Original mean (post sex-split only)",
       y="Crosswalked mean")

print(p_all_prev)

ggsave("plot.png", plot = p_all_prev, width = 13, height = 10)





