

rm(list=ls())

Sys.umask(mode = 002)

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(ggplot2)
library(msm)
library(crosswalk, lib.loc = "FILEPATH")
library(readxl)

source("FILEPATH/merge_on_location_metadata.R")

# source all central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

ids <- fread("FILEPATH")

bundle_id = 5882 

bv_id = 31085


# ----- GA MICRODATA CROSSWALK

agesex_split_data_fp <- "FILEPATH"

ga_28_map <- fread("FILEPATH")

data <- data.table(read_excel(agesex_split_data_fp))

data[is.na(clinical_data_type), clinical_data_type := "non_clinical"]

data[sex == "Male", sex_id := 1]
data[sex == "Female", sex_id := 2]

data[, mean_adj_agesex := mean]
data[, se_adj_agesex := standard_error]

data <- data[, -c("variance", "var_adj", "se_unadj", "mean_unadj", "mean_adj")]

files <- list.files("FILEPATH", full.names = T)
files <- rbindlist(lapply(files, readRDS))

pdf("FILEPATH")

files_plot <- copy(files)

files_plot <- dcast(files_plot, nid + location_id + year_id + sex_id + me_name ~ paste0("mean_", ga_method), value.var = "prevalence")

ggplot(files_plot[me_name == "ga_28"]) + 
  geom_point(aes(x = mean_lmp, y = mean_oe), color = "blue") + 
  geom_abline(slope = 1, intercept = 0) + theme_bw() + 
  ggtitle("GA28 LMP-OE Matches") + xlim(0,0.02) + ylim(0,0.02) 

dev.off()

files <- files[me_name == "ga_28" & ga_method == "oe"]
files <- files[sex_id != 3]
files <- files[, -c("me_name", "ga_method")]
setnames(files, c("prevalence","sample_size", "cases"), c("mean_adj_LMP", "sample_size_LMP", "cases_LMP"))

z <- qnorm(0.975)
files[, se_adj_LMP := (1/(1+z^2/sample_size_LMP)) * sqrt(mean_adj_LMP*(1-mean_adj_LMP)/sample_size_LMP + z^2/(4*sample_size_LMP^2))]
files[, microdata_lmp_tag := 1]
# files[, mean_adj_agesex := mean]
# files[, se_adj_agesex := standard_error]

data <- merge(data, files, all.x = T)

data[!is.na(sample_size_LMP), sample_size := sample_size_LMP]
data[!is.na(cases_LMP), cases := cases_LMP]
data[!is.na(mean_adj_LMP), mean := mean_adj_LMP]
data[!is.na(se_adj_LMP), standard_error := se_adj_LMP]


data <- data[, -c("sample_size_LMP", "cases_LMP")]


# ----- GA_28 LMP Prevalence Crosswalk

ga_model <- py_load_object(filename = paste0("FILEPATH"), pickle = "dill")

data <- merge(data, ga_28_map[, .(nid, ga_estimate_method_tag)], all.x = T, by = "nid")

data[microdata_lmp_tag == 1, ga_estimate_method_tag := "LMP"]


data[is.na(ga_estimate_method_tag) & is.na(cv_ga_37), ga_estimate_method_tag := "UNKNOWN"]
data[cv_ga_37 == 1, ga_estimate_method_tag := "NO_XWALK"]

noxwalk_data <- data[mean == 0 | mean == 1 | 
                       ga_estimate_method_tag %in% c("NO_XWALK", "UNKNOWN", "OTHER") | 
                       !is.na(mean_adj_LMP), ]

data <- data[!(mean == 0 | mean == 1 | 
                 ga_estimate_method_tag%in% c("NO_XWALK", "UNKNOWN", "OTHER") | 
                 !is.na(mean_adj_LMP)), ]

data <- data[, -c("microdata_lmp_tag")]

data[, c("mean_adj_LMP", "se_adj_LMP", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = ga_model,       # result of CWModel()
  df = data,            # original data with obs to be adjusted
  orig_dorms = "ga_estimate_method_tag", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

data[, mean := mean_adj_LMP]
data[, standard_error := se_adj_LMP]

noxwalk_data[is.na(mean_adj_LMP), mean_adj_LMP := mean]
noxwalk_data[is.na(se_adj_LMP), se_adj_LMP := standard_error]

data <- rbind(data, noxwalk_data, use.names = T, fill = T)

data <- data[, -c("diff", "diff_se", "data_id")]

write.csv(data, "FILEPATH", row.names = F, na = "")


pdf("FILEPATH", width =20, height = 12)

ylimend = 0.1

betas <- data.table(ga_model$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(ga_model$gamma, 5))


plot_data <- data[is.na(cv_ga_37)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_agesex, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28 LMP Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28 LMP Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[ga_estimate_method_tag %in% c("LMP") , ihme_loc_id], 1,3))){
  
  print(ihme_loc)
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_agesex, color = clinical_data_type, shape = ga_estimate_method_tag)) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 LMP Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag) ) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 LMP Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()




# ----- GA_28 GA_OTHER Prevalence Crosswalk


data <- fread("FILEPATH")

data[ga_estimate_method_tag %in% c("UNKNOWN", "OTHER"), ga_estimate_method_tag := "OtherGA"]

# Remove from pool of crosswalk matches ga_37 data, inpatient, or MarketScan 2000 (issues in data) 
noxwalk_data <- data[cv_ga_37 == 1 | ga_estimate_method_tag %in% c("NO_XWALK", "LMP") | mean == 0 | mean == 1]
data <- data[is.na(cv_ga_37) & !( ga_estimate_method_tag %in% c("NO_XWALK", "LMP")  | mean == 0 | mean == 1) ]


# Create a temporary index
data[, temp_index := .I]

data[, mid_year := ( year_start + year_end ) / 2]
data[, mid_year_cat := cut(mid_year, breaks = seq(1990, 2025, 1))]

matches <- merge(data[ga_estimate_method_tag == "OE", .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean_OE = mean, se_OE = standard_error, nid.denom = nid)], 
                 data[ga_estimate_method_tag == "OtherGA", .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean_OtherGA = mean, se_OtherGA = standard_error, nid.num = nid)], allow.cartesian = T)

# Remove US Claims & Inpatient from matches pool to inform the crosswalk
matches <- matches[!(nid.num %in% data[clinical_data_type %in% c("inpatient", "claims"), nid]) ]

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = matches$mean_OtherGA, 
    sd = matches$se_OtherGA,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = matches$mean_OE, 
    sd = matches$se_OE,
    transformation = "linear_to_logit")
))

names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

matches[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

matches[, dorm_alt := "OtherGA"]
matches[, dorm_ref := "OE"]


matches <- merge_on_location_metadata(matches)

pdf("FILEPATH", width = 15, height = 12)

ggplot(matches[]) + 
  geom_point(aes(x = mean_OtherGA, y = mean_OE), color = "blue", alpha = 0.2) + 
  geom_abline(slope = 1, intercept = 0) + 
  facet_wrap(~ihme_loc_id) + theme_bw() + xlim(0,0.02) + ylim(0,0.02) + 
  ggtitle("GA28 OtherGA-OE Matches")

dev.off()

matches_CWData <- CWData(
  df = matches,
  obs = "logit_diff",       # matched differences in logit space
  obs_se = "logit_diff_se", # SE of matched differences in logit space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",#,#
  covs = list("sex_id"), 
  study_id = "nid.num"
)

# fit model
matches_CWModel <- CWModel(
  cwdata = matches_CWData,           # result of CWData() function call
  obs_type = "diff_logit",
  cov_models = list(CovModel("intercept"), 
                    CovModel("sex_id")),# must be "diff_logit" or "diff_log"
  gold_dorm = "OE",   # level of 'ref_dorms' that's the gold standard
  use_random_intercept = T
)

py_save_object(object = matches_CWModel, filename = paste0("FILEPATH"), pickle = "dill")

data[, nid := as.character(nid)]

data[, c("mean_adj_OtherGA", "se_adj_OtherGA", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = matches_CWModel,       # result of CWModel()
  df = data,            # original data with obs to be adjusted
  orig_dorms = "ga_estimate_method_tag",  # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error",  # standard error of original mean,
  study_id = "nid"
)

data[, mean := mean_adj_OtherGA]
data[, standard_error := se_adj_OtherGA]


noxwalk_data[, mean_adj_OtherGA := mean]
noxwalk_data[, se_adj_OtherGA := standard_error]


data <- rbind(data, noxwalk_data, use.names = T, fill = T)

data <- data[, -c("diff", "diff_se", "data_id")]

write.csv(data, "FILEPATH", row.names = F, na = "")

repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = matches_CWModel, 
  cwdata = matches_CWData,
  continuous_variables = list("sex_id"),
  obs_method = 'OtherGA',
  plot_note = "ga_OtherGA_OE_xwalk", 
  plots_dir = "FILEPATH", 
  file_name = "ga_OtherGA_OE_xwalk_funnel",
  write_file = TRUE
)

pdf("FILEPATH", width =18, height = 12)

betas <- data.table(matches_CWModel$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(matches_CWModel$gamma, 5))

ylimend = 0.1

plot_data <- data[is.na(cv_ga_37)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_LMP, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28 OtherGA Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28 OtherGA Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[ga_estimate_method_tag %in% c("OtherGA"), ihme_loc_id], 1,3))){
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_LMP, color = clinical_data_type, shape = ga_estimate_method_tag)) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 OtherGA Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag) ) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 OtherGA Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()




# ----- Claims crosswalk



data <- fread("FILEPATH")

# Remove from pool of crosswalk matches ga_37 data, inpatient, or MarketScan 2000 (issues in data) 
noxwalk_data <- data[cv_ga_37 == 1 | clinical_data_type == "inpatient" | nid == 244369 | mean == 0 | mean == 1]
data <- data[is.na(cv_ga_37) & !(clinical_data_type == "inpatient" | nid == 244369 | mean == 0 | mean == 1) ]

# Create a temporary index
data[, temp_index := .I]

# Change to sex_id
data[sex == "Male", sex_id := 1]
data[sex == "Female", sex_id := 2]

data[, mid_year := ( year_start + year_end ) / 2]
data[, mid_year_cat := cut(mid_year, breaks = seq(1990, 2025, 1))]

matches <- merge(data[clinical_data_type == "non_clinical", .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean_other = mean, se_other = standard_error, nid.denom = nid)], 
                 data[clinical_data_type == "claims", .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean_claims = mean, se_claims = standard_error, nid.num = nid)], allow.cartesian = T)

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = matches$mean_claims, 
    sd = matches$se_claims,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = matches$mean_other, 
    sd = matches$se_other,
    transformation = "linear_to_logit")
))

names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

matches[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

matches[, dorm_alt := "claims"]
matches[, dorm_ref := "non_clinical"]

# # Remove Califonria outliers
# matches <- matches[mean_other > 0.01]

matches <- merge_on_location_metadata(matches)

pdf("FILEPATH", width = 15, height = 12)

ggplot(matches[]) + 
  geom_point(aes(x = mean_claims, y = mean_other), color = "blue", alpha = 0.2) + 
  geom_abline(slope = 1, intercept = 0) + 
  facet_wrap(~location_name) + theme_bw() + xlim(0,0.02) + ylim(0,0.02) + 
  ggtitle("GA28 Claims-VR Matches")

dev.off()



matches_CWData <- CWData(
  df = matches,
  obs = "logit_diff",       # matched differences in logit space
  obs_se = "logit_diff_se", # SE of matched differences in logit space
  alt_dorms = "dorm_alt",   # var for the alternative def/method
  ref_dorms = "dorm_ref",#,#
  covs = list("sex_id"), 
  study_id = "nid.num"
)

# fit model
matches_CWModel <- CWModel(
  cwdata = matches_CWData,           # result of CWData() function call
  obs_type = "diff_logit",
  cov_models = list(CovModel("intercept"), 
                    CovModel("sex_id")),# must be "diff_logit" or "diff_log"
  gold_dorm = "non_clinical",  # level of 'ref_dorms' that's the gold standard
  use_random_intercept = T
)

py_save_object(object = matches_CWModel, filename = paste0("FILEPATH"), pickle = "dill")

data[, nid := as.character(nid)]

data[, c("mean_adj_claims", "se_adj_claims", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = matches_CWModel,       # result of CWModel()
  df = data,            # original data with obs to be adjusted
  orig_dorms = "clinical_data_type",  # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error",  # standard error of original mean
  study_id = "nid"
)

data[, mean := mean_adj_claims]
data[, standard_error := se_adj_claims]

noxwalk_data[, mean_adj_claims := mean]
noxwalk_data[, se_adj_claims := standard_error]


data <- rbind(data, noxwalk_data, use.names = T, fill = T)

data <- data[, -c("diff", "diff_se", "data_id")]

write.csv(data, "FILEPATH", row.names = F, na = "")

repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = matches_CWModel, 
  cwdata = matches_CWData,
  continuous_variables = list("sex_id"),
  obs_method = 'claims',
  plot_note = "claims_vr_xwalk", 
  plots_dir = "FILEPATH", 
  file_name = "ga28_claims_vr_xwalk_funnel",
  write_file = TRUE
)

pdf("FILEPATH", width =18, height = 12)

betas <- data.table(matches_CWModel$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(matches_CWModel$gamma, 5))

ylimend = 0.1

plot_data <- data[is.na(cv_ga_37)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_LMP, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28 Claims Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28 Claims Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[clinical_data_type == "claims", ihme_loc_id], 1,3))){
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_LMP, color = clinical_data_type, shape = ga_estimate_method_tag) ) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 Claims Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = clinical_data_type, shape = ga_estimate_method_tag)) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28 Claims Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()



locs <- get_location_metadata(22)[,.(location_id,path_to_top_parent,level,location_name)]

pdf("FILEPATH", width=20, height=10) 

for(l in c(0:5)) { 
  print(paste0("l = ",l))
  data <- copy(plot_data)
  
  data <- melt(data, id.vars = c("clinical_data_type", "path_to_top_parent", "ga_estimate_method_tag", "location_id", "year_id", "sex_id", "nid"), measure.vars = c("mean_adj_agesex", "mean_adj_LMP", "mean_adj_OtherGA", "mean_adj_claims"))
  
  
  if(l == 0){
    data[, parent_loc_id := 0]
    data[, page_loc_id := 1]
  } else if (l == 1) {
    data[, parent_loc_id := 1]
    data[, page_loc_id := 1]
  } else {
    data[, parent_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l - 1)]
    data[, parent_loc_id := as.integer(parent_loc_id)]
    data[, page_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l)]
    data[, page_loc_id := as.integer(page_loc_id)]
  }
  
  data[, plot_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l+1)]
  data[, plot_loc_id := as.integer(plot_loc_id)]
  data <- data[!is.na(plot_loc_id)]
  
  for(plot_loc in unique(na.omit(data$plot_loc_id))){
    page_data_est <- data[plot_loc_id == plot_loc, ]
    page_data_est <- merge(page_data_est, locs[, list(plot_loc_name = location_name, plot_loc_id = location_id)], all.x= T, by = "plot_loc_id")
    
    if(nrow(page_data_est) == 0){
      gg <- ggplot() + geom_blank() + ggtitle(paste0("No data for ",  unique(locs[location_id == plot_loc, location_name])))
      print(gg)
      next
    }
    
    gg <- ggplot(data=page_data_est, aes(x=plot_loc_name, y=value, fill = variable)) + 
      geom_boxplot() + theme_bw() + ylim(0,0.03)
    
    print(gg)
  }
  
}

dev.off()



pdf("FILEPATH", width=20, height=10) 

for(l in c(0:5)) { 
  print(paste0("l = ",l))
  data <- copy(plot_data)
  
  if(l == 0){
    data[, parent_loc_id := 0]
    data[, page_loc_id := 1]
  } else if (l == 1) {
    data[, parent_loc_id := 1]
    data[, page_loc_id := 1]
  } else {
    data[, parent_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l - 1)]
    data[, parent_loc_id := as.integer(parent_loc_id)]
    data[, page_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l)]
    data[, page_loc_id := as.integer(page_loc_id)]
  }
  
  data[, plot_loc_id := tstrsplit(x = path_to_top_parent, ",", keep = l+1)]
  data[, plot_loc_id := as.integer(plot_loc_id)]
  data <- data[!is.na(plot_loc_id)]
  
  for(page_loc in unique(na.omit(data$page_loc_id))){
    
    page_data_est <- data[page_loc_id == page_loc, ]
    
    page_data_est <- merge(page_data_est, locs[, list(plot_loc_name = location_name, plot_loc_id = location_id)], all.x= T, by = "plot_loc_id")
    page_data_est <- merge(page_data_est, locs[, list(page_loc_name = location_name, page_loc_id = location_id)], all.x= T, by = "page_loc_id")
    page_data_est <- merge(page_data_est, locs[, list(parent_loc_name = location_name, parent_loc_id = location_id)], all.x= T, by = "parent_loc_id")
    
    
    if(nrow(page_data_est) == 0){
      gg <- ggplot() + geom_blank() + ggtitle(paste0("No data for ",  unique(locs[location_id == plot_loc, location_name])))
      print(gg)
      next
    }
    
    gg <- ggplot(data=page_data_est, aes(x=year_id, y=mean, color = interaction(ga_estimate_method_tag, clinical_data_type))) + 
      geom_point() + theme_bw() + ggtitle(unique(locs[location_id == page_loc, location_name])) + ylim(0,0.03)
    
    if(l == 0){
      gg <- gg + facet_wrap(~plot_loc_name, ncol = 15)
    } else if(l %in% c(1,2)){
      gg <- gg + facet_wrap(~plot_loc_name, ncol = 15)
    } else {
      gg <- gg + #geom_ribbon(data = page_data_est[type == "estimates" & sex_id == s & age_group_id == a], aes(x = year_id, ymin = gpr_lower, ymax = gpr_upper, group = interaction(comparison, location_id), fill = comparison), alpha = 0.1) + 
        facet_wrap(~plot_loc_name, ncol = 15)
    }
    
    print(gg)
  }
  
}

dev.off()