
rm(list=ls())


Sys.umask(mode = 002)

os <- .Platform$OS.type


library(data.table)
library(magrittr)
library(ggplot2)
library(msm)
library(readxl)
library(openxlsx)
library(crosswalk, lib.loc = "FILEPATH")

source("FILEPATH/get_location_metadata.R"  )
source("FILEPATH/merge_on_location_metadata.R")


locs <- get_location_metadata(22)[,.(location_id,path_to_top_parent,level,location_name)]


# source all central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

ids <- fread("FILEPATH")

ga28 <- fread("FILEPATH")
ga28[is.na(cv_ga_37), orig_bundle := "ga_28"]
ga28[!is.na(cv_ga_37), orig_bundle := "ga_37"]

ga37 <- fread("FILEPATH")
ga37[is.na(cv_ga_28), orig_bundle := "ga_37"]
ga37[!is.na(cv_ga_28), orig_bundle := "ga_28"]

# ----- Crosswalk GA28 to GA37





data <- copy(ga37)

# Claims missing cv-lbw
data[is.na(cv_lbw), cv_lbw := 0 ]

noxwalk_data <- data[mean == 0 | mean == 1]
data <- data[!(mean == 0 | mean == 1 )]

# Create a temporary index
data[, temp_index := .I]

data[, mid_year := ( year_start + year_end ) / 2]
data[, mid_year_cat := cut(mid_year, breaks = seq(1990, 2025, 1))]

# ----- make matches from both bundles

matches <- merge(ga37[orig_bundle == "ga_37" & clinical_data_type == "non_clinical" & cv_lbw == 0 & !(mean == 0 | mean == 1 ), .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean_ga37 = mean, se_ga37 = standard_error, nid.denom = nid)],
                 ga28[orig_bundle == "ga_28" & clinical_data_type == "non_clinical" & !(mean == 0 | mean == 1 ), .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean_ga28 = mean, se_ga28 = standard_error, nid.num = nid)], allow.cartesian = T)


# Keep matches within a certain bounds

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = matches$mean_ga28,
    sd = matches$se_ga28,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = matches$mean_ga37,
    sd = matches$se_ga37,
    transformation = "linear_to_logit")
))

names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

matches[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

matches[, dorm_alt := "ga_28"]
matches[, dorm_ref := "ga_37"]

matches <- merge_on_location_metadata(matches)

pdf("FILEPATH", width = 15, height = 12)

ggplot(matches[]) +
  geom_point(aes(x = mean_ga28, y = mean_ga37), color = "blue", alpha = 0.2) +
  geom_abline(slope = 1, intercept = 0) +
  facet_wrap(~ihme_loc_id) + theme_bw() + xlim(0,0.5) + ylim(0,0.5) +
  ggtitle("ga_37 GA28-GA37 Matches")

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




matches_CWModel <- py_load_object(filename = paste0("FILEPATH"), pickle = "dill")


data[, c("mean_adj_ga28ga37sq", "se_adj_ga28ga37sq", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = matches_CWModel,       # result of CWModel()
  df = data,            # original data with obs to be adjusted
  orig_dorms = "orig_bundle",  # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

data[, mean := mean_adj_ga28ga37sq]
data[, standard_error := se_adj_ga28ga37sq]

noxwalk_data[, mean_adj_ga28ga37sq := mean]
noxwalk_data[, se_adj_ga28ga37sq := standard_error]

data <- rbind(data, noxwalk_data, use.names = T, fill = T)

data <- data[, -c("diff", "diff_se", "data_id")]

write.csv(data, "FILEPATH", row.names = F, na = "")

repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = matches_CWModel, 
  cwdata = matches_CWData,
  continuous_variables = list("sex_id"),
  obs_method = 'ga_28',
  plot_note = "ga28ga37sq", 
  plots_dir = "FILEPATH", 
  file_name = "ga28ga37sq_funnel",
  write_file = TRUE
)






pdf("FILEPATH", width =18, height = 12)

betas <- data.table(matches_CWModel$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(matches_CWModel$gamma, 5))

ylimend = 0.5


plot_data <- data#[is.na(cv_ga_28)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

plot_data[, cv_lbw := as.factor(cv_lbw)]

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_lbwga37, color = as.factor(orig_bundle), shape = cv_lbw), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle), shape = cv_lbw), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[cv_ga_28 == 1, ihme_loc_id], 1,3))){
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_lbwga37, color = as.factor(orig_bundle), shape = cv_lbw)) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle), shape = cv_lbw)) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()


# ----- Crosswalk 37 ubti 27




data <- copy(ga28)

noxwalk_data <- data[mean == 0 | mean == 1]
data <- data[!(mean == 0 | mean == 1 )]

# Create a temporary index
data[, temp_index := .I]

data[, mid_year := ( year_start + year_end ) / 2]
data[, mid_year_cat := cut(mid_year, breaks = seq(1990, 2025, 1))]

# ----- make matches from both bundles

matches <- merge(ga28[orig_bundle == "ga_28" & clinical_data_type == "non_clinical" & !(mean == 0 | mean == 1 ), .(location_id, sex, sex_id, mid_year_cat, index.denom = temp_index, mean_ga28 = mean, se_ga28 = standard_error, nid.denom = nid)],
                 ga37[orig_bundle == "ga_37" & clinical_data_type == "non_clinical" & cv_lbw == 0 & !(mean == 0 | mean == 1 ), .(location_id, sex, sex_id, mid_year_cat, index.num = temp_index, mean_ga37 = mean, se_ga37 = standard_error, nid.num = nid)], allow.cartesian = T)


# Keep matches within a certain bounds

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = matches$mean_ga37,
    sd = matches$se_ga37,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = matches$mean_ga28,
    sd = matches$se_ga28,
    transformation = "linear_to_logit")
))

names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

matches[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

matches[, dorm_alt := "ga_37"]
matches[, dorm_ref := "ga_28"]

matches <- merge_on_location_metadata(matches)

pdf("FILEPATH", width = 15, height = 12)

ggplot(matches[]) +
  geom_point(aes(x = mean_ga37, y = mean_ga28), color = "blue", alpha = 0.2) +
  geom_abline(slope = 1, intercept = 0) +
  facet_wrap(~ihme_loc_id) + theme_bw() + xlim(0,0.5) + ylim(0,0.5) +
  ggtitle("ga_28 GA37-GA28 Matches")

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


matches_CWModel <- py_load_object(filename = paste0("FILEPATH"), pickle = "dill")


data[, c("mean_adj_ga28ga37sq", "se_adj_ga28ga37sq", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = matches_CWModel,       # result of CWModel()
  df = data,            # original data with obs to be adjusted
  orig_dorms = "orig_bundle",  # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

data[, mean := mean_adj_ga28ga37sq]
data[, standard_error := se_adj_ga28ga37sq]

noxwalk_data[, mean_adj_ga28ga37sq := mean]
noxwalk_data[, se_adj_ga28ga37sq := standard_error]

data <- rbind(data, noxwalk_data, use.names = T, fill = T)

data <- data[, -c("diff", "diff_se", "data_id")]

write.csv(data, "FILEPATH", row.names = F, na = "")

repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = matches_CWModel, 
  cwdata = matches_CWData,
  continuous_variables = list("sex_id"),
  obs_method = 'ga_37',
  plot_note = "ga28ga37sq", 
  plots_dir = "FILEPATH", 
  file_name = "ga28ga37sq_funnel",
  write_file = TRUE
)




pdf("FILEPATH", width =18, height = 12)

betas <- data.table(matches_CWModel$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(matches_CWModel$gamma, 5))

ylimend = 0.5


plot_data <- data#[is.na(cv_ga_28)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

plot_data[, cv_lbw := as.factor(cv_lbw)]

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_claims, color = as.factor(orig_bundle)), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle)), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[cv_ga_37 == 1, ihme_loc_id], 1,3))){
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_claims, color = as.factor(orig_bundle))) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle))) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()





pdf("FILEPATH", width =18, height = 12)

betas <- data.table(matches_CWModel$create_result_df())

subtitle = lapply(1:nrow(betas), function(i){
  beta_row = paste(betas[i,dorms], betas[i, cov_names], betas[i, round(beta, 5)], betas[i, round(beta_sd, 5)], "\n")
  return(beta_row)
}) %>% unlist() %>% paste0(collapse = "")

subtitle <- paste(subtitle, "gamma:", round(matches_CWModel$gamma, 5))

ylimend = 0.5


plot_data <- data#[is.na(cv_ga_28)]
plot_data <- plot_data[, -c("parent_name.x", "parent_name.y")]

plot_data <- merge_on_location_metadata(plot_data)

plot_data[, cv_lbw := as.factor(cv_lbw)]

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean_adj_claims, color = as.factor(orig_bundle)), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,ylimend) + ylab("Unadjusted") + 
  theme_bw() + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

ggplot(plot_data) + 
  geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle)), alpha = 0.2) + 
  facet_grid(~sex) + ylim(0,0.02) + ylab("Adjusted") + 
  theme_bw()  + ggtitle("GA28-GA37 Xwalk", subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))

for(ihme_loc in unique(substr(plot_data[cv_ga_37 == 1, ihme_loc_id], 1,3))){
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean_adj_claims, color = as.factor(orig_bundle))) + 
    facet_grid(sex~location_name) + ylim(0,ylimend) + ylab("Unadjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
  
  gg <- ggplot(plot_data[ihme_loc_id %like% ihme_loc]) + 
    geom_point(aes(x = year_id, y = mean, color = as.factor(orig_bundle))) + 
    facet_grid(sex~location_name) + ylim(0,0.02) + ylab("Adjusted") + 
    theme_bw()  + ggtitle(paste0("GA28-GA37 Xwalk - ", ihme_loc), subtitle = subtitle)#+ theme(axis.text.x=element_blank()) #+ theme(scale_size_manual(values = c(0.0005,0.001, 0.002,0.006)))
  
  print(gg)
  
}


dev.off()


