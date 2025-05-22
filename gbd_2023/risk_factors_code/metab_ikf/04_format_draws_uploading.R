################################################################################
# IKF risk factor
# pull together RR draws for all risk-outcome pairs related to IKF, save results
################################################################################

rm(list=ls())

# Load libraries and packages ---------------------------------------------

library(data.table)
library(tidyverse)
library(ggplot2)
library(matrixStats)
library(plotly)

invisible(sapply(list.files("FILEPATH", full.names = T), source))

# CONFIG ----------------------------------------------------------------
model_dir <- "FILEPATH"
version_cvd <- "ikf_cvd_2024_07_10"
version_gout <- "ikf_gout_2024_07_09"
output_path <- 'FILEPATH'

# description for uploading models to db, and outdir folder will be named according to 
# date and description
description <- 'gbd2023_new_sr_data' #gbd2023


# Create directories ------------------------------------------------------
date <- Sys.Date()
out_dir <- paste0(output_path, date, "/", description)

if (!file.exists(out_dir)) {
  dir.create(out_dir, recursive=T)
  message(paste('Creating output dir', out_dir))
} else {
  message(paste(out_dir ,'already exists'))
}

# Compile RR draws --------------------------------------------------------
# Pulling previous round RR 
rr_old <- df <- get_draws("rei_id", 341, source = "rr", release_id = 9)

# Prepping data: Loading in csv files of draws and merging data
outcome <- "pad"
pred_PAD1 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_albuminuria-cvd_", outcome, ".csv")))
pred_PAD1$parameter <- 'cat4'
pred_PAD1$cause_id <- 502

pred_PAD2 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage3-cvd_", outcome, ".csv")))
pred_PAD2$parameter <- 'cat3'
pred_PAD2$cause_id <- 502

pred_PAD3 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage4-cvd_", outcome, ".csv")))
pred_PAD3$parameter <- 'cat2'
pred_PAD3$cause_id <- 502

pred_PAD4 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage5-cvd_", outcome, ".csv")))
pred_PAD4$parameter <- 'cat1'
pred_PAD4$cause_id <- 502

# Merged dataframe of pred_PAD
pred_PAD <- rbind(pred_PAD1, pred_PAD2, pred_PAD3, pred_PAD4)

outcome <- "ihd"
pred_ihd1 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_albuminuria-cvd_", outcome, ".csv")))
pred_ihd1$parameter <- 'cat4'
pred_ihd1$cause_id <- 493

pred_ihd2 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage3-cvd_", outcome, ".csv")))
pred_ihd2$parameter <- 'cat3'
pred_ihd2$cause_id <- 493

pred_ihd3 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage4-cvd_", outcome, ".csv")))
pred_ihd3$parameter <- 'cat2'
pred_ihd3$cause_id <- 493

pred_ihd4 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage5-cvd_", outcome, ".csv")))
pred_ihd4$parameter <- 'cat1'
pred_ihd4$cause_id <- 493

# Merged df of pred
pred_ihd <- rbind(pred_ihd1, pred_ihd2, pred_ihd3, pred_ihd4)

outcome <- "stroke"
pred_str1 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_albuminuria-cvd_", outcome, ".csv")))
pred_str1$parameter <- 'cat4'
pred_str1$cause_id <- 495

pred_str2 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage3-cvd_", outcome, ".csv")))
pred_str2$parameter <- 'cat3'
pred_str2$cause_id <- 495

pred_str3 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage4-cvd_", outcome, ".csv")))
pred_str3$parameter <- 'cat2'
pred_str3$cause_id <- 495

pred_str4 <- data.table(read.csv(paste0(model_dir, version_cvd, "/rrs/rr_draws_metab_ikf_stage5-cvd_", outcome, ".csv")))
pred_str4$parameter <- 'cat1'
pred_str4$cause_id <- 495

# Merged df
pred_str <- rbind(pred_str1, pred_str2, pred_str3, pred_str4)

# gout
pred_gout <- data.table(read.csv(paste0(model_dir, version_gout, "/metab_ikf-msk_gout/outer_draws.csv")))

#cast draws wide 
pred_gout$parameter <- 'cat3'
pred_gout$cause_id <- 632
pred_gout2 <- pred_gout
pred_gout2$parameter <- 'cat2'
pred_gout3 <- pred_gout
pred_gout3$parameter <- 'cat1'

# Merged df
pred_gout <- rbind(pred_gout, pred_gout2, pred_gout3)

# Bind the 3 df
data <- rbind(pred_PAD, pred_ihd, pred_str, fill=TRUE)

# add Intracerebral hemorrhage as a stroke outcome
pred_str$cause_id <- 496
data <- rbind(data, pred_str, fill=TRUE)

# Adding age group id to data
data$age_mean <- data$age_mean-2.5
data$age_group_id[data$age_mean==25] <- 10 
data$age_group_id[data$age_mean==30] <- 11
data$age_group_id[data$age_mean==35] <- 12 
data$age_group_id[data$age_mean==40] <- 13 
data$age_group_id[data$age_mean==45] <- 14 
data$age_group_id[data$age_mean==50] <- 15 
data$age_group_id[data$age_mean==55] <- 16 
data$age_group_id[data$age_mean==60] <- 17 
data$age_group_id[data$age_mean==65] <- 18 
data$age_group_id[data$age_mean==70] <- 19 
data$age_group_id[data$age_mean==75] <- 20 
data$age_group_id[data$age_mean==80] <- 30 
data$age_group_id[data$age_mean==85] <- 31
data$age_group_id[data$age_mean==90] <- 32 
data$age_group_id[data$age_mean==95] <- 235 

data$draw_0 <- data$draw_1000
data$draw_1000 <- NULL

# Adding additional columns for formatting --------------------------------

# Add age group id
ages <- c(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)

# make all the age-specific rows for gout
# for each age group id we are adding the same gout data
for (a in ages) {
  print(a)
  pred_gout2 <- copy(pred_gout)
  pred_gout2$age_group_id <- a
  data <- rbind(data, pred_gout2, fill=TRUE)
}

data$mortality <- 1
data$morbidity <- 1
data$metric_id <- 3
data$sex_id <- 1

data2 <- data
data2$sex_id <- 2

data <- rbind(data, data2)
data$location_id <- 1
data$rei_id <- 341
data$modelable_entity_id <- 10944

rr <- data

# from previous round, use draws from asymptomatic (cat5) for all outcomes, and albuminuria (cat4) 
# for gout(cause_id 632) (which is all 1's)
rr_old2 <- rr_old[(rr_old$parameter=="cat5" |
                     (rr_old$cause_id == 632 & rr_old$parameter == "cat4")) &
                    rr_old$year_id==1990,]

rr_old2$year_id <- NULL
rr_old2 <- subset(rr_old2, select = -model_version_id)

rr$age_mean <- NULL
rr <- data.frame(rr)
rr[,grep("draw_", names(rr))] <- exp(rr[,grep("draw_", names(rr))])

rr$exposure <- NA

rr <- rbind(rr, rr_old2)
rr_all <- rr
rr_all$year_id <- 1990

for (y in c(1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)) { # estimation years for round
  rr$year_id <- y
  rr_all <- rbind(rr_all, rr)
}
rr <- as.data.table(rr_all)

# set 85+ RR as constant

# create a subset of age 30 values so we can replace 31, 32 and 235 with this subset
rr_age_80_constant <- rr[age_group_id == 30, ]
draw_cols <- grep("draw", names(rr), value = TRUE)

for (col in draw_cols) {
  age_80_values <- rr_age_80_constant[[col]]
  
  rr[age_group_id == 31, (col) := age_80_values]
  rr[age_group_id == 32, (col) := age_80_values]
  rr[age_group_id == 235, (col) := age_80_values]
}



# Check points ----------------------------------------------------------------

# check that there for one cause, sex, year, age_group_id for each parameter 
check <- rr %>% filter(cause_id == 632 & sex_id == 1 & year_id == 1990 & age_group_id == 14)

# check that RRs are constant for age group id 30, 31, 32, 235
check <- rr %>% filter(cause_id == 493 & sex_id == 1 & year_id == 1990 & parameter == 'cat4' & age_group_id %in% c(30, 31, 32, 235))

# check squareness 
table(rr$parameter, rr$cause_id)
for (i in unique(rr$cause_id)) {
  rr2 <- rr[rr$cause_id==i, ]
  print(table(rr2$age_group_id, rr2$parameter))
}

# Save csv that will be uploaded --------------------------------------------
write.csv(rr, paste0(out_dir, "/", "KD_RR_GBD2023_padihdstroke_gout.csv"))


# Upload data ------------------------------------------------------


input_dir <- paste0(out_dir, "/")
risk_type <- 'rr'
release_id <- 16
mark_best <- TRUE
sex_id <- c(1,2)
modelable_entity_id <- 10944
input_file_pattern <- paste0("KD_RR_GBD2023_padihdstroke_gout.csv")

save_results_risk(input_dir=input_dir, input_file_pattern=input_file_pattern,
                  modelable_entity_id=modelable_entity_id,
                  description=description, risk_type=risk_type, sex_id=sex_id,
                  release_id = release_id, mark_best=TRUE)



# Vetting ----------------------------------------------------------------------

# create a scatter plot
old_rr <- as.data.table(get_draws("rei_id", 341, source="rr", release_id=9))
new_rr <- as.data.table(get_draws("rei_id", 341, source="rr", release_id=16))

old_rr[, mean := rowMeans(.SD), .SDcols = paste0("draw_", 0:999)]
new_rr[, mean := rowMeans(.SD), .SDcols = paste0("draw_", 0:999)]

new_rr$lower <- apply(new_rr[, paste0("draw_", 0:999)],1,quantile,probs=c(.025))
old_rr$lower <- apply(old_rr[, paste0("draw_", 0:999)],1,quantile,probs=c(.025))

new_rr$upper <- apply(new_rr[, paste0("draw_", 0:999)],1,quantile,probs=c(.975))
old_rr$upper <- apply(old_rr[, paste0("draw_", 0:999)],1,quantile,probs=c(.975))

old_rr[,paste0("draw_", 0:999)] <- NULL
new_rr[,paste0("draw_", 0:999)] <- NULL

old_rr[, version := "GBD 2021"]
new_rr[, version := "GBD 2023"]

df <- rbind(old_rr, new_rr)
df_plot <- df[year_id == 2020 & sex_id == 1, ] # all years and sexes are the same so just plot one of each

df_plot$age_plot[df_plot$age_group_id==10] <- 25 
df_plot$age_plot[df_plot$age_group_id==11] <- 30
df_plot$age_plot[df_plot$age_group_id==12] <- 35 
df_plot$age_plot[df_plot$age_group_id==13] <- 40 
df_plot$age_plot[df_plot$age_group_id==14] <- 45 
df_plot$age_plot[df_plot$age_group_id==15] <- 50 
df_plot$age_plot[df_plot$age_group_id==16] <- 55 
df_plot$age_plot[df_plot$age_group_id==17] <- 60 
df_plot$age_plot[df_plot$age_group_id==18] <- 65 
df_plot$age_plot[df_plot$age_group_id==19] <- 70 
df_plot$age_plot[df_plot$age_group_id==20] <- 75 
df_plot$age_plot[df_plot$age_group_id==30] <- 80 
df_plot$age_plot[df_plot$age_group_id==31] <- 85
df_plot$age_plot[df_plot$age_group_id==32] <- 90 
df_plot$age_plot[df_plot$age_group_id==235] <- 95 

pdf(paste0(out_dir, "/scatter_gbd2021_gbd_2023.pdf"), width = 12, height = 8.5)
for (x in unique(df_plot$cause_id)) {
  temp <- df_plot[cause_id == x,]
  p <- ggplot(temp, aes(x = age_plot, y= mean, group = version, color = version)) +
    geom_line() +
    facet_grid(cols = vars(parameter)) +
    theme_minimal() +
    labs(title = paste0("KD RR for cause id: ", x),
         x = "Age",
         y = "Mean RR")
  print(p)
}
dev.off()


