# Sarah Wulf Hanson
# 28 September 2020
# GBD
# IKF risk factor
# create RR draws for all risk-outcome pairs related to IKF

rm(list=ls())

library(data.table)
library(ggplot2)
library(matrixStats)
library(plotly)
source("/home/j/temp/reed/prog/projects/run_mr_brt/mr_brt_functions.R")
source("/ihme/cc_resources/libraries/current/r/get_draws.R")
source("/ihme/cc_resources/libraries/current/r/get_age_metadata.R")



model_dir <- "/ihme/scratch/users/qwr/ylds/ckd/escore/with_gbd_age_bins/"
version_ihd_str <- "with_gbd_age_bins"
# 5 ihd and stroke are modeled the same way 
# version 4 uses 10% trim
# version 5 uses linear left tail
version_gout <- "with_gbd_age_bins"
# version 1 has 10% trim, version 2 has no trim.  use 10% trim.


# Compile RR draws
# 493 ihd
# 495 ischemic stroke
# 496 Intracerebral hemorrhage
# 502 Peripheral artery disease
# 632 gout 
# cat1 = ME 10734 stage5
# cat2 = ME 10733 stage4
# cat3 = ME 10732 stage3
# cat4 = ME 10509 albuminuria
# cat5 = asymptomatic

outcome <- "chdh"
pred_ihd1 <- data.table(read.csv(paste0(model_dir, "rr_draws_albuminuria_", outcome, ".csv")))
pred_ihd1$parameter <- 'cat4'
pred_ihd1$cause_id <- 493
pred_ihd2 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage3_", outcome, ".csv")))
pred_ihd2$parameter <- 'cat3'
pred_ihd2$cause_id <- 493
pred_ihd3 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage4_", outcome, ".csv")))
pred_ihd3$parameter <- 'cat2'
pred_ihd3$cause_id <- 493
pred_ihd4 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage5_", outcome, ".csv")))
pred_ihd4$parameter <- 'cat1'
pred_ihd4$cause_id <- 493
pred_ihd <- rbind(pred_ihd1, pred_ihd2, pred_ihd3, pred_ihd4)
outcome <- "str"
pred_str1 <- data.table(read.csv(paste0(model_dir, "rr_draws_albuminuria_", outcome, ".csv")))
pred_str1$parameter <- 'cat4'
pred_str1$cause_id <- 495
pred_str2 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage3_", outcome, ".csv")))
pred_str2$parameter <- 'cat3'
pred_str2$cause_id <- 495
pred_str3 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage4_", outcome, ".csv")))
pred_str3$parameter <- 'cat2'
pred_str3$cause_id <- 495
pred_str4 <- data.table(read.csv(paste0(model_dir, "rr_draws_stage5_", outcome, ".csv")))
pred_str4$parameter <- 'cat1'
pred_str4$cause_id <- 495
pred_str <- rbind(pred_str1, pred_str2, pred_str3, pred_str4)
exp <- "ckd3_5"
out <- "gout"
pred_gout <- data.table(read.csv(paste0(model_dir, "rr_draws_", exp, "_", out, ".csv")))
pred_gout$parameter <- 'cat3'
pred_gout$cause_id <- 632
pred_gout2 <- pred_gout
pred_gout2$parameter <- 'cat2'
pred_gout3 <- pred_gout
pred_gout3$parameter <- 'cat1'
pred_gout <- rbind(pred_gout, pred_gout2, pred_gout3)

names(pred_ihd) <- sub("V", "draw_", names(pred_ihd))
names(pred_str) <- sub("V", "draw_", names(pred_str))
names(pred_gout) <- sub("V", "draw_", names(pred_gout))


ages <- get_age_metadata(19)
ages <- ages$age_group_id

data <- rbind(pred_ihd, pred_str, fill=TRUE)
# add Intracerebral hemorrhage as a stroke outcome
pred_str$cause_id <- 496
data <- rbind(data, pred_str, fill=TRUE)
# what to do about PAD???
# pred_ihd$cause_id <- 502
# data <- rbind(data, pred_ihd)


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

# make all the age-specific rows for gout
for(a in ages) {
  #print(a)
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

rr <- data[age_group_id %in% c(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)]



########################################
# NEEDS MORE FORMATTING BEFORE UPLOAD
# Format for upload using GBD 2019's RR as template:
# cat1 = ME 10734 stage5
# cat2 = ME 10733 stage4
# cat3 = ME 10732 stage3
# cat4 = ME 10509 albuminuria
# cat5 = asymptomatic
########################################
source("/ihme/cc_resources/libraries/current/r/get_draws.R")
gbd2017 <- get_draws("rei_id", 341, source="rr", gbd_round_id=5)
# write.csv(gbd2017, "/ihme/epi/ckd/ckd_code/kd/gbd_round_compare/KD_RR_GBD2017.csv")

gbd2019 <- get_draws("rei_id", 341, source="rr", decomp_step="step4", gbd_round_id=6)
# write.csv(gbd2019, "/ihme/epi/ckd/ckd_code/kd/gbd_round_compare/KD_RR_GBD2019.csv")

#from GBD 2019, use draws from asymptomatic, PAD, and albuminuria for gout (which is all 1's)
gbd2019 <- gbd2019[(gbd2019$parameter=="cat5" | 
                      gbd2019$cause_id == 502 | # when new data for PAD comes in, remove this line 
                      (gbd2019$cause_id == 632 & gbd2019$parameter == "cat4")) & 
                     gbd2019$year_id==1990,]

pad_2017 <- gbd2017[gbd2017$cause_id == 502]
pad_only <- gbd2019[gbd2019$cause_id == 502]

gbd2019$year_id <- NULL

rr$X <- NULL
rr$age_mean <- NULL
rr <- data.frame(rr)

rr[,grep("draw_", names(rr))] <- exp(rr[,grep("draw_", names(rr))])


library(general.utilities, lib.loc = "/ihme/code/ylds/general/packages/development")
# rehspaing to understand
gbd2019_long <- reshape_draws(gbd2019, 
                           draw_name = "draw",
                           direction = "long",
                           draw_column_name = "draw",
                           draw_value_name = "values")

# easier to view summary statistics about the draw outputs
summary(gbd2019_long$values)

ggplot(gbd2019_long, aes(x=values)) + 
  geom_density() + theme_bw()

gbd2019_long %>%
  group_by(location_id, age_group_id, sex_id, year_id) %>%
  summarise(kd_mean = mean(values),
            lower = quantile(values, .025),
            upper = quantile(values, .975))



# bind on rows for asymptomatic parameter (cat5) and cause_id 502 (PAD)
rr$exposure <- NA
rr$draw_0 <- rr$draw_1000
rr$draw_1000 <- NULL
setdiff(names(rr), names(gbd2019))
setdiff(names(gbd2019), names(rr))

rr <- rbind(rr, gbd2019)
rr_all <- rr
rr_all$year_id <- 1990
for (y in c(1991:2022)) {
  rr$year_id <- y
  rr_all <- rbind(rr_all, rr)
}
rr <- rr_all


table(rr$parameter, rr$cause_id)
for(i in unique(rr$cause_id)) {
  rr2 <- rr[cause_id==i]
  print(table(rr2$age_group_id, rr2$parameter))
}

# check that there for one cause, sex, year, age_group_id fore each parameter 
check <- rr %>% filter(cause_id == 493 & sex_id == 1 & year_id == 1992 & age_group_id == 14)

#  write.csv(rr, file = paste0("/ihme/epi/ckd/ckd_code/kd/rel_risk_draws/gbd2020/output/KD_RR_GBD2020_", date, ".csv"))
write.csv(rr, file = paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/", version_ihd_str, "/KD_RR_GBD2020_ihdstroke_gout.csv"))


# Save KD RR  
source("/ihme/cc_resources/libraries/current/r/save_results_risk.R")

input_dir <- paste0("/ihme/scratch/users/qwr/ylds/ckd/escore/", version_ihd_str,"/")
description <- 'draws_no_fisher_all_age_bins'
risk_type <- 'rr'
decomp_step <- 'iterative'
mark_best <- TRUE
sex_id <- c(1,2)
modelable_entity_id <- 10944
input_file_pattern <- paste0("KD_RR_GBD2020_ihdstroke_gout.csv")
save_results_risk(input_dir=input_dir, input_file_pattern=input_file_pattern, modelable_entity_id=modelable_entity_id,
                  description=description, risk_type=risk_type, sex_id=sex_id, decomp_step=decomp_step, mark_best=FALSE)
# 632021 = RR no fisher, age bins
# 578873 = RR for all years, no left linear tail


## Uploader requirements:

#  The following columns are required: cause_id, age_group_id, year_id, location_id, sex_id, mortality, 
#   morbidity, parameter. If any of these columns/values are part of the file name (as specified by the 
#   input_file_pattern argument), then they can be omitted from the file contents (this is true for all 
#   save_results functions).

#  Draw columns can either be prefixed with ‘rr_’ or ‘draw_’.

#  Since RRs may not vary by location, there are no location requirements. One global location is fine. 
#   If RRs vary by region or some other location aggregate, please make sure the provided locations cover 
#   all most detailed locations. For example, if your RRs are different for Asia but constant for all 
#   other locations, please provide at least Asia and Global.

#  Sexes are required to be 1, 2 or 1 and 2. The sex_id argument defaults to [1,2] but you can change that 
#   if you’re uploading a sex specific model.

#  Years default to estimation years (ie get_demographics with team “epi”), but you can change the year_id 
#   argument if you want to upload more.

#  Age groups must be from the set returned from get_demographics (ie most detailed ages). But not every 
#   age must be included if your model is age restricted.

#  Morbidity and mortality must be binary (0/1).

#  Parameter values must either be the string “per unit” or a string like “cat1” or “cat2” (aka matches the 
#   regex “catd+”).

#  No nulls are allowed.

#  No duplicates are allowed.



# How did relative risks change overall? 

# create a scatter plot

older_rr <- get_draws("rei_id", 341, source="rr", gbd_round_id=5)
old_rr <- get_draws("rei_id", 341, source="rr", decomp_step="step4", gbd_round_id=6)
new_rr <- get_draws("rei_id", 341, source="rr", decomp_step="iterative", gbd_round_id=7, version_id = 632021)
# version_id = 575330 for ihd/str version 4 (no linear left tail)
# version_id = 578861 for ihd/str version 5 (linear left tail)
backup <- data.frame(new_rr)
backup2 <- data.frame(old_rr)
backup3 <- data.frame(older_rr)
new_rr <- backup
old_rr <- backup2
older_rr <- backup3
# new_rr <- rr


plot_risks <- function(new, old, para, causeid) {
  
  df <- new[parameter == para & cause_id == causeid,]
  df2 <- old[parameter == para & cause_id == causeid,]
  
  rr_mean <- rowMeans(df[, 12:ncol(df)])
  
  df <- cbind(df[,c("parameter", "cause_id", "age_group_id", "mortality", "morbidity", 
                    "metric_id", "sex_id", "location_id", "rei_id", "modelable_entity_id",
                    "year_id")], rr_mean)
  
  rr_mean <- rowMeans(df2[, grep("draw_", names(df2))])
  
  df2 <- cbind(df2[,c("parameter", "cause_id", "age_group_id", "mortality", "morbidity", 
                      "metric_id", "sex_id", "location_id", "rei_id", "modelable_entity_id",
                      "year_id")], rr_mean)
  
  return(list(df, df2))
}


older_rr$older_rr_mean <- rowMeans(older_rr[, grep("draw_", names(older_rr))])
old_rr$old_rr_mean <- rowMeans(old_rr[, grep("draw_", names(old_rr))])
new_rr$new_rr_mean <- rowMeans(new_rr[, grep("draw_", names(new_rr))])

new_rr$lower <- apply(new_rr[, grep("draw_", names(new_rr))],1,quantile,probs=c(.025))
old_rr$lower <- apply(old_rr[, grep("draw_", names(old_rr))],1,quantile,probs=c(.025))
older_rr$lower <- apply(older_rr[, grep("draw_", names(older_rr))],1,quantile,probs=c(.025))
new_rr$upper <- apply(new_rr[, grep("draw_", names(new_rr))],1,quantile,probs=c(.025))
old_rr$upper <- apply(old_rr[, grep("draw_", names(old_rr))],1,quantile,probs=c(.025))
older_rr$upper <- apply(older_rr[, grep("draw_", names(older_rr))],1,quantile,probs=c(.025))

older_rr[,grep("draw_", names(older_rr))] <- NULL
old_rr[,grep("draw_", names(old_rr))] <- NULL
new_rr[,grep("draw_", names(new_rr))] <- NULL

df <- merge(new_rr, old_rr, by=c("parameter", "cause_id", "age_group_id", "mortality", "morbidity", 
                                 "metric_id", "sex_id", "location_id", "rei_id", "modelable_entity_id", "year_id"))
df <- merge(df, older_rr, by=c("parameter", "cause_id", "age_group_id", "mortality", "morbidity", 
                               "metric_id", "sex_id", "location_id", "rei_id", "modelable_entity_id", "year_id"))


ggplot() +
  geom_density(data = df, aes(x=old_rr_mean), color="darkblue", fill="lightblue", alpha=.5) +
  geom_density(data = df, aes(x=new_rr_mean), color="red", fill="grey", alpha=.5) +
  scale_color_discrete(name = "Legend", labels = c("2017 RR", "Current RR w Optum")) +
  lims(x = c(0,25)) +
  labs(x="Relative Risk Draw Means", title = "All Relative Risks") +
  theme_bw()


ggplot() +
  geom_density(data = df, aes(x= old_rr_mean), color="darkblue", fill="lightblue", alpha=.5) +
  geom_density(data = df, aes(x = new_rr_mean), color="red", fill="grey", alpha=.5) +
  lims(x = c(-1,8)) +
  labs(x="Relative Risk Draw Means", title = "Relative Risks (cat 1:Stage 5, cat 5: none)", y="Distribution of Draws") +
  theme_bw() +
  facet_grid(parameter~.)


#p
#Normal ggplot



#### Checking the PAF's

#source("/ihme/cc_resources/libraries/current/r/get_draws.R")
#data <- get_draws("rei_id", 341, source="paf", decomp_step="step4", gbd_round_id=6, year_id = 2017, age_group_id = 22,
#                  version_id = 471800, measure_id = 1, metric_id = 2)

#source("/ihme/code/risk/diagnostics/paf_scatter.R")

#paf_scatter(rei_id = 341, year_id = 2017, loc_level = 4, version_id = 463505, gbd_round_id = 6, measure_id = 3,
#            file_path = "/ihme/epi/ckd/ikf/incorrect_pafs.pdf")
#415229 # step 2
#463505 # nonexp
#471800 # updated


#### plot relative risk comparisons



convert_age_groups <- function(df) {
  
  df$age[df$age_group_id == 5] <- 4
  df$age[df$age_group_id == 6] <- 9
  df$age[df$age_group_id == 7] <- 14
  df$age[df$age_group_id == 8] <- 19
  df$age[df$age_group_id == 9] <- 24
  df$age[df$age_group_id == 10] <- 29
  df$age[df$age_group_id == 11] <- 34
  df$age[df$age_group_id == 12] <- 39
  df$age[df$age_group_id == 13] <- 44
  df$age[df$age_group_id == 14] <- 49
  df$age[df$age_group_id == 15] <- 54
  df$age[df$age_group_id == 16] <- 59
  df$age[df$age_group_id == 17] <- 64
  df$age[df$age_group_id == 18] <- 69
  df$age[df$age_group_id == 19] <- 74
  df$age[df$age_group_id == 20] <- 79
  df$age[df$age_group_id == 30] <- 84
  df$age[df$age_group_id == 31] <- 89
  df$age[df$age_group_id == 32] <- 94
  df$age[df$age_group_id == 235] <- 95
  
  return(df)
  
  
}

new_rr$round <- "GBD2020"
old_rr$round <- "GBD2019"
older_rr$round <- "GBD2017"

new_rr$value <- new_rr$new_rr_mean
old_rr$value <- old_rr$old_rr_mean
older_rr$value <- older_rr$older_rr_mean
new_rr$new_rr_mean <- NULL
old_rr$old_rr_mean <- NULL
older_rr$older_rr_mean <- NULL
data = rbind(new_rr, old_rr, older_rr)
data = convert_age_groups(data)

data <- data[data$cause_id == 493 | data$cause_id == 495,]


data$parameter[data$parameter == 'cat1'] <- "Stage5"
data$parameter[data$parameter == 'cat2'] <- "Stage4"
data$parameter[data$parameter == 'cat3'] <- "Stage3"
data$parameter[data$parameter == 'cat4'] <- "Albuminuria"
data$parameter[data$parameter == 'cat5'] <- "Asymptomatic"

data$parameter <- as.factor(data$parameter)
data$parameter <- factor(data$parameter, levels = c("Stage5", "Stage4", "Stage3", "Albuminuria", "Asymptomatic"))

levels(data$parameter)



p1 <- ggplot(data=data, aes(x = age, y = value, colour = round)) +
  geom_line(size = 1.2) +
  geom_ribbon(data=data, aes(x = age, ymin = lower, ymax = upper, fill = round), linetype=0, alpha = 0.15) +
  facet_grid(cause_id ~ parameter) +
  theme_bw() +
  labs(x = "Age", y = "Relative Risk", title = "IKF RR Comparison (493 = IHD, 495 = Stroke)")

ggplotly(p1)
p1 
#normal ggplot

p2<-ggplot(data=data, aes(x = age, y = log(value), colour = round)) +
  geom_line(size = 1.2) +
  geom_ribbon(data=data, aes(x = age, ymin = log(lower), ymax = log(upper), fill = round), linetype=0, alpha = 0.15) +
  facet_grid(cause_id ~ parameter) +
  theme_bw() +
  labs(x = "Age", y = "Relative Risk (LOG)", title = "LOG IKF RR Comparison (493 = IHD, 495 = Stroke)")

ggplotly(p2)
p2

data %>% filter(parameter %in% c("Albuminuria", "Stage3"))


### Checking something on March 1st, 2020
data <- get_draws("rei_id", 341, source="rr", decomp_step="step4", 
                  gbd_round_id=6, status = "best")


data <- convert_draws_long(data)
data$measure_id <- NA
glimpse(data)


a <- data %>% group_by(parameter, age_group_id, cause_id) %>%
  summarize(mean = mean(values))
a

