# Author: USERNAME
# Date: DATE
# Description: Create a model of standard deviation for risk factors

# ==============================================================================

rm(list=ls())

# Packages
library(data.table)
library(ggplot2)
library(openxlsx)
library(grDevices)
library(lme4)
library(RColorBrewer)
library(splines)

# Shared functions
central <- "/FILEPATH/"
source(paste0(central, "get_bundle_data.R"))
source(paste0(central, "get_age_metadata.R"))
source(paste0(central, "get_location_metadata.R"))

# Parameters
me <- "sbp"
release_id <- 16

if(me=='sbp'){
  bundle_id <- 4787
} else {
  bundle_id <- 4904
}
model_start_age <- 25
date <- gsub('-', '_', Sys.Date())

# Filepaths
old_sd_data_path <- ifelse(me=='sbp', 
                           "/FILEPATH/to_sd_decompiterative_2020_06_19.csv",
                           "/FILEPATH/to_sd_decompiterative_2020_06_19.csv")
output_folder <- paste0("/FILEPATH/", me, "/")

##############################################################################################################
##################################### # Pull in data #########################################################
##############################################################################################################

# pull in SD data that was save in the bundle (started doing this for GBD 2023, when we switched to winnower)
bundle_data <- get_bundle_data(bundle_id = bundle_id)
recent_micro <- bundle_data[data_type==1 & grepl('extraction/tabulation via winnower', note_SR) & me_name == me]
setnames(recent_micro, c('val', 'standard_deviation'), c('mean', 'sd'))

# add in sex_id
recent_micro[sex == "Male", sex_id := 1]
recent_micro[sex == "Female", sex_id := 2]
recent_micro[sex == "Both", sex_id := 3]

# format ages to align with standard GBD age groups
recent_micro[, age_start := age_start_orig]
recent_micro[, age_end := age_end_orig]
recent_micro <- recent_micro[age_start >= model_start_age]
recent_micro[age_start > 95, age_start := 95]  
recent_micro[, age_end := age_end - age_end%%5 + 4]     
recent_micro[, age_start := age_start - age_start%%5]
recent_micro[age_end > 99, age_end := 99]     

# drop age groups that are too large and 'both sexes'
recent_micro <- recent_micro[(age_end-age_start)==4  & sex_id %in% c(1,2)]

# merge on age_group_id
recent_micro[, age_group_id := NULL]
age_meta <- get_age_metadata(release_id = release_id)
age_meta <- age_meta[age_group_years_start >= model_start_age,.(age_group_id, age_group_years_start, age_group_years_end)]
age_meta[, age_group_years_end := age_group_years_end - 1]
age_meta[age_group_years_end > 99, age_group_years_end := 99]
setnames(age_meta, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
recent_micro <- merge(recent_micro, age_meta[,.(age_group_id, age_start, age_end)],
                      by = c('age_start', 'age_end'))

recent_micro <- recent_micro[, .(nid, ihme_loc_id, smaller_site_unit, age_group_id, year_id, sex_id, me_name, mean, sd, sample_size)]
recent_micro[, cv_lit := 0]

# subset to standard locations
recent_micro[ihme_loc_id == "CHN_44533", ihme_loc_id := 'CHN']
stnd_locs <- get_location_metadata(location_set_id = 101, release_id = release_id)
recent_micro <- recent_micro[ihme_loc_id %in% stnd_locs$ihme_loc_id, ]

# merge on loc_metadata
locs <- get_location_metadata(location_set_id=22, release_id = release_id)[, .(location_id, ihme_loc_id, super_region_name, region_name, level)]  
recent_micro <- merge(recent_micro, locs, by="ihme_loc_id")

# drop small sample sizes
message(paste("Dropping", nrow(recent_micro[sample_size<=10]), "rows for sample size <= 10"))
recent_micro <- recent_micro[sample_size>10]

# drop where sd is 0--sample size should take care of this
message(paste("Dropping", nrow(recent_micro[sd==0]), "rows where sd==0"))
recent_micro <- recent_micro[sd!=0]

# pull in SD data that was used in GBD 2021 & earlier
old_sd_data <- fread(old_sd_data_path)
old_sd_data[, V1 := NULL]

# combine data
full <- rbind(recent_micro, old_sd_data)
nrow(full)
table(full$cv_lit)

# create plots
pdf(file=paste0(output_folder, 'sd_data_plots_', date, '.pdf'), height = 7, width = 12)
for(sex in c(1,2)){
  message("Making diagnostic plots for sex_id ", sex)
  full.s<-full[sex_id==sex,]
  for(age in unique(full.s$age_group_id)){
    full.s<-full[age_group_id==age, ]
    correl<-cor(full.s$mean, full.s$sd, method="spearman")
    
    p<-ggplot(data=full.s, aes(x=sd))+
      geom_histogram(aes(fill=super_region_name),color="black")+
      ggtitle(paste0("Distribution of observed SD, sex_id: ", sex, ", age_group_id: ", age))+
      theme_classic()+
      theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
    print(p)
    
    p<-ggplot(data=full, aes(x=mean, y=sd, color=super_region_name))+
      geom_point(alpha=0.2)+
      geom_rug(alpha=0.4)+
      ggtitle(paste0("Sex_id: ", sex, ", age_group_id: ", age))+
      labs(subtitle = paste0("Spearman correlation: ", round(correl, digits=3)))
      theme_classic()+
      theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
    print(p)
  }
}
dev.off()

##############################################################################################################
##################################### # Create model #########################################################
##############################################################################################################

# prep data for modeling
full[, log_mean := log(mean)]
full <- merge(full, age_meta[,.(age_start, age_group_id)])

# model
#form <- "log(sd)~log_mean+factor(age_group_id)+factor(sex_id)"
form <- "log(sd)~log_mean+bs(age_start, knots=c(35, 50, 65))+factor(sex_id)+year_id" 
mod <- lm(formula=as.formula(form), data=full)
summary(mod)

# get predictions
preds <- exp(predict.lm(mod, interval="confidence"))
preds <- cbind(full, preds)
preds[, diff := sd-fit]
is.rmse <- sqrt(mean(preds$diff^2))

# plot predictions vs data
expand_spectral <- colorRampPalette(brewer.pal(11, "Spectral"))
pdf(file=paste0(output_folder, 'sd_model_fit_', date, '.pdf'), height = 7, width = 12)
p <- ggplot(data=preds, aes(x=sd, y=fit, color=as.factor(age_start))) + 
  geom_point() + geom_abline(slope=1, intercept=0) +
  facet_wrap(~factor(sex_id, levels=c(1,2), labels=c('Male', 'Female'))) +
  labs(color = 'Age start', x = 'Observed SD', y = 'Modeled SD', title = paste0('In-sample RMSE: ', round(is.rmse, 2))) +
  theme_bw() + scale_color_manual(values = expand_spectral(length(unique(preds$age_start))))
print(p)
dev.off()

# plot model on dummy data
new_data <- data.table()
for(yr in seq(1990, 2020, by=10)){
  temp <- unique(full[,.(age_group_id, age_start, sex_id)])
  temp[, year_id := yr]
  new_data <- rbind(new_data, temp)
}
new_data[, mean := mean(full$mean)]
new_data[, log_mean := log(mean)]
preds <- exp(predict.lm(mod, interval="confidence", newdata = new_data))
preds <- cbind(new_data, preds)
preds[, sex := ifelse(sex_id==1, "Male", "Female")]

pdf(file=paste0(output_folder, 'sd_model_', date, '.pdf'), height = 7, width = 12)
p<-ggplot(data=preds, aes(x=age_start, y=fit, color=as.factor(year_id), fill=as.factor(year_id)))+
  geom_line(linewidth=1.5)+
  geom_ribbon(aes(ymin=lwr, ymax=upr, color=NULL), alpha=0.5)+
  facet_wrap(~sex)+
  scale_color_brewer(palette="Set1")+
  scale_fill_brewer(palette="Set1")+
  xlab("Age")+
  ylab("Estimated SD")+
  ggtitle(paste0("SD estimated for a mean ", me, " of ", round(mean(full$mean), 2)))+
  labs(color='year_id', fill='year_id')+
  theme_bw()+
  theme(text = element_text(size=17))
print(p)
dev.off()

##############################################################################################################
##################################### # Save model #########################################################
##############################################################################################################

saveRDS(mod, file=paste0(output_folder, me, "_sd_mod_release_", release_id, "_", date, ".rds"))
