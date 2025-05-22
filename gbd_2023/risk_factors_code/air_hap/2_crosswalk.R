# Purpose: crosswalk HAP data before STGPR

#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

# Libraries ###########################################################################


library(data.table)
library(magrittr)
library(msm)
library(ggplot2)
library(openxlsx)
library(readr)

# Crosswalk package
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
# functions
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")

"%ni%" <- Negate("%in%")

# values ######################################################
release<-16  # GBD2023 release id
loc_set<-35  # location set id: Model results
fuel_type <- c("solid","coal","crop","dung","wood")

# bundle ids
hap_bundle_id <- 4736
coal_bundle_id <- 6167
crop_bundle_id <- 6173
dung_bundle_id <- 6176
wood_bundle_id <- 6170

# bundle versions
hap_v <- 47032
coal_v <- 47033
crop_v <- 47035
dung_v <- 47036
wood_v <- 47034

bundle_ids <- c(hap_bundle_id, coal_bundle_id, crop_bundle_id, dung_bundle_id, wood_bundle_id)
bundle_versions <- c(hap_v, coal_v, crop_v, dung_v, wood_v)

# directories
home_dir <- "FILEPATH"
xwalk_dir <- file.path(home_dir, "FILEPATH")
vet_dir <- "FILEPATH"

descrip <- "Duplicates removed, loc id updates, plus new data"

# locations
locs <- get_location_metadata(location_set_id=loc_set, release_id = release)

#------------------Save bundle versions------------------------------------

# # MR-BRT crosswalk #########################################################################
## edits ###############################
# read in all of the bundle data (for all fuel types) into 1 table
out <- data.table()
for (n in 1:length(bundle_versions)){
  temp <- as.data.table(get_bundle_version(bundle_versions[n], fetch="all"))
  temp <- temp[, fuel_type:=fuel_type[n]]
  out <- rbind(out, temp, use.names=T, fill=T)
}

# add in survey method column
out[, survey_method:=ifelse(cv_hh==1, "household", "individual")]

# save for future ref
write_excel_csv(out, paste0(xwalk_dir, "/combined_fuel_types.csv"))

# start here if not saving bundle versions #################################################
out <- fread(paste0(xwalk_dir, "/combined_fuel_types.csv"))

# only use data that is not outliered and only the solid fuel since this is the only fuel we use for crosswalking
data_metareg <- out[is_outlier==0 & fuel_type=="solid"]

# create a column that we will be matching on, for this we will match on year and country
data_metareg[, key:=paste0(year_id, "_", substr(ihme_loc_id,1,3))]

## Deal with 0s and 1s #############################################
# For this we decided to take half of the lowest non-zero value by region

# merge on region data
data_metareg <- merge(data_metareg, locs[, .(location_id,region_id)], by="location_id", all.x=T)

# find half the lowest non-zero value by region
regions <- unique(data_metareg$region_id)

lowest_val <- data.table()

for (x in regions) {
  # x<-5
  print(x)
  subset <- data_metareg[region_id==x & val!=0,]
  half_val <- (min(subset$val)/2)
  table <- data.table(region_id=x, zero_replace=half_val)
  lowest_val <- rbind(lowest_val, table)
}

# find half of the diff between the highest non-1 value-  do this on the global scale
half_max_val <- ((1-max(data_metareg[val<1,val]))/2)
one_replace <- (max(data_metareg[val<1,val])+half_max_val)

#N ow replace the 0s with their half lowest values
# merge the lowest value table with the original table
data_metareg <- merge(data_metareg, lowest_val, by="region_id", all.x=T)

# now replace the zeros
data_metareg[val==0, val:=zero_replace]

# now replace the 1s with a very high value
data_metareg[val==1, val:=one_replace]

# now remove the zero_replace column
data_metareg <- data_metareg[, -c("zero_replace")]

## Logit transformation ###########################
# transform means and SDs to logit space
# first have to make the tables into dataframes to work with cw function
ref_logit <- as.data.frame(copy(data_metareg)[survey_method=="individual"])
alt_logit <- as.data.frame(copy(data_metareg)[survey_method=="household"])

# linear to logit transformation- and convert back to data.table
ref_logit[, c("mean_logit","se_logit")] <- as.data.table(cw$utils$linear_to_logit(mean = array(ref_logit$val), sd = array(ref_logit$standard_error)))
alt_logit[, c("mean_logit","se_logit")] <- as.data.table(cw$utils$linear_to_logit(mean = array(alt_logit$val), sd = array(alt_logit$standard_error)))

# ref and alt to all of the column names
colnames(ref_logit) <- paste(colnames(ref_logit), "ref", sep="_")
colnames(alt_logit) <- paste(colnames(alt_logit), "alt", sep="_")

# Now make the matched pairs
data_combo <- as.data.table(merge(ref_logit, alt_logit, by.x="key_ref", by.y="key_alt"), all.y=T)

# add in the alt and ref definitions
data_combo[, refvar:="individual"]
data_combo[, altvar:="household"]

# create a match id to be used at the study id in the crosswalk
data_combo[, match_id:=1:.N]

# calculating the difference based on example code from Reed's CW training
data_combo[, ':='(logit_diff=mean_logit_alt-mean_logit_ref,
                   logit_diff_se=sqrt(se_logit_alt^2 + se_logit_ref^2))]


## Crosswalk ################################
# Previously they used MRBRT, but now we are using crosswalk

# set up the data
cw_dataset <- cw$CWData(
  df = data_combo,
  obs = "logit_diff",
  obs_se = "logit_diff_se",
  alt_dorms = "altvar",
  ref_dorms = "refvar",
  study_id = "match_id",
  add_intercept = T
)

# create the model
cw_model<-cw$CWModel(
  cwdata = cw_dataset,
  obs_type = "diff_logit",
  cov_models = list(cw$CovModel(cov_name="intercept")),
  gold_dorm = "individual"
)

# fit the model
cw_model$fit()

# Save the coefficient of the crosswalk to be used in the appendix write up
cw_coef <- cw_model$create_result_df()
write_excel_csv(cw_coef, paste0(xwalk_dir, "/cw_coefficients.csv"))

# Now adjust your values based on the crosswalk
cw_preds <- cw_model$adjust_orig_vals(
  df = data_metareg,  # the original dataset
  orig_dorms = "survey_method",  # the original definition
  orig_vals_mean = "val",  # this is the original vals column, it must be in linear space
  orig_vals_se = "standard_error"  # same as above
)

# Now add the adjustments to the original data set
data_metareg[, c("meanvar_adjusted", "sdvar_adjusted",
               "pred_logit", "pred_se_logit", "data_id")] <- cw_preds

## plot ####################################
# make plots to see how the crosswalk went
pdf(paste0(xwalk_dir, "/plots/CW_plot_", Sys.Date(), ".pdf"), width=11, height=8.5)

ggplot(data_metareg[survey_method=="household"],
       aes(x=val, y=meanvar_adjusted)) +
  geom_point() +
  geom_abline(slope=1,intercept=0) +
  labs(title = "HAP, solid fuel type: Crosswalk from household to individual",
       x="original value (proportion)",
       y="adjusted value (proportion)") +
  theme_minimal()

dev.off()

# Update values with CW ################################################################
# Replace hh data with crosswalked values

# merge data_metareg with the original dataset
adjusted_vals <- data_metareg[, c("seq","meanvar_adjusted","sdvar_adjusted")]

out_final <- merge(out,adjusted_vals, by="seq", all.x=T)

# now replace the values with the crosswalked values
out_final[!is.na(meanvar_adjusted) & survey_method=="household",':='(val=meanvar_adjusted, variance=(sdvar_adjusted^2), standard_error=sdvar_adjusted)]

# remove the adjusted columns
out_final$meanvar_adjusted <- NULL
out_final$sdvar_adjusted <- NULL

# add in required columns
out_final[fuel_type=="solid" & survey_method=="household" & is_outlier==0, crosswalk_parent_seq:=seq]

# Funnel plot ####################################################################
repl_python()

plots<-import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = cw_model,
  cwdata = cw_dataset,
  continuous_variables = list(),
  obs_method = "household",  # alt def
  plot_note = "GBD2023 HAP CW funnel plot",
  plots_dir = xwalk_dir,
  file_name = "CW_funnel_plot",
  write_file = T
)

# Save CSVs ######################################################################
for (fuel in fuel_type){
  print(fuel)
  write.xlsx(out_final[fuel_type==fuel & is_outlier==0,], paste0(xwalk_dir, "/upload_cw_", fuel, ".xlsx"), sheetName="extraction", rowNames=F)
}

# Vetting ##################################################################################
CW_v <- 42682 #crosswalk version
b_v <- 44595 #bundle version

CW_bundle <- get_crosswalk_version(CW_v)

bundle_og <- get_bundle_version(b_v)[, .(seq,val,upper,lower)]
setnames(bundle_og, c("val","upper","lower"), c("val_og","upper_og","lower_og"))

# Merge the OG with the CW version
data <- merge(CW_bundle, bundle_og, by="seq", all.x=T)

# Merge super region so we can produce a graph for each
data <- merge(data, locs[, .(location_id,super_region_name)], by="location_id", all.x=T)

# Graph the difference between the original and new values
pdf(file=paste0(xwalk_dir, "FILEPATH_", format(Sys.Date(), "%m-%d-%y.pdf")), height = 9, width = 16)
graph <- ggplot(data[survey_method=="household"], aes(x=val_og, y=val)) +
  geom_point()+
  geom_abline(intercept = 0, slope = 1) +
  theme_minimal() +
  labs(title = "HAP Solid: Original Household vs Crosswalk to Individual",
       x = "Original Household (prop)",
       y = "Crosswalked to Individual (prop)") +
  facet_wrap(~super_region_name) +
  theme(plot.title = element_text(hjust = 0.5))

print(graph)
dev.off()