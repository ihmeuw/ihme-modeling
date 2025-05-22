################################################################################
## DESCRIPTION: Aggregates ST-GPR model results across global, super-region, and region
## INPUTS: run_id of STGPR models ##
## OUTPUTS: CSVs of aggregated age-pattern results ##
## AUTHOR:
## DATE:
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


# Base filepaths
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- paste0(code_dir, "FILEPATH")
save_dir <- "FILEPATH"

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
source("FILEPATH") # Load the aggregating functions
source("FILEPATH/stgpr/api/public.R") # this is the new path for STGPR functions
library(dplyr)

# Which version of age-pattern models
version = 1
age_group_set_id <- 24
release_id <- 16

# Read in tracking sheet
tracking <- fread(paste0(work_dir, "age_pattern_tracking.csv"))

# Load the models
ow_ap <- get_estimates(tracking[model_index_id == 1, run_id], "final")
ob_ap <- get_estimates(tracking[model_index_id == 2, run_id], "final")
uw_ap <- get_estimates(tracking[model_index_id == 3, run_id], "final")
suw_ap <- get_estimates(tracking[model_index_id == 4, run_id], "final")

# Run the functions
##### Prev ow
ow_reg_binned <- reg_agg(ow_ap, year_bins = 20)
ow_sr_binned <- sr_agg(ow_ap, year_bins = 20)
ow_glb_binned <- glb_agg(ow_ap, year_bins = 20)

##### Prop obese
ob_reg_binned <- reg_agg(ob_ap, year_bins = 20)
ob_sr_binned <- sr_agg(ob_ap, year_bins = 20)
ob_glb_binned <- glb_agg(ob_ap, year_bins = 20)

##### Prev underweight
uw_reg_binned <- reg_agg(uw_ap, year_bins = 20)
uw_sr_binned <- sr_agg(uw_ap, year_bins = 20)
uw_glb_binned <- glb_agg(uw_ap, year_bins = 20)

##### Prop severe underweight
suw_reg_binned <- reg_agg(suw_ap, year_bins = 20)
suw_sr_binned <- sr_agg(suw_ap, year_bins = 20)
suw_glb_binned <- glb_agg(suw_ap, year_bins = 20)

#######
# Age pattern by tertile of overweight and obesity
ow_categ <- cat_agg(ow_ap, year_bins = 20)
ob_categ <- cat_agg(ob_ap, year_bins = 20)
uw_categ <- cat_agg(uw_ap , year_bins = 20)
suw_categ <- cat_agg(suw_ap, year_bins = 20)

ow_categ_all_years <- cat_agg(ow_ap, super_regions = F)
ob_categ_all_years <- cat_agg(ob_ap, super_regions = F)
uw_categ_all_years <- cat_agg(uw_ap, super_regions = F)
suw_categ_all_years <- cat_agg(suw_ap, super_regions = F)

ow_categ_ap <- ow_categ_all_years[[1]]
ow_tert_locs <- ow_categ_all_years[[2]]

uw_categ_ap <- uw_categ_all_years[[1]]
uw_tert_locs <- uw_categ_all_years[[2]]

ob_categ_ap <- ob_categ_all_years[[1]]
ob_tert_locs <- ob_categ_all_years[[2]]

suw_categ_ap <- suw_categ_all_years[[1]]
suw_tert_locs <- suw_categ_all_years[[2]]

# Need to expand the list of ow_tert_locs to include subnational estimates
lvl_4 <- unique(locs[level == 4, .(location_id, parent_id)])
lvl_5 <- unique(locs[level == 5, .(location_id, parent_id, path_to_top_parent)])
sex_id = c(1,2)

# Duplicate out the locations and add sex_id
lvl_4 <- rbind(lvl_4, lvl_4) %>% mutate(sex_id = 1) %>% as.data.table(.)
lvl_4[1:552, sex_id := 2]

# Search for the level 3 location in the path_to_top_parent string
lvl_5 <- rbind(lvl_5, lvl_5) %>% mutate(sex_id = 1) %>% as.data.table(.)
lvl_5[1:149, sex_id := 2]
lvl_5[grep(',95,', lvl_5$path_to_top_parent), parent_id := 95]
lvl_5[grep(',163,', lvl_5$path_to_top_parent), parent_id := 163]
lvl_5[grep(',6,', lvl_5$path_to_top_parent), parent_id := 6]
lvl_5[grep(',180,', lvl_5$path_to_top_parent), parent_id := 180]
lvl_5$path_to_top_parent <- NULL

# Make new column parent_id to merge the weight tertile column on
lvl_3_ow <- copy(ow_tert_locs)
lvl_3_ob <- copy(ob_tert_locs)
lvl_3_uw <- copy(uw_tert_locs)
lvl_3_suw <- copy(suw_tert_locs)

lvl_3_ow$parent_id <- lvl_3_ow$location_id
lvl_3_ob$parent_id <- lvl_3_ob$location_id
lvl_3_uw$parent_id <- lvl_3_uw$location_id
lvl_3_suw$parent_id <- lvl_3_suw$location_id

lvl_4_ow <- merge(lvl_4, lvl_3_ow[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_4_ob <- merge(lvl_4, lvl_3_ob[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_4_uw <- merge(lvl_4, lvl_3_uw[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_4_suw <- merge(lvl_4, lvl_3_suw[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)

lvl_5_ow <- merge(lvl_5, lvl_3_ow[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_5_ob <- merge(lvl_5, lvl_3_ob[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_5_uw <- merge(lvl_5, lvl_3_uw[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)
lvl_5_suw <- merge(lvl_5, lvl_3_suw[,.(parent_id, weight_tert, sex_id)], by = c("parent_id", "sex_id"), all.x = T) %>% mutate(parent_id = NULL)

ow_tert_locs <- bind_rows(ow_tert_locs, lvl_4_ow, lvl_5_ow)
ob_tert_locs <- bind_rows(ob_tert_locs, lvl_4_ob, lvl_5_ob)
uw_tert_locs <- bind_rows(uw_tert_locs, lvl_4_uw, lvl_5_uw)
suw_tert_locs <- bind_rows(suw_tert_locs, lvl_4_suw, lvl_5_suw)


#####
# Save results
#####
# Going with the age-sex pattern created from tertiles of overweight and obesity
fwrite(ow_categ_ap, paste0(save_dir, "results/tert_ow_as_pattern.csv"))
fwrite(ob_categ_ap, paste0(save_dir, "results/tert_ob_as_pattern.csv"))
fwrite(uw_categ_ap, paste0(save_dir, "results/tert_uw_as_pattern.csv"))
fwrite(suw_categ_ap, paste0(save_dir, "results/tert_suw_as_pattern.csv"))

fwrite(ow_tert_locs, paste0(save_dir, "results/tert_ow_mapping.csv"))
fwrite(ob_tert_locs, paste0(save_dir, "results/tert_ob_mapping.csv"))
fwrite(uw_tert_locs, paste0(save_dir, "results/tert_uw_mapping.csv"))
fwrite(suw_tert_locs, paste0(save_dir, "results/tert_suw_mapping.csv"))



################################################################################
## Plot age patterns
################################################################################

# Read in ages
ages <- get_age_metadata(age_group_set_id=age_group_set_id, release_id=release_id)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

#####
# Global
#####

# Overweight set up
binned <- F
if(binned){
  master_glb_ow <- ow_glb_unbinned[year_id == 2022]
  master_glb_ow[,year_bin := "2022"][,year_id := NULL]
  setnames(master_glb_ow, "glb_mean", "glb_mean_binned")
  master_glb_ow <- rbind(master_glb_ow, ow_glb_binned)
} else {
  master_glb_ow <- copy(ow_glb_binned)
}

master_glb_ow <- merge(master_glb_ow, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_glb_ow[,age_mid := age_start + (age_end - age_start)/2]
master_glb_ow[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/glb_ow_ap_v", version, ".pdf"), width = 12, height = 8)
glb_ow_ap <- ggplot()+
  geom_point(data = master_glb_ow, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  geom_line(data = master_glb_ow, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  facet_wrap(~sex)+
  ylab("Prevalence of overweight")+
  xlab("Age")+
  labs(color = "Year bin")+
  ggtitle("Global age pattern for prevalence of overweight")+
  theme(plot.title = element_text(hjust = 0.5))
print(glb_ow_ap)
dev.off()

# Prop obese set up
if(binned){
  master_glb_ob <- ob_glb_unbinned[year_id == 2022]
  master_glb_ob[,year_bin := "2022"][,year_id := NULL]
  setnames(master_glb_ob, "glb_mean", "glb_mean_binned")
  master_glb_ob <- rbind(master_glb_ob, ob_glb_binned)
} else {
  master_glb_ob <- copy(ob_glb_binned)
}

master_glb_ob <- merge(master_glb_ob, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_glb_ob[,age_mid := age_start + (age_end - age_start)/2]
master_glb_ob[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/glb_ob_ap_v", version, ".pdf"), width = 12, height = 8)
glb_ob_ap <- ggplot()+
  geom_point(data = master_glb_ob, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  geom_line(data = master_glb_ob, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  facet_wrap(~sex)+
  ylab("Proportion overweight who are obese")+
  xlab("Age")+
  labs(color = "Year bin")+
  ggtitle("Global age pattern for proportion obese")+
  theme(plot.title = element_text(hjust = 0.5))
print(glb_ob_ap)
dev.off()

# Underweight set up
binned <- F
if(binned){
  master_glb_uw <- uw_glb_unbinned[year_id == 2022]
  master_glb_uw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_glb_uw, "glb_mean", "glb_mean_binned")
  master_glb_uw <- rbind(master_glb_uw, uw_glb_binned)
} else {
  master_glb_uw <- copy(uw_glb_binned)
}

master_glb_uw <- merge(master_glb_uw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_glb_uw[,age_mid := age_start + (age_end - age_start)/2]
master_glb_uw[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/glb_uw_ap_v", version, ".pdf"), width = 12, height = 8)
glb_uw_ap <- ggplot()+
  geom_point(data = master_glb_uw, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  geom_line(data = master_glb_uw, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  facet_wrap(~sex)+
  ylab("Prevalence of onderweight")+
  xlab("Age")+
  labs(color = "Year bin")+
  ggtitle("Global age pattern for prevalence of underweight")+
  theme(plot.title = element_text(hjust = 0.5))
print(glb_uw_ap)
dev.off()

# Prop severe underweight set up
if(binned){
  master_glb_suw <- suw_glb_unbinned[year_id == 2022]
  master_glb_suw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_glb_suw, "glb_mean", "glb_mean_binned")
  master_glb_suw <- rbind(master_glb_suw, suw_glb_binned)
} else {
  master_glb_suw <- copy(suw_glb_binned)
}

master_glb_suw <- merge(master_glb_suw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_glb_suw[,age_mid := age_start + (age_end - age_start)/2]
master_glb_suw[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/glb_suw_ap_v", version, ".pdf"), width = 12, height = 8)
glb_suw_ap <- ggplot()+
  geom_point(data = master_glb_suw, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  geom_line(data = master_glb_suw, aes(x = age_mid, y = glb_mean_binned, color = year_bin))+
  facet_wrap(~sex)+
  ylab("Proportion overweight who are severe underweight")+
  xlab("Age")+
  labs(color = "Year bin")+
  ggtitle("Global age pattern for proportion severe underweight")+
  theme(plot.title = element_text(hjust = 0.5))
print(glb_suw_ap)
dev.off()

#####
# Super-regions
#####

# Overweight set up

if(binned){
  master_sr_ow <- ow_sr_unbinned[year_id == 2022]
  master_sr_ow[,year_bin := "2022"][,year_id := NULL]
  setnames(master_sr_ow, "sr_mean", "sr_mean_binned")
  master_sr_ow <- rbind(master_sr_ow, ow_sr_binned)
} else {
  master_sr_ow <- copy(ow_sr_binned)
}

master_sr_ow <- merge(master_sr_ow, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_sr_ow[,age_mid := age_start + (age_end - age_start)/2]
master_sr_ow[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_sr_ow <- merge(master_sr_ow, unique(locs[,.(super_region_id, super_region_name)]), by = "super_region_id")

pdf(paste0(save_dir, "plots/sr_ow_ap_v", version, ".pdf"), width = 12, height = 8)
for(sr in unique(master_sr_ow$super_region_name)){
  sr_ow_ap <- ggplot()+
    geom_point(data = master_sr_ow[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    geom_line(data = master_sr_ow[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Prevalence of overweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of overweight in ", sr))+
    theme(plot.title = element_text(hjust = 0.5))
  print(sr_ow_ap)
}
dev.off()

# Prop obese set up

if(binned){
  master_sr_ob <- ob_sr_unbinned[year_id == 2022]
  master_sr_ob[,year_bin := "2022"][,year_id := NULL]
  setnames(master_sr_ob, "sr_mean", "sr_mean_binned")
  master_sr_ob <- rbind(master_sr_ob, ob_sr_binned)
} else {
  master_sr_ob <- copy(ob_sr_binned)
}

master_sr_ob <- merge(master_sr_ob, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_sr_ob[,age_mid := age_start + (age_end - age_start)/2]
master_sr_ob[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_sr_ob <- merge(master_sr_ob, unique(locs[,.(super_region_id, super_region_name)]), by = "super_region_id")

pdf(paste0(save_dir, "plots/sr_ob_ap_v", version, ".pdf"), width = 12, height = 8)
for(sr in unique(master_sr_ob$super_region_name)){
  sr_ob_ap <- ggplot()+
    geom_point(data = master_sr_ob[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    geom_line(data = master_sr_ob[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Proportion overweight who are obese")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for proportion obese in ", sr))+
    theme(plot.title = element_text(hjust = 0.5))
  print(sr_ob_ap)
}
dev.off()

# Underweight set up

if(binned){
  master_sr_uw <- uw_sr_unbinned[year_id == 2022]
  master_sr_uw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_sr_uw, "sr_mean", "sr_mean_binned")
  master_sr_uw <- rbind(master_sr_uw, uw_sr_binned)
} else {
  master_sr_uw <- copy(uw_sr_binned)
}

master_sr_uw <- merge(master_sr_uw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_sr_uw[,age_mid := age_start + (age_end - age_start)/2]
master_sr_uw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_sr_uw <- merge(master_sr_uw, unique(locs[,.(super_region_id, super_region_name)]), by = "super_region_id")

pdf(paste0(save_dir, "plots/sr_uw_ap_v", version, ".pdf"), width = 12, height = 8)
for(sr in unique(master_sr_uw$super_region_name)){
  sr_uw_ap <- ggplot()+
    geom_point(data = master_sr_uw[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    geom_line(data = master_sr_uw[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Prevalence of underweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of underweight in ", sr))+
    theme(plot.title = element_text(hjust = 0.5))
  print(sr_uw_ap)
}
dev.off()

# Prop severe underweight set up

if(binned){
  master_sr_suw <- suw_sr_unbinned[year_id == 2022]
  master_sr_suw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_sr_suw, "sr_mean", "sr_mean_binned")
  master_sr_suw <- rbind(master_sr_suw, suw_sr_binned)
} else {
  master_sr_suw <- copy(suw_sr_binned)
}

master_sr_suw <- merge(master_sr_suw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_sr_suw[,age_mid := age_start + (age_end - age_start)/2]
master_sr_suw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_sr_suw <- merge(master_sr_suw, unique(locs[,.(super_region_id, super_region_name)]), by = "super_region_id")

pdf(paste0(save_dir, "plots/sr_suw_ap_v", version, ".pdf"), width = 12, height = 8)
for(sr in unique(master_sr_suw$super_region_name)){
  sr_suw_ap <- ggplot()+
    geom_point(data = master_sr_suw[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    geom_line(data = master_sr_suw[super_region_name == sr], aes(x = age_mid, y = sr_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Proportion underweight who are severe underweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for proportion severe underweight in ", sr))+
    theme(plot.title = element_text(hjust = 0.5))
  print(sr_suw_ap)
}
dev.off()

#####
# Regions
#####

# Overweight set up

if(binned){
  master_reg_ow <- ow_reg_unbinned[year_id == 2022]
  master_reg_ow[,year_bin := "2022"][,year_id := NULL]
  setnames(master_reg_ow, "reg_mean", "reg_mean_binned")
  master_reg_ow <- rbind(master_reg_ow, ow_reg_binned)
} else {
  master_reg_ow <- copy(ow_reg_binned)
}
master_reg_ow <- merge(master_reg_ow, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_reg_ow[,age_mid := age_start + (age_end - age_start)/2]
master_reg_ow[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_reg_ow <- merge(master_reg_ow, unique(locs[,.(region_id, region_name)]), by = "region_id")

pdf(paste0(save_dir, "plots/reg_ow_ap_v", version, ".pdf"), width = 12, height = 8)
for(reg in unique(master_reg_ow$region_name)){
  reg_ow_ap <- ggplot()+
    geom_point(data = master_reg_ow[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    geom_line(data = master_reg_ow[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Prevalence of overweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of overweight in ", reg))+
    theme(plot.title = element_text(hjust = 0.5))
  print(reg_ow_ap)
}
dev.off()

# Prop obese set up

if(binned){
  master_reg_ob <- ob_reg_unbinned[year_id == 2022]
  master_reg_ob[,year_bin := "2022"][,year_id := NULL]
  setnames(master_reg_ob, "reg_mean", "reg_mean_binned")
  master_reg_ob <- rbind(master_reg_ob, ob_reg_binned)
} else {
  master_reg_ob <- copy(ob_reg_binned)
}
master_reg_ob <- merge(master_reg_ob, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_reg_ob[,age_mid := age_start + (age_end - age_start)/2]
master_reg_ob[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_reg_ob <- merge(master_reg_ob, unique(locs[,.(region_id, region_name)]), by = "region_id")

pdf(paste0(save_dir, "plots/reg_ob_ap_v", version, ".pdf"), width = 12, height = 8)
for(reg in unique(master_reg_ob$region_name)){
  reg_ob_ap <- ggplot()+
    geom_point(data = master_reg_ob[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    geom_line(data = master_reg_ob[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Proportion overweight who are obese")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of overweight in ", reg))+
    theme(plot.title = element_text(hjust = 0.5))
  print(reg_ob_ap)
}
dev.off()


# Underweight set up

if(binned){
  master_reg_uw <- uw_reg_unbinned[year_id == 2022]
  master_reg_uw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_reg_uw, "reg_mean", "reg_mean_binned")
  master_reg_uw <- rbind(master_reg_uw, uw_reg_binned)
} else {
  master_reg_uw <- copy(uw_reg_binned)
}
master_reg_uw <- merge(master_reg_uw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_reg_uw[,age_mid := age_start + (age_end - age_start)/2]
master_reg_uw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_reg_uw <- merge(master_reg_uw, unique(locs[,.(region_id, region_name)]), by = "region_id")

pdf(paste0(save_dir, "plots/reg_uw_ap_v", version, ".pdf"), width = 12, height = 8)
for(reg in unique(master_reg_uw$region_name)){
  reg_uw_ap <- ggplot()+
    geom_point(data = master_reg_uw[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    geom_line(data = master_reg_uw[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Prevalence of underweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of underweight in ", reg))+
    theme(plot.title = element_text(hjust = 0.5))
  print(reg_uw_ap)
}
dev.off()

# Prop severe underweight set up

if(binned){
  master_reg_suw <- suw_reg_unbinned[year_id == 2022]
  master_reg_suw[,year_bin := "2022"][,year_id := NULL]
  setnames(master_reg_suw, "reg_mean", "reg_mean_binned")
  master_reg_suw <- rbind(master_reg_suw, suw_reg_binned)
} else {
  master_reg_suw <- copy(suw_reg_binned)
}
master_reg_suw <- merge(master_reg_suw, ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_reg_suw[,age_mid := age_start + (age_end - age_start)/2]
master_reg_suw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_reg_suw <- merge(master_reg_suw, unique(locs[,.(region_id, region_name)]), by = "region_id")

pdf(paste0(save_dir, "plots/reg_suw_ap_v", version, ".pdf"), width = 12, height = 8)
for(reg in unique(master_reg_suw$region_name)){
  reg_suw_ap <- ggplot()+
    geom_point(data = master_reg_suw[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    geom_line(data = master_reg_suw[region_name == reg], aes(x = age_mid, y = reg_mean_binned, color = year_bin))+
    facet_wrap(~sex)+
    ylab("Proportion underweight who are severe underweight")+
    xlab("Age")+
    labs(color = "Year bin")+
    ggtitle(paste0("Age pattern for prevalence of severe underweight in ", reg))+
    theme(plot.title = element_text(hjust = 0.5))
  print(reg_suw_ap)
}
dev.off()


##############################################################################

# Overweight tertiles with 20 year bins
master_ow <- merge(ow_categ[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ow[,age_mid := age_start + (age_end - age_start)/2]
master_ow[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_ow_ap.pdf"), width = 12, height = 8)

tert_1_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ow[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 1st tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_1_ow_p)               

tert_2_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ow[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 2nd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_2_ow_p)  

tert_3_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ow[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_3_ow_p)

dev.off()

# obese tertiles with 20 year bins
master_ob <- merge(ob_categ[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ob[,age_mid := age_start + (age_end - age_start)/2]
master_ob[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_ob_ap.pdf"), width = 12, height = 8)

tert_1_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ob[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of obese")+
  xlab("age")+
  ggtitle("Age pattern for Proportion of obese 1st tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_1_ob_p)               

tert_2_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ob[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of obese")+
  xlab("age")+
  ggtitle("Age pattern for Proportion of obese 2nd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_2_ob_p)  

tert_3_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_ob[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of obese")+
  xlab("age")+
  ggtitle("Age pattern for Proportion of obese 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_3_ob_p)

dev.off()

# Underweight tertiles with 20 year bins
master_uw <- merge(uw_categ[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_uw[,age_mid := age_start + (age_end - age_start)/2]
master_uw[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_uw_ap.pdf"), width = 12, height = 8)

tert_1_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_uw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 1st tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_1_uw_p)               

tert_2_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_uw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 2nd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_2_uw_p)  

tert_3_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_uw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_3_uw_p)

dev.off()

# severe underweight tertiles with 20 year bins
master_suw <- merge(suw_categ[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_suw[,age_mid := age_start + (age_end - age_start)/2]
master_suw[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_suw_ap.pdf"), width = 12, height = 8)

tert_1_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_suw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion underweight who are severe underweight 1st tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_1_suw_p)               

tert_2_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_suw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion underweight who are severe underweight 2nd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_2_suw_p)  

tert_3_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  geom_line(data = master_suw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned, color = year_bin))+
  ylab("Proportion of severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion underweight who are severe underweight 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)
print(tert_3_suw_p)

dev.off()

######
#Overweight tertiles across all years and super-region layer
#####
master_ow <- merge(ow_categ_all_years[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ow_sr <- merge(ow_categ_all_years[[2]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ow[,age_mid := age_start + (age_end - age_start)/2]
master_ow_sr[,age_mid := age_start + (age_end - age_start)/2]
master_ow[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_ow_sr[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_ow_ap_all_years_v2.pdf"), width = 12, height = 8)

tert_1_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ow[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ow_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ow_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 1st tertile locations")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_1_ow_p)               

tert_2_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ow[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ow_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ow_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 2nd tertile locations")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_2_ow_p)  

tert_3_ow_p <- ggplot()+
  geom_point(data = master_ow[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ow[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ow_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ow_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of overweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of overweight 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_3_ow_p)

dev.off()

######
#Obese tertiles across all years
#####
master_ob <- merge(ob_categ_all_years[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ob_sr <- merge(ob_categ_all_years[[2]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_ob[,age_mid := age_start + (age_end - age_start)/2]
master_ob_sr[,age_mid := age_start + (age_end - age_start)/2]
master_ob[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_ob_sr[,sex := ifelse(sex_id == 1, "Male", "Female")]


pdf(paste0(save_dir, "plots/tertile_ob_ap_all_years_v2.pdf"), width = 12, height = 8)

tert_1_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ob[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ob_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ob_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of overweight who are obese")+
  xlab("age")+
  ggtitle("Age pattern for proportion obese, 1st tertile locations")+
  facet_wrap(~sex)
print(tert_1_ob_p)               

tert_2_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ob[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ob_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ob_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of overweight who are obese")+
  xlab("age")+
  ggtitle("Age pattern for proportion obese, 2nd tertile locations")+
  facet_wrap(~sex)
print(tert_2_ob_p)  

tert_3_ob_p <- ggplot()+
  geom_point(data = master_ob[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_ob[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_ob_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_ob_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of overweight who are obese")+
  xlab("age")+
  ggtitle("Age pattern for proportion obese, 3rd tertile locations")+
  facet_wrap(~sex)
print(tert_3_ob_p)

dev.off()


######
#underweight tertiles across all years and super-region layer
#####
master_uw <- merge(uw_categ_all_years[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_uw_sr <- merge(uw_categ_all_years[[2]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_uw[,age_mid := age_start + (age_end - age_start)/2]
master_uw_sr[,age_mid := age_start + (age_end - age_start)/2]
master_uw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_uw_sr[,sex := ifelse(sex_id == 1, "Male", "Female")]

pdf(paste0(save_dir, "plots/tertile_uw_ap_all_years_v2.pdf"), width = 12, height = 8)

tert_1_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_uw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_uw_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_uw_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 1st tertile locations")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_1_uw_p)               

tert_2_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_uw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_uw_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_uw_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 2nd tertile locations")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_2_uw_p)  

tert_3_uw_p <- ggplot()+
  geom_point(data = master_uw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_uw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_uw_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_uw_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Prevalence of underweight")+
  xlab("age")+
  ggtitle("Age pattern for prevalence of underweight 3rd tertile locations")+
  labs(color = "Year bin")+
  facet_wrap(~sex)+
  labs(color = "Super-region ID")
print(tert_3_uw_p)

dev.off()

######
#severe underweight tertiles across all years
#####
master_suw <- merge(suw_categ_all_years[[1]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_suw_sr <- merge(suw_categ_all_years[[2]], ages[,.(age_group_id, age_start, age_end)], by = "age_group_id")
master_suw[,age_mid := age_start + (age_end - age_start)/2]
master_suw_sr[,age_mid := age_start + (age_end - age_start)/2]
master_suw[,sex := ifelse(sex_id == 1, "Male", "Female")]
master_suw_sr[,sex := ifelse(sex_id == 1, "Male", "Female")]


pdf(paste0(save_dir, "plots/tertile_suw_ap_all_years_v2.pdf"), width = 12, height = 8)

tert_1_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_suw[weight_tert == 1], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_suw_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_suw_sr[weight_tert == 1], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of underweight who are severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion severe underweight, 1st tertile locations")+
  facet_wrap(~sex)
print(tert_1_suw_p)               

tert_2_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_suw[weight_tert == 2], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_suw_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_suw_sr[weight_tert == 2], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of underweight who are severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion severe underweight, 2nd tertile locations")+
  facet_wrap(~sex)
print(tert_2_suw_p)  

tert_3_suw_p <- ggplot()+
  geom_point(data = master_suw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_line(data = master_suw[weight_tert == 3], aes(x = age_mid, y = weight_tert_val_binned))+
  geom_point(data = master_suw_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  geom_line(data = master_suw_sr[weight_tert == 3], aes(x = age_mid, y = sr_weight_tert_binned, color = as.factor(super_region_id)), alpha = 0.5)+
  ylab("Proportion of underweight who are severe underweight")+
  xlab("age")+
  ggtitle("Age pattern for proportion severe underweight, 3rd tertile locations")+
  facet_wrap(~sex)
print(tert_3_suw_p)

dev.off()
