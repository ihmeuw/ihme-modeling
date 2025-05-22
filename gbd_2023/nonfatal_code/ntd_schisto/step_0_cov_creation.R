##*********************************************************************************************************************************************************************
##*********************************************************************************************************************************************************************
##Purpose: Covariate for schisto models
##*********************************************************************************************************************************************************************
##*********************************************************************************************************************************************************************

#schisto cummulative treated according to WHO PCT databank

### ---------------------------------------------------------------------------------------- ######
### ========================= BOILERPLATE ========================= ###
rm(list=ls())
data_root <- "ADDRESS"
cause <- "ntd_schisto"

### ---------------------------------------------------------------------------------------- ######
# 0. Settings ---------------------------------------------------------------------------
j <- ("ADDRESS")

library(readstata13)
library(tidyverse)
library(haven)
library(data.table)
library(dplyr)
library(openxlsx)
library(readxl)
library(ggrepel)


source("/FILEPATH/get_location_metadata.R")
source("/FILEPATHget_demographics.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_best_model_versions.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/save_results_covariate.R")

release_id <- ADDRESS
year_id <- 1980:2024

# year would lag estimates 
gbd_year <- 2022


##### ---------------------
# Upload cov bundle
#change to 1 for updating bundle
u= 0
if(u =1 ){

# 1. Read in data ---------------------------------------------------------------------------
#total location list
all_locs <- get_location_metadata(location_set_id=22,  release_id = release_id)
all_locs <- all_locs[all_locs$level == 3,]

# pull current bundle
sch_mda <- get_bundle_version(bundle_version_id = ADDRESS)
#correcting - excluding subnational Niger
sch_mda <- subset( sch_mda, location_id !=ADDRESS)

#create a dt with variables needed
loc_vars <- sch_mda[ , c(1:6, 11:15, 29:32  )]
loc_vars <- distinct(loc_vars, location_id, .keep_all =T)


list_var <- sch_mda[, c(21:28)]
list_var <- colnames(list_var)

new_data <- read.xlsx("FILEPATH")

#Rename var
new_data_var <-  new_data[, c(4:11)]
new_data_var <- colnames(new_data_var)

setnames(new_data, c("Population.requiring.PC.for.SCH.annually", "SAC.population.requiring.PC.for.SCH.annually", "Number.of.people.targeted", "Reported.number.of.people.treated",
                     "Age.group", "Reported.number.of.SAC.treated", "Programme.coverage.(%)", "National.coverage.(%)" ),
         c("sac_pop_at_risk", "all_pop_at_risk", "pop_targeted", "pop_treated", "age_group", "sac_treated", "coverage", "nat_coverage"))


new_data1 <- merge(x= loc_vars, y= new_data, by.x= "location_name", by.y = "country", all.y=T)
new_data1[ , year_start := year]
new_data1[ , year_end := year]
new_data1[ , nat_coverage := nat_coverage /100]

sch_mda$nat_coverage <- as.numeric(sch_mda$nat_coverage)

compare_data <- merge(x = sch_mda, y= new_data1, by= c("location_id", "year_start"), all.x=T, all.y=T)

compare_data[, diff_nat := nat_coverage.x - (nat_coverage.y)]
compare_data[, diff_nat2 := nat_coverage.x/ (nat_coverage.y)]
compare_data1 <- subset(compare_data, diff_nat !=0)

sch_mda_data <- subset(sch_mda, select = c(nid, underlying_nid, field_citation_value, location_name, location_id, year_start, year_end, sex , seq, origin_seq, origin_id, measure, mean, lower, upper, bundle_id  ))

sch_mda_updated_data <- merge(x= sch_mda_data, y = new_data1, by = c('nid', 'underlying_nid', 'field_citation_value', 'location_name', 'location_id', 'year_start', 'year_end', 'sex' , 'measure', 'mean', 'lower', 'upper', 'bundle_id') , all.y= T, all.x=T)
sch_mda_updated_data[ , origin_id := 1]
sch_mda_updated_data[ , is_outlier := 0]
sch_mda_updated_data[ , age_start := 0]
sch_mda_updated_data[ , age_end := 99]
sch_mda_updated_data[ is.na(lancet_label), note_mean := "COMMENT"]

#############################################################################################################

#################################################################################################################
#Model covariate

### pull bundle data

main <- get_bundle_version(ADDRESS)
  
locs <- get_location_metadata(location_set_id=22,release_id = release_id)
locs <- locs[,.(location_id, location_name, region_name, region_id, ihme_loc_id, parent_id)]

mainnew <- merge(main, locs, by = c("location_name", "location_id"), all.x=T)
mainnew[, year_id := year_start]

temp_df23 <- mainnew[mainnew$year_id  == 2021,]
temp_df24 <- mainnew[mainnew$year_id  == 2021,]

#taking 2019 for China - 0 mda 
temp_df21china <- mainnew[mainnew$location_id == 6 & mainnew$year_id  == 2019,]
temp_df20china <- mainnew[mainnew$location_id == 6 & mainnew$year_id  == 2019,]
temp_df22china <- mainnew[mainnew$location_id == 6 & mainnew$year_id  == 2019,]
temp_df23china <- mainnew[mainnew$location_id == 6 & mainnew$year_id  == 2019,]
temp_df24china <- mainnew[mainnew$location_id == 6 & mainnew$year_id  == 2019,]

#Dominican Rep, Suriname, Venezuela, Iraq, Libya, Oman, Saudi Arabia, and Yemen have data up to 2016, No PC required
temp_df23$year_id <- 2023
temp_df24$year_id <- 2024


temp_df20china$year_id  <- 2020
temp_df21china$year_id  <- 2021
temp_df22china$year_id  <- 2022
temp_df23china$year_id  <- 2024
temp_df24china$year_id  <- 2023
mainnew <- rbind(mainnew,  temp_df21china,temp_df20china, temp_df22china, temp_df23, temp_df23china, temp_df24, temp_df24china)

mainnew$nat_coverage <- as.numeric(mainnew$nat_coverage)

mainnew2 <- copy(mainnew)
mainnew2$nat_coverage <- ifelse(is.na(mainnew2$nat_coverage) , 0, mainnew2$nat_coverage)

mainnew2 <- as.data.table(mainnew2)
mainnew2 <- mainnew2[order(location_id, year_id)]

#using cumsum 
mainnew2[, mda_upd := cumsum(nat_coverage), by = location_id]

###exclude countries for which no data reported, no MDA eligible
list_exc <- c(111,118,143,147,150,152)
z <- subset(mainnew2, (location_id %in% list_exc ))

mainnew3 <- subset(mainnew2, !(location_id %in% list_exc ))

mda_treatment_cov_2022 <- copy(mainnew3)
mda_treatment_cov_2022[, mean_value := mda_upd]
mda_treatment_cov_2022[, lower_value := mda_upd]
mda_treatment_cov_2022[, upper_value := mda_upd]

locsunique <- unique(mda_treatment_cov_2022$location_id)
mda_2005p <- subset(mda_treatment_cov_2022, year_id == 2006 & location_id %in% locsunique)
mda_2005p[, mean_value := 0]
mda_2005p[, lower_value := 0]
mda_2005p[, upper_value := 0]

for (year in c(1980:2005)) {
  mda_2005p$year_id <- year
  mda_treatment_cov_2022 <- rbind(mda_treatment_cov_2022, mda_2005p)
}

mda_indo06_10 <- subset(mda_treatment_cov_2022, year_id == 2010 & location_id ==11)
mda_indo06_10[, mean_value := 0]
mda_indo06_10[, lower_value := 0]
mda_indo06_10[, upper_value := 0]

for (year in c(1980:2009)) {
  mda_indo06_10$year_id <- year
  mda_treatment_cov_2022 <- rbind(mda_treatment_cov_2022, mda_indo06_10)
}

mda_ss06_11 <- subset(mda_treatment_cov_2022, year_id == 2011 & location_id == 435)
mda_ss06_11[, mean_value := 0]
mda_ss06_11[, lower_value := 0]
mda_ss06_11[, upper_value := 0]

for (year in c(1980:2010)) {
  mda_ss06_11$year_id <- year
  mda_treatment_cov_2022 <- rbind(mda_treatment_cov_2022, mda_ss06_11)
}

#append
mda_treatment_cov_2022a <- mda_treatment_cov_2022
mda_treatment_cov_2022a <- subset(mda_treatment_cov_2022a, select = c(location_id, sex, year_id,mean_value, lower_value, upper_value  ))
mda_treatment_cov_2022a[, round := "COMMENT"]

mda_indo06_10 <- subset(mda_treatment_cov_2022, year_id >= 2006 & location_id ==11)

#Estimating subnational
loc.meta <- get_location_metadata(location_set_id=22,  release_id = release_id)
mda_treatment_cov_2022a <- merge(x=mda_treatment_cov_2022a, y= loc.meta, by = 'location_id', all.x=T )

#mda_treatment_cov_2022a$nat_map <- gsub("\\_.*","",mda_treatment_cov_2022a$ihme_loc_id)
#mda_treatment_cov_2022a[, nat_map := location_id]

subnat_locs <- copy(loc.meta)
subnat_locs <- subset(subnat_locs, level >3 )
#subnat_locs[, nat_map := parent_id]
subnat_locs$nat_map <- gsub("\\_.*","",subnat_locs$ihme_loc_id)

subnat_val <- copy(mda_treatment_cov_2022a)
#subnat_val[, nat_map := location_id]
subnat_val$nat_map <- gsub("\\_.*","",subnat_val$ihme_loc_id)
subnat_val <- subset(subnat_val, select = c(nat_map, sex, year_id, mean_value, lower_value, upper_value, round  ))

sub_nat_final <- merge(x= subnat_locs, y= subnat_val, by= 'nat_map',  allow.cartesian=TRUE)

#append nat and subnat

mda_treatment_cov_f1 <- rbind(mda_treatment_cov_2022a, sub_nat_final, fill=T)
mda_treatment_cov_f1 <- subset(mda_treatment_cov_f1, select = c(location_id, year_id, mean_value, lower_value, upper_value, round))

non_zero_loc <- unique(mda_treatment_cov_f1$location_id)
zero_loc <- copy(loc.meta)
zero_loc <- subset(zero_loc, level  >= 3)
zero_loc <- subset(zero_loc, !(location_id %in% non_zero_loc))

 template <- NULL
 location_id <- unique(zero_loc$location_id)
 template <- as.data.table(location_id)
 template <- bind_rows(replicate(45, template, simplify = FALSE))
 template <- as.data.table(template)
 template <- template[order(location_id)]
 template <- cbind(template, year_id)

mda_zero <- copy(template) 
mda_zero[, mean_value := 0]
mda_zero[, lower_value := 0]
mda_zero[, upper_value := 0]
mda_zero[, round := "COMMENT"]


#append final data
mda_treatment_cov_f2 <- rbind(mda_treatment_cov_f1, mda_zero)
loc.meta2 <- subset(loc.meta, select = c (location_id, location_name))

mda_treatment_cov_f2 <- merge(x= mda_treatment_cov_f2, y=loc.meta2 , by = 'location_id', all.x=T )

#adding additional variables - # creating variables, age , sex, covariate_id, covariate_name_short,
mda_treatment_cov_f2[, age_group_id := 22]
mda_treatment_cov_f2[, sex_id := 3]
mda_treatment_cov_f2[, sex := "Both"]
mda_treatment_cov_f2[, covariate_id := ADDRESS]
mda_treatment_cov_f2[, covariate_name_short := 'COMMENT']

# save file
write.csv(mda_treatment_cov_f2, "FILEPATH")

save_cov <- save_results_covariate(
  input_dir = "FILEPATH",
  input_file_pattern = "ADDRESS",
  covariate_id=ADDRESS,
  description="COMMENT",
  release_id=ADDRESS
  
)
