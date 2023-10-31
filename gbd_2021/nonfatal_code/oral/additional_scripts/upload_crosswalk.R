


library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
#install.packages('msm', lib = '/ihme/homes/jab0412/R_packages')
library('msm')
library("LaplacesDemon")
library("car")
library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)


####Variables
outdir <- "FILEPATH"
bundle <- 262
measure <- 'prevalence'
trim <- 'trim' # trim or no_trim
trim_percent <- 0.15
version <- "07_30_V1"




#####Applying crosswalk
### For dental bundles, data may need to be combined for different measures (incidence and prevalence)
### For bundle 262 (chronic periodontal disease), data needs to be combined for all crosswalks
### File paths will need to be changed
df <- read.xlsx("FILEPATH")

df_prev <- read.xlsx("FILEPATH")
df_inc <- read.xlsx("FILEPATH")

locs <- get_location_metadata(22, decomp_step = 'step3')
#locs_super <- c('location_id', 'super_region_name')
locs <- locs[, c('location_id', 'super_region_id')]
#sdi_map <- fread('FILEPATH')

df_inc <- data.table(merge(df_inc, locs, by = 'location_id', all.x = TRUE))
#all_split <- merge(all_split, sdi_map, by = 'super_region_id', all.x = TRUE)

sex_names <- fread("FILEPATH")
df_inc <- merge(df_inc, sex_names, by = c('sex'), all.x = TRUE)



print(colnames(df_prev)[!colnames(df_prev) %in% colnames(df_inc)])

df_inc <- df_inc[, age_mid := round((age_start + age_end)/2)]


df_inc <- df_inc[, year_id := round((year_start + year_end) / 2)]

## The following columns adjust the incidence data to square with the prevalence data

df_inc$meanvar_adjusted <- df_inc$mean
df_inc$sdvar_adjusted <- df_inc$standard_error

df_inc$pred_logit <- 0
df_inc$pred_se_logit <- 0
df_inc$data_id <- c(6802:8290)
df_inc$obs_method <- "measured"

df <- rbind(df_prev, df_inc)




#Uploading Crosswalk Version
version_map <- read.xlsx('FILEPATH')

if (bun_id == 262){
  ##### For Bundle 262
  df_atch4 <- read.xlsx('FILEPATH')
  df_atch5 <- read.xlsx('FILEPATH')
  df_cpi3 <- read.xlsx('FILEPATH')
  
  
  check_df <- df_atch4[df_atch4$cv_atchloss4ormore == 0 & df_atch4$cv_atchloss5ormore == 0 & df_atch4$cv_cpiclass3 == 0, ]
  
  df_atch4 <- df_atch4[df_atch4$cv_atchloss4ormore == 1, ]
  df_atch5 <- df_atch5[df_atch5$cv_atchloss5ormore == 1, ]
  df_cpi3 <- df_cpi3[df_cpi3$cv_cpiclass3 == 1, ]
  
  df <- rbind(check_df, df_atch4, df_atch5, df_cpi3)
  
  
}
#Adjusting Rows for Crosswalk Validations
## Only keep rows where group_review == 0
df$standard_error[df$lower == 0 & is.na(df$upper) & df$measure == "prevalence"] <- 0
df$upper[df$lower == 0 & is.na(df$upper) & df$measure == "prevalence"] <- 0

df$mean[is.na(df$mean)] <- 0
df$cases[is.na(df$cases)] <- 0


df$lower[df$lower < 0] <- 0

df <- df[df$group_review == 1 | is.na(df$group_review) | df$group_review == ' ', ]

df$unit_value_as_published[is.na(df$unit_value_as_published)] <- 1






write.xlsx(df,
           file = paste0("FILEPATH", bundle, "_upload.xlsx"),
           row.names = FALSE, sheetName = "extraction")


result <- save_crosswalk_version(33839, 
                                 data_filepath = paste0("FILEPATH", bundle, "_upload.xlsx"),
                                 description = "262_Chronic_Periodontal_All_Crosswalk_08_26")



print(result$crosswalk_version_id)


check_df <- get_crosswalk_version(29132)

version_map$crosswalk_version_id[version_map$bundle_id == bundle] <- result$crosswalk_version_id


write.xlsx(version_map,
           file = "FILEPATH",
           sheetName = 'extraction', row.names = FALSE)















