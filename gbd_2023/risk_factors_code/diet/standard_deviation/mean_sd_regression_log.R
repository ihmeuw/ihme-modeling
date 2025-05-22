################################################################################
## DESCRIPTION ## Estimates standard deviation from mean for each diet risk using logistic regression.
## AUTHOR 
## DATE CREATED 
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
out_dir <- paste0(share_dir, "FILEPATH")



## LOAD DEPENDENCIES -----------------------------------------------------
source(paste0(code_dir, 'FILEPATH/primer.R'))
library(dplyr)
library(readstata13)
library(readxl)
library(data.table)
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_draws.R")



## LOAD RELEVANT DATA ----------------------------------------------------
old_all_diet_data <- as.data.table(read.dta13("FILEPATH/GBD_2017_no_as_split.dta"))

# Get diet IDs
diet_ids <- as.data.table(read.csv(paste0(out_dir, "diet_ids.csv")))

# Get SD correction factors
sd_corrections <- as.data.table(read_xlsx(paste0(out_dir, "intake_sd_correction_wknd.xlsx")))

locs <- get_location_metadata(35, gbd_round_id = 7) %>%
  select(location_name, location_id, ihme_loc_id, level, location_type)
countries <- filter(locs, level == 3)
exposure_2017_raw <- get_draws('modelable_entity_id', diet_ids$me_id,
                               location_id=countries$location_id,
                               year_id=2017, sex_id=c(1, 2), 
                               age_group_id=c(seq(10, 20), 30, 31, 32, 235), 
                               gbd_round_id = 5,
                               num_workers = 25,
                               source = "epi")
sd_2017_raw <- get_draws('modelable_entity_id', diet_ids$sd_id,
                               location_id=countries$location_id,
                               year_id=2017, sex_id=c(1, 2), 
                               age_group_id=c(seq(10, 20), 30, 31, 32, 235), 
                               gbd_round_id = 5,
                               num_workers = 25,
                               source = "epi")


## BODY ------------------------------------------------------------------

dr_data <- old_all_diet_data[cv_sales_data==0 & cv_FFQ==0 & cv_cv_hhbs==0 & cv_fao==0]
dr_data <- dr_data[!is.na(standard_deviation)]

ggplot(dr_data, aes(x = mean, y = standard_deviation))+geom_point()+facet_grid(~ihme_risk, scales = "free")

# Outlier points
dr_data <- dr_data[standard_deviation <= 4 * mean]

# Filter to only 25+ data
dr_data <- dr_data[age_start >= 25]

# Separate urinary sodium from other diet_salt
dr_data1 <- dr_data[cv_cv_urinary_sodium != 1]
dr_data2 <- dr_data[cv_cv_urinary_sodium == 1]
dr_data2[,ihme_risk := "diet_salt_urinary"]
dr_data <- rbind(dr_data1, dr_data2)
rm(dr_data1, dr_data2)

# Get omega-3 estimate with Japan data outlired: it influeces the regression hugely
dr_data1 <- dr_data[ihme_risk == "diet_omega_3"][location_name != "Japan"]
dr_data1[,ihme_risk := "diet_omega_3_nojapan"]
dr_data <- rbind(dr_data, dr_data1)
rm(dr_data1)

# Do regression - one for each dietary risk factor 
predicted_data <- data.table()
for (risk in unique(dr_data$ihme_risk)) {
  print(sprintf("Processing %s", risk))
  
  # Train the regression
  train_data <- dr_data[ihme_risk == risk]
  reg <- lm(log(standard_deviation) ~ log(mean), data = train_data)
  
  # Apply the regression
  temp_data <- copy(train_data)
  temp_data$pred_sd <- predict(reg, newdata = temp_data)
  temp_data$intercept <- reg$coefficients["(Intercept)"]
  temp_data$beta <- reg$coefficients["log(mean)"]
  predicted_data <- rbind(predicted_data, temp_data)
}
rm(train_data, reg, temp_data)

# Get country column
loc_names <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
country_from_3 <- loc_names[level==3,.(location_id,location_name)]
setnames(country_from_3, c("location_name", "location_id"), c("country_name", "country_id"))
country_from_4 <- merge(loc_names[level==4,.(location_id, parent_id)], country_from_3,
                        by.x="parent_id", by.y="country_id")
setnames(country_from_4, c("parent_id"), c("country_id"))
country_from_5 <- merge(loc_names[level==5,.(location_id, parent_id)], country_from_4,
                        by.x="parent_id", by.y="location_id")
country_from_5$parent_id <- NULL
country_from_3[, location_id := country_id]
loc_merge <- rbind(country_from_3, country_from_4, country_from_5)
predicted_data <- merge(predicted_data, loc_merge, by = "location_id")


# Make table of dietary factors, betas, and correction coefficients -----
betacorr <- predicted_data[,.(ihme_risk, intercept, beta)] %>%
  unique() %>%
  as.data.table()
betacorr[ihme_risk == "diet_fish", beta := betacorr[ihme_risk == "diet_omega_3", beta]]
betacorr[ihme_risk == "diet_fish", intercept := betacorr[ihme_risk == "diet_omega_3", intercept]]
betacorr[ihme_risk == "diet_calcium_low", ihme_risk := "diet_calcium"]
betacorr <- merge(betacorr, sd_corrections[,.(risk, ratio_mean)], by.x = "ihme_risk", by.y = "risk")
betacorr[ihme_risk == "diet_fish", ihme_risk := "diet_omega_3"]
names(betacorr) <- c("me_name", "intercept (log space)", "beta (log space)", "correction factor (normal space)")
betacorr <- betacorr[(order(me_name))]


# Save dataset of betas and correction coefficients
write.csv(betacorr, paste0(out_dir, "betas_corrections.csv"), row.names = F)

