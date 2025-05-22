# team: GBD Injuries
# script: apply adjustments to data (crosswalk!) for self-harm in GBD 2023

rm(list=ls())

source("FILEPATH")
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Get data
#data <- get_bundle_version(bundle_version_id = X)
data <- fread("FILEPATH")

# Ignore EMR row & CSMR row
data <- data[measure != "mtexcess"]
data <- data[measure != "mtspecific"]

# Make sure clinical data is tagged correctly
data[clinical_data_type == "inpatient", c("cv_recv_care")] <- 1
data[clinical_data_type == "inpatient", c("cv_no_care")] <- 0
data[clinical_data_type == "inpatient", c("cv_inpatient")] <- 1
data[clinical_data_type == "inpatient", c("cv_outpatient")] <- 0

data[clinical_data_type == "outpatient", c("cv_recv_care")] <- 1
data[clinical_data_type == "outpatient", c("cv_no_care")] <- 0
data[clinical_data_type == "outpatient", c("cv_inpatient")] <- 0
data[clinical_data_type == "outpatient", c("cv_outpatient")] <- 1

# Make sure non-clinical data is tagged correctly (new extractions)
data <- data[clinical_data_type == "" & (cv_outpatient == 1 | cv_inpatient == 1 | cv_ed == 1), cv_recv_care := 1]

# Make sure MNG data is tagged correctly
data <- data[location_id == 38 & nid != 547492 & source_type %like% "inpatient", cv_inpatient := 1]
data <- data[location_id == 38 & nid != 547492 & source_type %like% "inpatient", cv_recv_care := 1]

# # Turn any NAs into 0s
# test <- data[is.na(cv_inpatient), cv_inpatient := 0]
# test <- test[is.na(cv_outpatient), cv_outpatient := 0]
# test <- test[is.na(cv_recv_care), cv_recv_care := 0]
# test <- test[is.na(cv_no_care), cv_no_care := 0]

# adjust oldest age group so modeling midpoint isn"t around 110 years old
data$age_end <- ifelse(data$age_end > 99, 99, data$age_end)

#########################################
#########################################
########### ST-GPR ADJUSTMENT ###########
#########################################
#########################################

# load output from inj_recv_care_prop ST-GPR model
stgpr_id <- 57827
stgpr <- get_estimates(stgpr_id, entity = "final")

ages <- get_age_spans()
stgpr <- merge(ages, stgpr, by = c("age_group_id"))
setnames(
  stgpr,
  c("age_group_years_start", "age_group_years_end"),
  c("age_start", "age_end")
)

stgpr$gpr_mean <- stgpr$val
stgpr$gpr_se <- (stgpr$gpr_mean - stgpr$lower) / 1.96
stgpr$midpoint <- (stgpr$age_start + stgpr$age_end) / 2

df <- data
df$midpoint <- (df$age_start + df$age_end) / 2

df$gpr_mean <- NA
df$gpr_se <- NA

for (i in 1:nrow(df)) {
  mid <- df[i, "midpoint"]
  
  stgpr_sub <-
    subset(stgpr,
           year_id == df$year_start[i] &
             location_id == df$location_id[i])
  
  min_dif <- which.min(abs(stgpr_sub$midpoint - as.numeric(mid)))
  
  df$gpr_mean[i] <- stgpr_sub[min_dif, gpr_mean]
  df$gpr_se[i] <- stgpr_sub[min_dif, gpr_se]
  
  print(i)
  
}

# save old mean
df$mean_unadjusted <- df$mean

# save backup for debugging
#backup <- df
df <- backup

# # adjust data tagged with cv_no_care
# df$mean <-
#   ifelse(df$cv_no_care == 1,
#          (df$mean / (1 - df$gpr_mean)) * (df$gpr_mean),
#          df$mean)
no_care <- df[cv_no_care == 1]

# adjust data tagged with no covariates (injuries warranting care)
no_covs <- df[cv_recv_care == 0 & cv_no_care == 0 & cv_inpatient == 0 & cv_outpatient == 0]
df <- df[cv_recv_care == 0 & cv_no_care == 0 & cv_inpatient == 0 & cv_outpatient == 0, mean := mean*gpr_mean]
# df$mean <-
#   ifelse((df$cv_recv_care == 0) &
#            (df$cv_no_care == 0) &
#            (df$cv_inpatient == 0) &
#            (df$cv_outpatient == 0),
#          df$mean * df$gpr_mean,
#          df$mean
#   )

# recalculate standard error using closed form solution
# df$standard_error <-
#   ifelse((df$cv_no_care == 1) |
#            ((df$cv_recv_care == 0) &
#               (df$cv_no_care == 0) &
#               (df$cv_inpatient == 0) &
#               (df$cv_outpatient == 0)
#            ),
#          sqrt((df$standard_error * df$gpr_se) + (df$standard_error * df$gpr_mean) + (df$mean_unadjusted *
#                                                                                        df$gpr_se)
#          ),
#          df$standard_error)
df <- df[(cv_no_care == 1) | (cv_recv_care == 0 & cv_no_care == 0 & cv_inpatient == 0 & cv_outpatient == 0), 
         standard_error := sqrt((standard_error*gpr_se) + (standard_error*gpr_mean) + (mean_unadjusted*gpr_se))]

# clear out lower value that is no longer correct
# df$lower <-
#   ifelse((df$cv_no_care == 1) |
#            ((df$cv_recv_care == 0) &
#               (df$cv_no_care == 0) &
#               (df$cv_inpatient == 0) &
#               (df$cv_outpatient == 0)
#            ),
#          NA,
#          df$lower)
df <- df[(cv_no_care == 1) | (cv_recv_care == 0 & cv_no_care == 0 & cv_inpatient == 0 & cv_outpatient == 0), 
         lower := NA]

# clear out upper value that is no longer correct
# df$upper <-
#   ifelse((df$cv_no_care == 1) |
#            ((df$cv_recv_care == 0) &
#               (df$cv_no_care == 0) &
#               (df$cv_inpatient == 0) &
#               (df$cv_outpatient == 0)
#            ),
#          NA,
#          df$upper)
df <- df[(cv_no_care == 1) | (cv_recv_care == 0 & cv_no_care == 0 & cv_inpatient == 0 & cv_outpatient == 0), 
         upper := NA]

# at this point all data will all be at least cv_recv_care not going to change
# the covariates tagged though to preserve information about the original data
# point

fwrite(df, paste0("/FILEPATH/post_recv_care_", Sys.Date(), ".csv"))
