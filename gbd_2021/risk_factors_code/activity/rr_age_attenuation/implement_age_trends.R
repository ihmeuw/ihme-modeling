#--------------------------------------------------------------------------------------
#  Implementation of using metabolic mediator age trends to generate
#     age specific risk curves. 
# 
#  Author: 
#  Edited:
#  
#
#  Inputs: rei_id and cause_id, filepath to MR-BRT exposure dependent draws
#--------------------------------------------------------------------------------------
rm(list = ls())
# Setup ---------------------------
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(data.table)
library(ggplot2)

#-----------------------------------

# Arguments ------------------------
mediator_pct_draw_dir <- "FILEPATH"

rei_id <- 125
cause_id <- 495 # 495 for ischemic stroke; 493 for IHD


# Run your RO ----------------------


# or a 'forced' average
# bmi = 370 ; sbp = 107; fpg = 141; ldl = 367
if(cause_id %in% c(493, 495)) {
  saverage_age_pct <- simple_average(causeid = cause_id,
                                     med_ids = c(107, 367))
} else {
  # No data for LDL and hem stroke 
  saverage_age_pct <- simple_average(causeid = cause_id,
                                     med_ids = c(107,141))
}

# Hold constant below 35 and above 85
# Load ages for adding on gbd age group ids
ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages[,.(age_start, age_end, age_group_id)]
special_ages <- c(10, 11, 31, 32, 235)
rep_ages <- function(age.id) {
  
  if(age.id < 12) {
    temp <- saverage_age_pct[age_group_id == 12]
    temp[, `:=` (age_group_id = age.id, 
                 age_start = ages[age_group_id == age.id,]$age_start, 
                 age_end = ages[age_group_id == age.id,]$age_end)]
    saverage_age_pct <<- rbind(saverage_age_pct[age_group_id != age.id], temp, use.names = TRUE)
  }
  
  if(age.id > 30) {
    temp <- saverage_age_pct[age_group_id == 30]
    temp[, `:=` (age_group_id = age.id, 
                 age_start = ages[age_group_id == age.id,]$age_start, 
                 age_end = ages[age_group_id == age.id,]$age_end)]
    saverage_age_pct <<- rbind(saverage_age_pct[age_group_id != age.id], temp, use.names = TRUE)
  }
  
}

sapply(special_ages, rep_ages)

fwrite(saverage_age_pct, "FILEPATH")
