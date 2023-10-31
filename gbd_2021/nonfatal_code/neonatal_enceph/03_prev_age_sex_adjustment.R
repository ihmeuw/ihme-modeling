## ******************************************************************************
##
## Purpose: Generate age and sex weights from inpatient data to age-sex split 
##          enceph prevalence data 
## Input:   Processed inpatient data (under 1 aggregated and CF applied)
## Output:  csvs of weights
## Created: 2020-05-12
## Last updated: 2020-05-21
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<-"PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

pacman::p_load(data.table, openxlsx, msm)

source("PATHNAME/get_population.R")
source("PATHNAME/functions_agesex_split.R")

bun_id <- 338
bv_data_under1 <- data.table(read.xlsx("PATHNAME"))

#fill in any missing cases or sample sizes
if (nrow(bv_data_under1[is.na(cases) | is.na(sample_size)]) > 0) {
  bv_data_under1 <- calculate_cases_fromse(raw_dt = bv_data_under1)
}

# For claims data in GBD2020, assume all the data is actually birth prevalence
bv_data_under1[clinical_data_type == 'claims, inpatient only', age_end := 0]

write.xlsx(bv_data_under1,
           file = paste0("PATHNAME"), sheetName = 'extraction')


# Generate sex weights from pooled clinical data
at_birth <- bv_data_under1[clinical_data_type != '' & age_end == 0]
at_birth <- at_birth[is_outlier == 0 | is.na(is_outlier)]
at_birth <- calculate_cases_fromse(raw_dt = at_birth)
cases_1 <- sum(at_birth[sex == 'Male', cases]) / sum(at_birth$cases)
cases_2 <- sum(at_birth[sex == 'Female', cases]) / sum(at_birth$cases)
pop_1 <- sum(at_birth[sex == 'Male', sample_size]) / sum(at_birth$sample_size)
pop_2 <- sum(at_birth[sex == 'Female', sample_size]) / sum(at_birth$sample_size)
pooled_weights <- data.table(sex_id = c(1,2), case_weight = c(cases_1, cases_2),
                                   pop_weight = c(pop_1, pop_2),
                                   age_group_id = 164)

write.csv(pooled_weights, file = "PATHNAME", row.names = FALSE)


# No more age weights. Just get sex weights for splitting the lit data.

# case_weight_1 <- sum(bv_data[age_end == 0.01917808, cases]) / sum(bv_data$cases)
# case_weight_2 <- sum(bv_data[age_end == 0.07671233, cases]) / sum(bv_data$cases)
# case_weight_3 <- sum(bv_data[age_end == 0.5, cases]) / sum(bv_data$cases)
# case_weight_4 <- sum(bv_data[age_end == 0.999, cases]) / sum(bv_data$cases)
# 
# pop_weight_1 <- sum(bv_data[age_end == 0.01917808, sample_size]) / sum(bv_data$sample_size)
# pop_weight_2 <- sum(bv_data[age_end == 0.07671233, sample_size]) / sum(bv_data$sample_size)
# pop_weight_3 <- sum(bv_data[age_end == 0.5, sample_size]) / sum(bv_data$sample_size)
# pop_weight_4 <- sum(bv_data[age_end == 0.999, sample_size]) / sum(bv_data$sample_size)
# 
# pooled_weights <- data.table(age_group_id = c(2,3,388,389), sex_id = 1,
#                              case_weight = c(case_weight_1, case_weight_2, case_weight_3, case_weight_4),
#                              pop_weight = c(pop_weight_1, pop_weight_2, pop_weight_3, pop_weight_4))
# 
# pooled_weights_female <- data.table(age_group_id = c(2,3,388,389), sex_id = 2,
#                              case_weight = c(case_weight_1, case_weight_2, case_weight_3, case_weight_4),
#                              pop_weight = c(pop_weight_1, pop_weight_2, pop_weight_3, pop_weight_4))

