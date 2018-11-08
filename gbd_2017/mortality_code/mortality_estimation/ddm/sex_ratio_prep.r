# Author: 


rm(list=ls())
library(data.table); library(haven); library(readr); library(readstata13); library(assertable); library(plyr); library(DBI); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j" 
  user <- Sys.getenv("USER")
  version_id <- commandArgs(trailingOnly = T)[1]
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
}
main_dir <- paste0("FILEPATH", version_id, "FILEPATH")


envelope <- get_mort_outputs(model_name = "with shock death number", model_type = "estimate", gbd_year = 2016)
envelope <- envelope[, list(ihme_loc_id, year = year_id, sex_id, age_group_id, mean)]
envelope[sex_id == 1, sex := "male"]
envelope[sex_id == 2, sex := "female"]
envelope[sex_id == 3, sex := "both"]
envelope[, sex_id := NULL]


both_sex_only_indicators <- fread(paste0("FILEPATH", version_id, "FILEPATH"))
both_sex_only_indicators[, source_type := NULL]
both_sex_only_indicators[, sex := "both"]
males <- copy(both_sex_only_indicators)
males[, sex := "male"]

females <- copy(both_sex_only_indicators)
females[, sex := "female"]

both_sex_only_indicators <- rbind(both_sex_only_indicators, males, females)

sex_ratios <- merge(both_sex_only_indicators, envelope, by = c('ihme_loc_id', 'year', 'sex'), all.x = T)



ages <- data.table(get_age_map('all'))
ages <- ages[, list(age_group_id, age_group_name, age_group_years_start, age_group_years_end)]
sex_ratios <- merge(sex_ratios, ages, by = 'age_group_id', all.x = T)
sex_ratios <- sex_ratios[!age_group_name %in% c("70+ years","All Ages", "5-14 years","Under 5", "Early Neonatal", "Late Neonatal", "Post Neonatal", "50-69 years", "15-49 years")]

sex_ratio_80plus <- sex_ratios[age_group_id %in% c(30:33)]
sex_ratio_80plus <- sex_ratio_80plus[, lapply(.SD, sum, na.rm = T), .SDcols = c('mean'), by = c('ihme_loc_id', 'sex', 'year')]
sex_ratio_80plus[, age_group_id := 21]
sex_ratio_80plus <- merge(sex_ratio_80plus, ages, by = 'age_group_id', all.x =T)

sex_ratio_45plus <- sex_ratios[age_group_id %in% c(14:20, 30:33)]
sex_ratio_45plus <- sex_ratio_45plus[, lapply(.SD, sum, na.rm = T), .SDcols = c('mean'), by = c('ihme_loc_id', 'sex', 'year')]
sex_ratio_45plus[, age_group_id := 223]
sex_ratio_45plus <- merge(sex_ratio_45plus, ages, by = 'age_group_id', all.x =T)

sex_ratio_60plus <- sex_ratios[age_group_id %in% c(17:20, 30:33)]
sex_ratio_60plus <- sex_ratio_60plus[, lapply(.SD, sum, na.rm = T), .SDcols = c('mean'), by = c('ihme_loc_id', 'sex', 'year')]
sex_ratio_60plus[, age_group_id := 231]
sex_ratio_60plus <- merge(sex_ratio_60plus, ages, by = 'age_group_id', all.x =T)

sex_ratios <- rbind(sex_ratios, sex_ratio_80plus, sex_ratio_45plus, sex_ratio_60plus)
rm(sex_ratio_80plus, sex_ratio_60plus, sex_ratio_45plus)

sex_ratios[, year_start := age_group_years_start]
sex_ratios[, year_end := age_group_years_end - 1]
sex_ratios[age_group_name == "<1 year", year_end := 0]
sex_ratios[age_group_name == "95 plus", year_end := 100]
sex_ratios[age_group_name == "80 plus", year_end := 100]
sex_ratios[age_group_name == "45 plus", year_end := 100]

sex_ratios <- sex_ratios[, list(ihme_loc_id, year, sex, age_group_id, age_group_name, year_start, year_end, mean)]

id_vars <- c('ihme_loc_id', 'year', 'sex', 'age_group_id', 'age_group_name')
template <- sex_ratios[, .(age = year_start:year_end), by = id_vars]
template <- template[, list(ihme_loc_id, year, sex, age_group_id, age_group_name, age)]

sex_ratios <- merge(template, sex_ratios, by = c('ihme_loc_id', 'year', 'sex', 'age_group_id', 'age_group_name'), all.x = T)
sex_ratios <- dcast.data.table(sex_ratios, ihme_loc_id + year + age_group_id + age_group_name + age + year_start + year_end ~ sex, value.var = 'mean')


sex_ratios[, male_ratio := male/both]
sex_ratios[, female_ratio := female/both]
sex_ratios[, age := as.character(age)]
sex_ratios[grepl("plus", age_group_name) & age_group_name != "95 plus", age := age_group_name]
sex_ratios[, age := gsub(" ", "", age)]
sex_ratios[, age_group_name := NULL]
sex_ratios <- unique(sex_ratios)

sex_ratio_males <- sex_ratios[, list(ihme_loc_id, year, sex = "male", age, ratio = male_ratio)]
sex_ratio_males <- dcast.data.table(sex_ratio_males, ihme_loc_id + year + sex ~ paste0("ratio", age), value.var = 'ratio')

sex_ratio_females <- sex_ratios[, list(ihme_loc_id, year, sex = "female", age, ratio = female_ratio)]
sex_ratio_females <- dcast.data.table(sex_ratio_females, ihme_loc_id + year + sex ~ paste0("ratio", age), value.var = 'ratio' )
final_sex_ratios <- rbind(sex_ratio_males,sex_ratio_females)

write_csv(final_sex_ratios, paste0(main_dir, "sex_ratios.csv"))

# DONE