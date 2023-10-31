
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Age-split literature data using population structures and cases/sample size given.
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"
bundle_id <- "VALUE"
bundle_version_id <- "VALUE"
folder_root <-  "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

z <- qnorm(0.975)

split_extraction_fpath <- "FILEPATH"

write_path <- "FILEPATH"

###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))
source("/FILEPATH/split_group_review.R")
source('/FILEPATH/sex_split.R')


###### Pull in data
#################################################################################

data <- get_bundle_version(bundle_version_id, fetch = "all")
data <- data[clinical_data_type == "",]
data[, `:=` (lower=NA, upper=NA)]

cfr_data_nids <- fread("FILEPATH/cfr.csv")[, unique(nid)]
data[nid%in%cfr_data_nids & measure == "mtwith", ]

###### 1. Use literature data to sex-split where there is GR information
#################################################################################

## France data
france_append <- split_group_review(to_split = copy(data[nid == "401206" & measure == "mtwith" & sex == "Both",]),
                               to_split_with = copy(data[nid == "401206" & measure == "mtwith" & sex != "Both",]),
                               keep_age=T, keep_sex=F)
france_append[, mean := cases/sample_size]
france_append[, standard_error := sqrt(mean/sample_size)]
data[nid == "401206" & measure == "mtwith", group_review := 0]

## Finland data
finland_append <- split_group_review(to_split = copy(data[nid == "400596" & sex == "Both",]),
                                to_split_with =  copy(data[nid == "400596" & sex != "Both",]))
finland_append[, mean := cases/sample_size]
finland_append[measure=="prevalence",standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
finland_append[measure=="incidence",standard_error := sqrt(mean/sample_size)]
finland_append[measure=="incidence"&cases<5,standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
data[nid == "400596", group_review := 0]

## Canada data
canada_append <- split_group_review(to_split = copy(data[nid == "401232" & measure == "incidence" & sex == "Both",]),
                                to_split_with = copy(data[nid=="401232" & measure == "incidence" & sex != "Both",]),
                                keep_age=T, keep_sex=F, keep_year=T)
canada_append[, mean := cases/sample_size]
canada_append[measure=="incidence",standard_error := sqrt(mean/sample_size)]
canada_append[measure=="incidence"&cases<5,standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
data[nid == "401232" & measure == "incidence", group_review := 0]

## Bring back together
data <- rbindlist(list(data, france_append, finland_append, canada_append))
data[, `:=` (lower=mean-1.96*standard_error, upper=mean+1.96*standard_error)]

## Fix Denmark
data[location_name=="Denmark"&measure=="incidence",`:=`(recall_type_value=1,mean=0.00002233,cases=NA)]
data[location_name=="Denmark"&measure=="incidence",cases:=sample_size*mean]
data[location_name=="Denmark"&measure=="incidence",note_modeler := paste0("Re-extracted using Catherine's math, recalc to 1-yr; ", note_modeler)]

###### 2. Use population structure of cases to sex-split 
#################################################################################

## Prepare the "split" dt
splits <- data.table(readxl::read_excel(split_extraction_fpath))
splits[is.na(sample_size) & !is.na(cases), sample_size := sum(cases), by = c("nid", "year_start", "sex")]
splits[mean>1, mean := mean/100]
splits[is.na(mean), mean := cases/sample_size]
setnames(splits, "mean", "ratio")
splits <- splits[, .(nid, age_start, age_end, sex, year_start, year_end, ratio, sex_ratio)]
splits <- splits[!(nid == 399085 & year_start == 1991)]
splits[, pct_female := sex_ratio/(1+sex_ratio)]
splits[, pct_male := 1-pct_female]

age_groups <- get_ids("age_group")
copy_data <- copy(data)
data <- copy(copy_data)

## Sex split
for (ni in unique(splits$nid)) {
  
  append <- copy(data[nid == ni & measure %in% c("incidence", "prevalence")])
  print(ni)
  
  if (nrow(append) > 0) {
    
    pop <- get_population(age_group_id = "all", single_year_age = T, location_id = unique(append$location_id), 
                          year_id = unique(append$year_start), sex_id = c(1, 2, 3), gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    pop <- merge(pop, age_groups, by="age_group_id", all.x=T, all.y=F)
    # Age group names are sometimes characters... I only really need the ones that are numeric on their own.
    pop$age_group_name <- as.numeric(pop$age_group_name)
    pop <- pop[!(is.na(age_group_name))]
    
    append <- merge(append, unique(splits[nid == ni, .(pct_male, pct_female, nid)]), by = "nid", allow.cartesian = T)
    
    for (i in 1:nrow(append)) {
      
      st <- append[i, age_start]
      en <- append[i, age_end]
      yr <- round(append[i, year_start], 5)
      
      pop_m <- pop[sex_id == 1 & age_group_name >= st & age_group_name <= en & year_id == yr, sum(population)]
      pop_f <- pop[sex_id == 2 & age_group_name >= st & age_group_name <= en & year_id == yr, sum(population)]
      
      pct_male_po <- pop_m/(pop_m + pop_f)
      
      append[i, pct_male_pop := pct_male_po]
      
    }
    
    append_m <- copy(append)[, `:=` (sex = "Male", cases = cases * pct_male, sample_size = sample_size * pct_male_pop)]
    append_f <- copy(append)[, `:=` (sex = "Female", cases = cases * pct_female, sample_size = sample_size * (1-pct_male_pop))]
    
    append <- rbind(append_m, append_f)
    append[, `:=` (standard_error = NA, upper = NA, lower = NA)]
    append[, mean := cases/sample_size]
    
    z <- qnorm(0.975)
    append[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    append[is.na(standard_error) & measure == "incidence", standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    append[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    
    data[nid == ni & measure %in% c("incidence", "prevalence"), is_outlier := 1]
    append <- dplyr::select(append, names(data))
    print(unique(append$location_name))
    print(summary(data[nid == ni & measure %in% c("incidence", "prevalence"), mean]))
    print(summary(append$mean))
    
    data <- rbind(data, append)
    
  }
  
}



###### 2. Use population structure of cases to age-split 
#################################################################################

splits <- splits[nid != 401193]

for (ni in unique(splits$nid)) {
  
  print(ni)
  append <- copy(data[nid == ni & measure %in% c("incidence", "prevalence")])
  
  if (nrow(append) > 0) {
    
    
    sw <- copy(splits[nid == ni])
    
    pop <- get_population(age_group_id = "all", single_year_age = T, location_id = unique(append$location_id), 
                          year_id = unique(append$year_start), sex_id = c(1, 2, 3), gbd_round_id = gbd_round_id, decomp_step = decomp_step)
    pop <- merge(pop, age_groups, by="age_group_id", all.x=T, all.y=F)
    # Age group names are sometimes characters... I only really need the ones that are numeric on their own.
    pop$age_group_name <- as.numeric(pop$age_group_name)
    pop <- pop[!(is.na(age_group_name))]
    
    if ("Both" %in% sw$sex) {
      
      append[, c("age_start", "age_end", "sex") := NULL]
      append <- merge(append, sw[, .(nid, age_start, age_end, ratio, sex)], by = "nid", allow.cartesian = T)
      
    } else {
      
      append[, c("age_start", "age_end") := NULL]
      append <- merge(append, sw[, .(nid, age_start, age_end, ratio, sex)], by = c("nid", "sex"), allow.cartesian = T)
      
    }
    
    for (i in 1:nrow(append)) {
      
      st <- append[i, age_start]
      en <- append[i, age_end]
      sex <- ifelse(append[i, sex] == "Male", 1, ifelse(append[i, sex] == "Female", 2, 3))
      yr <- round(append[i, year_start], 5)
      
      popu <- pop[age_group_name >= st & age_group_name <= en & sex_id == sex & year_id == yr, sum(population)]
      
      append[i, population := popu]
      
    }
    
    append[, pop_sum := sum(population), by = "sex"]
    append[, pop_ratio := population/pop_sum]
    
    append[, cases := cases * ratio]
    append[, sample_size := sample_size * pop_ratio]
    
    append[, mean := cases/sample_size]
    append[, `:=` (upper = NA, lower = NA, standard_error = NA)]
    
    z <- qnorm(0.975)
    append[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    append[is.na(standard_error) & measure == "incidence", standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    append[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    
    append[, note_modeler := paste0(note_modeler, " | age-sex split with group review data and population")]
    
    data[nid == ni & measure %in% c("incidence", "prevalence"), is_outlier := 1]
    append <- dplyr::select(append, names(data))
    print(unique(append$location_name))
    print(summary(data[nid == ni & measure %in% c("incidence", "prevalence"), mean]))
    print(summary(append$mean))
    
    data <- rbind(data, append)
    
  }
  
}


###### 3. Assorted clean-ups
#################################################################################

data <- data[!(measure %in% c("incidence", "prevalence") & mean == 0)] #Remnant of age-splitting
data[is.na(upper), `:=` (upper = mean + 1.96*standard_error, lower = mean - 1.96*standard_error)]
data[, cv_icd := ifelse(grepl("cv_ICD", note_modeler), 1, 0)]
data <- data[!(measure == "proportion")]
data <- data[!(group_review %in% 0)]


###### 4. Clean multi-year incidence, etc.
############################################################################################################

# Some data points are multi-year (incidence over 3 years, for example). We should change these to 1-year incidence, prevalence.

multi_year <- copy(data[((measure != "mtwith" & recall_type == "Period: years" & recall_type_value > 1) | 
                           (measure != "mtwith" & recall_type == "Period: months" & recall_type_value != 12) | 
                           (measure == "mtwith" & ((cfr_period == "Period: years" & cfr_value > 1) | 
                                                     (cfr_period == "Period: months" & cfr_period != 12))))])

multi_year[, recall_years := ifelse(measure != "mtwith"& recall_type=="Period: months", recall_type_value / 12,
                                    ifelse(measure != "mtwith"& recall_type=="Period: years", recall_type_value,
                                           ifelse(measure == "mtwith" & cfr_period == "Period: months", cfr_value / 12,
                                                  ifelse(measure == "mtwith" & cfr_period == "Period: years", cfr_value, NA))))]

# Formula for multi-year IR as Carrie Purcell gave it to me:
# CI,t = 1 - e^IR*t
# - ln (1 - CI,t)/t = IR
append_incidence <- copy(multi_year[measure %in% c("incidence", "mtwith")])
append_incidence[, `:=` (mean = -log(1-mean)/recall_years, note_modeler = paste0(note_modeler, " | adjusted from multi-year to single year using the formula: IR = -ln(1-CI,t)/t"))]
append_incidence[, `:=` (cases = mean * sample_size, standard_error = sqrt(mean/sample_size))]
#append_incidence[, seq_parent := ifelse(is.na(seq_parent), seq, seq_parent)]
append_incidence[, recall_years := NULL]

# Mark everything you changed as an outlier
data[!(group_review %in% c(0)) &
       ((measure =="incidence" & recall_type == "Period: years" & recall_type_value > 1) | (measure == "incidence" & recall_type == "Period: months" & recall_type_value != 12) | (measure == "mtwith" & ((cfr_period == "Period: years" & cfr_value > 1) | (cfr_period == "Period: months" & cfr_period != 12)))) &
       measure != "proportion", is_outlier := 1]

# Append the new stuff back on
data <- do.call("rbind", list(data, append_incidence))


data <- data[!(is_outlier %in% 1)]


###### 5. Save for crosswalking, sex-splitting.
############################################################################################################

write.csv(data, paste0(write_path, "age_split_lit_data_", date, ".csv"))




