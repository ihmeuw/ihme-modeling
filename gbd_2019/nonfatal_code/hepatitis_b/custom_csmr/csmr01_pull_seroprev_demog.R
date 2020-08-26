
## ******************************************************************************
## Purpose: Pull unique loc/year/age/sex combos from seroprev bundle
## ******************************************************************************

rm(list=ls())

library(data.table)
library(openxlsx)

source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")

#101 is hep B seroprev bundle
#102 is hep C seroprev bundle
# because of the decomp step only need to compare new data uploaded for step 1 with the CSMR data

dt <- FILEPATH

#################################################
## Manually input which acause and bundle you are working on 

#drop rows that are for both sexes, or for age spans greater than 20
dt <- dt[sex == 'Male' | sex == 'Female',]
dt[, age_span := age_end - age_start]
dt <- dt[age_span <= 25,]

#drop unnecessary columns
keep_columns <- c('location_id','sex','year_start','year_end','age_start','age_end')
dt <- dt[, keep_columns, with=FALSE]
dt <- dt[, age_start_bundle := age_start]

#drop any duplicate rows
dt <- dt[!duplicated(dt, by = c('location_id','sex','year_start','year_end','age_start','age_end')), ]


ages <- get_age_metadata(age_group_set_id = 12)
ages <- ages[, -c("age_group_weight_value")]
age_ids <- c(5:20,28,30:32,235)
ages <- ages[age_group_id %in% age_ids,]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))


#pull age group ids (inclusive) for all ranges

#handle rows where age_start year is not the starting year of a standard age group
#recode the age_start year to the previous standard age group
#i.e. age_start of 2 recodes to age group from 1-5 yrs
dt <- dt[age_start > 1 & age_start < 5, age_start := 1]
dt <- dt[age_start > 5 & age_start < 10, age_start := 5]
dt <- dt[age_start > 10 & age_start < 15, age_start := 10]
dt <- dt[age_start > 15 & age_start < 20, age_start := 15]
dt <- dt[age_start > 20 & age_start < 25, age_start := 20]
dt <- dt[age_start > 25 & age_start < 30, age_start := 25]
dt <- dt[age_start > 30 & age_start < 35, age_start := 30]
dt <- dt[age_start > 35 & age_start < 40, age_start := 35]
dt <- dt[age_start > 40 & age_start < 45, age_start := 40]
dt <- dt[age_start > 45 & age_start < 50, age_start := 45]
dt <- dt[age_start > 50 & age_start < 55, age_start := 50]
dt <- dt[age_start > 55 & age_start < 60, age_start := 55]
dt <- dt[age_start > 60 & age_start < 65, age_start := 60]
dt <- dt[age_start > 65 & age_start < 70, age_start := 65]
dt <- dt[age_start > 70 & age_start < 75, age_start := 70]
dt <- dt[age_start > 75 & age_start < 80, age_start := 75]
dt <- dt[age_start > 80 & age_start < 85, age_start := 80]
dt <- dt[age_start > 85 & age_start < 90, age_start := 85]
dt <- dt[age_start > 90 & age_start < 95, age_start := 90]
dt <- dt[age_start > 95, age_start := 95]

#merge age group ids based on age start year
dt <- merge(dt, ages, by = 'age_start', all.x = TRUE)

# ERROR = RUN THIS TO FIGURE OUT WHAT DOES NOT HAVE AGE GROUP ID FILLED IN 
dt[is.na(age_group_id)]
dt <- dt[is.na(age_group_id), `:=` (age_group_id = 5, age_end.y = 5)]


#add additional rows if the bundle age range is wider than a standard age group

#age_start_bundle stores the original age start
#age_end.x stores the original age end
#age_start is updated dynamically to match the age group
#age_end.y is updated dynamically to match the age group
for (i in 1:nrow(dt)) {
  print(i)
  if (dt[i,age_end.x] > dt[i,age_end.y]) {

    dup_table <- copy(dt[i])
    dup_table[age_start >= 5, age_start := age_start + 5]
    dup_table[age_start == 1, age_start := 5]
    dup_table[age_start == 0, age_start := 1]
    dup_table <- dup_table[, -c('age_group_id', 'age_end.y')]
    dup_table <- merge(dup_table, ages, by='age_start', all.x = TRUE)

    dup_i <- 1
    while (dup_table[dup_i, age_end.x] > dup_table[dup_i,age_end]) {

      temp_row <- copy(dup_table[dup_i])
      temp_row[age_start >= 5, age_start := age_start + 5]
      temp_row[age_start == 1, age_start := 5]

      temp_row <- temp_row[, -c('age_group_id', 'age_end')]
      temp_row <- merge(temp_row, ages, by='age_start', all.x = TRUE)

      dup_table <- rbind(dup_table, temp_row)
      dup_i <- nrow(dup_table)
    }

    setnames(dup_table, 'age_end', 'age_end.y')
    dt <- rbind(dt, dup_table)

  }
}

head(dt)
tail(dt)

#apply same expansion logic to create additional rows for unique years
dt[, year_id := year_start]

for (i in 1:nrow(dt)) {

  if (dt[i, year_id] < dt[i, year_end]) {
    dup_table <- copy(dt[i])
    dup_table$year_id <- dup_table$year_id + 1
    dup_i <- 1

    while (dup_table[dup_i, year_id] < dup_table[dup_i, year_end]) {
      temp_row <- copy(dup_table[dup_i])
      temp_row$year_id <- temp_row$year_id + 1
      dup_table <- rbind(dup_table, temp_row)
      dup_i <- nrow(dup_table)
    }

    dt <- rbind(dt, dup_table)
  }
}

tail(dt)

#drop any duplicate rows
dt <- dt[!duplicated(dt, by = c('location_id','sex','year_id','age_group_id')), c('location_id','sex','year_id','age_group_id')]
dt[sex == 'Female', sex_id := 2]
dt[sex == 'Male', sex_id := 1]
dt$sex <- NULL

no_dup_sex <- dt[!is.na(sex_id)]
dup_sex <- dt[is.na(sex_id)]

dup_sex1 <- copy(dup_sex[, sex_id := 1])
dup_sex2 <- copy(dup_sex[, sex_id := 2])

demog_specs <- rbind(no_dup_sex, dup_sex1, dup_sex2)
demog_specs <- unique(demog_specs)


#add the full set of demographic combos for estimation years
years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 6)
locs <- locs[most_detailed == 1, location_id]
expected <- expand.grid(location_id=locs, year_id=years, sex_id=c(1,2), age_group_id=age_ids)
expected <- as.data.table(expected)
comb <- rbind(demog_specs, expected)

#drop any duplicate rows
comb <- comb[!duplicated(comb, by = c('location_id','sex_id','year_id','age_group_id')), c('location_id','sex_id','year_id','age_group_id')]
