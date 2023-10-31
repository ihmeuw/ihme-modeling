# Title: Prep cbh for age-sex data
## Description: Compile CBH data for all sexes
rm(list=ls())

library(data.table)
library(readstata13)
library(foreign)
library(assertable)
library(argparse)
library(assertable)
library(plyr)
library(mortdb, lib.loc = "FILEPATH")

# For interactive testing
if(interactive()){

  version_id <- x

}else{

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "age sex version id")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
}

# directories
input_dir   = paste0("FILEPATH")
output_dir  = paste0("FILEPATH")
cbh_dir		  = "FILEPATH"
new_cbh_dir = "FILEPATH"

# age map
age_map <- c("enn" = 2,
             "lnn" = 3,
             "pnn" = 4,
             "pna" = 388,
             "pnb" = 389,
             "nn" = 42,
             "inf" = 28,
             "ch" = 5,
             "cha" = 238,
             # "cha" = 49,
             "chb" = 34,
             "u5" = 1)
age_map <- as.data.table(age_map, keep.rownames=T)
setnames(age_map, c("rn", "age_map"), c("age", "age_group_id"))

# compile CBH data --------------------------------------------------------

# pull list of survey folders - old directory
surveys <- list.dirs(cbh_dir, full.names=F, recursive=F)
surveys <- setdiff(surveys, c('Skeleton', 'FUNCTIONS', 'archive', 'DHS-OTHER'))
surveys <- union(surveys, c('DHS-OTHER/In-depth', 'DHS-OTHER/Special'))

# pull list of survey folder - new directory
surveys_new <- list.dirs(paste0("FILEPATH"), full.names=F, recursive=F)
surveys_new <- setdiff(surveys_new, c('_archive', '_documentation', '_Old_age_groups', '_pre_12-19_fix',
                                      '_ubcov', 'Skeleton', 'New folder', 'FINAL_ESTIMATES'))

# append old and new surveys into one survey list
surveys <-  union(surveys, surveys_new)

# initialize blanks
cbh <- data.table()
missing_files <- c()

# loop over survey and sex
for(survey in surveys){
  for(sex in c('males', 'females', 'both')){

    print(paste0(survey, ' - ', sex))

    file <- paste0("FILEPATH")
    file_old <- paste0("FILEPATH")
    if(file.exists(file)){
      temp <- as.data.table(read.dta13(file))
      setnames(temp, 'source', 'source_y')
      temp[, source := survey]
      temp[, sex := sub('s', '' , sex)]
      cbh <- rbind(cbh, temp, fill=T)

    } else if(file.exists(file_old)){
      temp <- as.data.table(read.dta13(file_old))
      setnames(temp, 'source', 'source_y')
      temp[, source := survey]
      temp[, sex := sub('s', '' , sex)]
      cbh <- rbind(cbh, temp, fill=T)
    } else {
      missing_files <- union(missing_files, paste0(survey, '-', sex))
    }

  } # end loop on sex
} # end loop on survey

# save missing files to a diagnostics file
missing_files <- data.table(folder = missing_files)
write.csv(missing_files, paste0("FILEPATH"))

# drop is missing q5
cbh <- cbh[!is.na(q5)]

# extract survey year, note that source_y column is both source and year
cbh[, survey_year := gsub("[^0-9]", "", source_y)]
cbh[, survey_year := strtoi(substr(survey_year, 1, 4))] # If survey spans multiple years use first year

# format source variable for consistency
cbh[source=='DHS-OTHER/In-depth', source:='DHS IN']
cbh[source=='DHS-OTHER/Special', source:='DHS SP']
cbh[source=='DHS_TLS', source:='DHS']
cbh[source=='IRQ IFHS', source:='IFHS']
cbh[source=='IRN DHS', source:='IRN HH SVY']
cbh[source=='East Timor 2003', source:='TLS2003']
cbh[source=='MICS', source:='MICS3']

# rename pna and pnb
setnames(cbh, "p_1p1_5", "p_pna")
setnames(cbh, "p_1p6_11", "p_pnb")

# sum death counts of pna and pnb for missing death_count_pnn
cbh[is.na(death_count_pnn), death_count_pnn := death_count_1_5mnths + death_count_6_11mnths]

# fill missing values
cbh[is.na(p_pnn), p_pnn := p_pna * p_pnb]
cbh[is.na(p_nn), p_nn := p_enn * p_lnn]
cbh[, p_inf := p_nn * p_pnn]
cbh[, p_ch := p_1p1 * p_1p2 * p_1p3 * p_1p4]
cbh[, p_cha := p_1p1]
cbh[, p_chb := p_1p2 * p_1p3 * p_1p4]
cbh[, p_u5 := 1 - q5]
death_cols <- c("death_count_enn", "death_count_lnn", "death_count_pnn", "death_count_1yr",
                "death_count_2yr", "death_count_3yr", "death_count_4yr") # must be mutually exclusive
cbh[, deaths_u5 := rowSums(.SD, na.rm=T), .SDcols = death_cols]

# convert to q-space
for(age in age_map$age){
  cbh[, (paste0("p_", age)) := 1 - get((paste0("p_", age)))]
}
colnames(cbh) <- sub("p", "q", colnames(cbh))

# fix ihme_loc_id 
setnames(cbh, "country", "ihme_loc_id")
cbh[nchar(ihme_loc_id) > 3 & !(ihme_loc_id %like% "_"),
    ihme_loc_id := paste0(substr(ihme_loc_id, 1, 3), "_",
                          substr(ihme_loc_id, 4, nchar(ihme_loc_id)))]
# check for missing ihme_loc_id
assert_values(cbh, colnames = "ihme_loc_id", test = "not_na")

# subset columns
cbh <- cbh[, c("ihme_loc_id", "year", "sex", "source", "source_y", "survey_year",
               "q_enn", "q_lnn", "q_nn", "q_pna", "q_pnb", "q_pnn", "q_inf",
               "q_ch", "q_cha", "q_chb", "q_u5", "deaths_u5")]

# drop duplicates
cbh <- unique(cbh)

# check whether data exists for male, female and both sexes
cbh <- cbh[, sex_count := .N, by=.(ihme_loc_id, year, source_y)]
assert_values(cbh, "sex_count", test = "lte", test_val = 3)

# mark incomplete data by sex
cbh[sex_count < 3, withinsex := 1]
cbh[is.na(withinsex), withinsex := 0]
cbh[, sex_count := NULL]

# check unique identifiers of data
dups <- duplicated(cbh, by= c("ihme_loc_id", "source_y", "year", "sex"))
if(unique(dups)!=F) stop('ihme_loc_id, source_y, year, and sex do not uniquely identify the data set')

# output cbh file
write.csv(cbh, paste0("FILEPATH"), row.names = F)
