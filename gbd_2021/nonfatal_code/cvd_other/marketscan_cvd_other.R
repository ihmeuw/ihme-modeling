
##
## Author: USERNAME
## Date: DATE
##
## Purpose: Use MarketScan data to generate the odds ratio for cvd_other prevalence. Adapted from using MEPS.
##

rm(list = ls())
date <- gsub("-", "_", Sys.Date())
pacman::p_load(data.table, ggplot2, RMySQL, foreign)


###### Paths, args
#################################################################################

central <- "FILEPATH"
save_path <- "FILEPATH"
gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

## Do you need to query the MCT database?
query_mct <- F

## cvd_other calls
icd9_call <- c("BETWEEN 417 AND 418", "BETWEEN 420 AND 421", "BETWEEN 423 AND 424", "BETWEEN 427 AND 428", "BETWEEN 442 AND 443", "BETWEEN 447 AND 450",
               "BETWEEN 456 AND 457", "BETWEEN 457 AND 458", "BETWEEN 459 AND 460")
icd10_call <- c("BETWEEN 'I272' AND 'I273'", "BETWEEN 'I28' AND 'I29'", "BETWEEN 'I30' AND 'I33'", "BETWEEN 'I47' AND 'I48'", "BETWEEN 'I51' AND 'I52'",
                "BETWEEN 'I68' AND 'I69'", "BETWEEN 'I72' AND 'I73'", "BETWEEN 'I77' AND 'I80'", "BETWEEN 'I86' AND 'I90'", "BETWEEN 'I98' AND 'I99'")

## Paths for USERNAME's ICD maps
icd9_path <- "FILEPATH"
icd10_path <- "FILEPATH"

## Marketscan database
mkt_host <- "HOSTNAME"
mkt_user <- "USERNAME"
mkt_pass <- "PASSWORD"

## Tables to query
mkt_tables <- data.table(expand.grid(years=c(2010:2017), tables= "TABLES"))
mkt_tables[, tables := paste0(tables, "_", years)]
mkt_tables <- mkt_tables$tables

## Pull in the ICD codes and remove the dot
icd9_codes <- gsub("\\.", "", data.table(readxl::read_excel(icd9_path, sheet = "ICD9-map"))[yll_cause=="cvd_other", icd_code])
icd10_codes <- gsub("\\.", "", data.table(readxl::read_excel(icd10_path, sheet = "ICD10_map"))[yll_cause=="cvd_other", icd_code])

all_codes <- c(icd9_codes, icd10_codes)

## Sample size path
samp_size_path <- "FILEPATH"


###### Functions
#################################################################################

for (func in paste0(central, list.files(central))) source(paste0(func))


###### 1. Pull in MarketScan data from database
#################################################################################

if (query_mct) {

  ## Pull in patient encounters with broadly the right codes. Using BETWEEN notation because it's MUCH faster and less load on the database.

  ## Columns to query for the inpatient/outpatient tables
  inpatient_columns <- "VALUES"
  outpatient_columns <- "VALUES"

  df_list <- data.table()
  con <- dbConnect(dbDriver("MySQL"), username=mkt_user, password=mkt_pass, host=mkt_host)

  for (table in mkt_tables) {
    print(paste0("Starting on table ", table))

    if (grepl("inpatient", table)) {

      cols <- inpatient_columns
      col_count <- "VALUE"
      date_col <- "VALUE"

    } else {

      cols <- outpatient_columns
      col_count <- "VALUE"
      date_col <- "VALUES"

    }

    ## ICD10 in 2016, 2017, ICD9 before then
    calls <- icd9_call
    codes <- icd9_codes
    if (grepl("2016|2017", table)) calls <- icd10_call
    if (grepl("2016|2017", table)) codes <- icd10_codes

    for (i in 1:col_count) {

      print(i)

      for (call in calls) {

        print(call)

        sql_query <- "QUERY"

        df <- dbGetQuery(con, sql_query)
        df <- data.table(df)

        ## Restrict to the right codes
        codes <- paste0(codes, collapse = "|")
        dx_cols <- names(df)[grepl("dx", names(df))]
        df <- df[unique(unlist(lapply(df[, dx_cols, with=F], grep, pattern=codes)))]

        df_list <- rbind(data.table(df_list), df, fill=T)

      }
    }
  }
  dbDisconnect(con)

  ## Save intermediate
  fwrite(df_list, paste0(save_path, "inpatient_outpatient_unprepped.csv"))

  ## Restrict to full-year enrollees
  full_yr_2010_2015 <- fread("FILEPATH")
  full_yr_2016 <- fread("FILEPATH")
  full_yr_2017 <- fread("FILEPATH")

  for (yr in 2010:2017) {
    print(yr)
    if (yr <= 2015) df_list[year == yr, full_yr := ifelse(enrolid %in% unique(full_yr_2010_2015[year == yr, enrolid]), 1, 0)]
    if (yr == 2016) df_list[year == yr, full_yr := ifelse(enrolid %in% unique(full_yr_2016$enrolid), 1, 0)]
    if (yr == 2017) df_list[year == yr, full_yr := ifelse(enrolid %in% unique(full_yr_2017$enrolid), 1, 0)]
  }

  df_list <- df_list[full_yr == 1,]

  ## Rough collapse because it's a huge data.table
  df_list[, number_patients := length(unique(enrolid)), by = c("age", "sex", "year")]
  df_list <- unique(df_list[, .(age, sex, year, number_patients)])

  ## Collapse into age groups
  ages <- get_age_metadata(19, gbd_round_id)
  df_list[, age := as.numeric(age)]
  for (id in unique(ages$age_group_id)) {
    lower <- ages[age_group_id==id, age_group_years_start]
    upper <- ages[age_group_id==id, age_group_years_end]
    df_list[age >= lower & age < upper, `:=` (age_group_id = id, age_start = lower, age_end = upper)]
  }

  ## Calculate number of patients per age/sex/year
  df_list[, number_patients := sum(number_patients), by = c("age_group_id", "sex", "year")]
  numerator <- unique(df_list[!is.na(sex) & !is.na(age_group_id), .(number_patients, year, age_group_id, sex, age_start, age_end)])
  setnames(numerator, "sex", "sex_id")
  numerator <- numerator[sex_id != 0]

  ## Pull denominator
  denominator <- rbindlist(lapply(X = 2010:2017, FUN = function(x) {
    df <- data.table(foreign::read.dta(paste0(samp_size_path, x, "_", "single_age_enrollees.dta")))
    df
  }))
  denominator[, age_start := as.numeric(age_start)]
  setnames(denominator, "age_start", "age")
  denominator[, c("age_end") := NULL]
  for (id in unique(ages$age_group_id)) {
    lower <- ages[age_group_id==id, age_group_years_start]
    upper <- ages[age_group_id==id, age_group_years_end]
    denominator[age >= lower & age < upper, `:=` (age_start = lower, age_end = upper, age_group_id = id)]
  }
  denominator[, denominator := sum(sample_size), by = c("age_group_id", "sex", "year")]
  denominator <- unique(denominator[, .(age_start, age_end, sex, year, denominator, age_group_id)])
  setnames(denominator, "sex", "sex_id")
  denominator[, sex_id := as.numeric(sex_id)]
  denominator <- denominator[sex_id != 0]

  ## Merge, calculate prevalence
  final_dt <- merge(numerator, denominator, by = c("age_group_id", "sex_id", "year", "age_start", "age_end"), all = T)
  final_dt[is.na(number_patients), number_patients := 0]
  final_dt[, prevalence := number_patients/denominator]
  z <- qnorm(0.975)
  final_dt[, standard_error := sqrt((prevalence * (1 - prevalence))/denominator + z^2/(4 * denominator^2))]

  ## Expand into draw-space. Assume a normal distribution. There must be a way to do this in data.table
  for (row in 1:nrow(final_dt)) {

    print(row)

    mn <- final_dt[row, prevalence]
    sd <- final_dt[row, standard_error]

    preds <- rnorm(n = 1000, mean = mn, sd = sd)

    final_dt[row, paste0("draw_", 0:999) := lapply(X = 1:1000, FUN = function(x) preds[x])]

  }

  ## Save
  fwrite(final_dt, paste0(save_path, "final_prevalence.csv"))

} else {

  final_dt <- fread(paste0(save_path, "final_prevalence.csv"))

}

###### 2. Calculate odds ratio
#################################################################################
setnames(final_dt, old = "year", new = "year_id")

# restrict to ICD-10 coded years (2016 and 2017)
final_dt = final_dt[year_id>=2016]

# reshape MarketScan data
final_dt <- melt(as.data.table(final_dt), measure.vars = names(final_dt)[grepl("draw", names(final_dt))], variable.name = "draw", value.name = "marketscan_val")

# data coded to age_group_id=2 are actually ages 0-1 as MarketScan only has age groups 0-1, 1-2, etc., recode appropriately
final_dt[, `:=` (age_end = ifelse(age_group_id==2, 1, age_end), age_group_id = ifelse(age_group_id==2, 28, age_group_id))]

ages <- get_age_metadata(19, gbd_round_id)

# use 2015 draw data to match most closely to 2016 and 2017 marketscan prevalence data
draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = "VALUE", source="epi",
                   gbd_round_id = gbd_round_id, decomp_step = decomp_step, status="best",
                   measure_id = 5, location_id = 102, year_id = 2015,
                   age_group_id = ages$age_group_id, sex_id = c(1, 2))

# reshape draws data
draws <- melt(draws, measure.vars = names(draws)[grepl("draw", names(draws))], variable.name = "draw", value.name = "draw_val")
draws <- draws[, c("age_group_id", "location_id", "sex_id", "year_id", "draw", "draw_val")]

# aggregate data for age groups 2, 3, 388, 389 into age group 28 to match MarketScan 0-1 age group
# pull population sizes
pop <- get_population(age_group_id = c(2, 3, 388, 389), sex_id = c(1,2), location_id = 102, year_id = unique(draws$year_id),
                      gbd_round_id = gbd_round_id, decomp_step = decomp_step, status = "best")

pop$run_id <- NULL

# sum to obtain ages 0-1 population size
library(dplyr)
pop_sums <- pop %>%
  group_by(sex_id, location_id, year_id) %>%
  summarise(pop_sums = sum(population))

# calculate age weights
pop <- merge(pop, pop_sums, by=c("year_id", "location_id", "sex_id"))
pop[, population := population/pop_sums]
pop$pop_sums <- NULL

# multiply draws of GBD age groups < 1 by age weights 
draws_under_1 = draws[age_group_id %in% c(2, 3, 388, 389)]
draws_under_1 = merge(draws_under_1, pop, by=c("year_id", "location_id", "sex_id", "age_group_id"))
draws_under_1[ , draw_val := draw_val*population]
draws_under_1$population=NULL

# sum to calculate draws for age bin 0-1
draws_under_1 <- draws_under_1 %>%
  group_by(sex_id, location_id, year_id, draw) %>%
  summarise(draw_val = sum(draw_val)) %>%
  as.data.table()

draws_under_1[, age_group_id := 28]

# update draws dataset with aggregated age group
draws = rbind(draws[!(age_group_id%in%c(2, 3, 388, 389))], draws_under_1)

# merge MarketScan data with draws data -- 2016 and 2017 marketscan data will be merged to 2015 draw data
merged_dt = merge(draws, final_dt, by=c("age_group_id", "sex_id", "draw"))

# Here we take the odds ratio of cvd_other divided by odds ratio of cvd_other due to HF
merged_dt[ , coef := (marketscan_val / (1 - marketscan_val)) / (draw_val / (1-draw_val))]

# Find average ratio for each sex, age group, and draw (collapsing on year)
merged_dt <- merged_dt %>%
  group_by(age_group_id, sex_id, draw) %>%
  summarise(coef = mean(coef))

###### 3. Save
#################################################################################

fwrite(merged_dt, paste0(save_path, "ratio_merge.csv"))

