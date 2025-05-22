
## ******************************************************************************
## Purpose: - Pull COD death counts for by loc/year/sex/age
##          - Pull population counts to be the denominator in the epi upload
##            sheet. Pool populations for the age buckets.
##          - Prep for upload to epi uploader
## Output:  Raw CSMR files, which need to be appended together through the bind script
## ******************************************************************************

rm(list= ls())


library(data.table)
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl)

source(paste0(FILEPATH, "/get_draws.R"))
source(paste0(FILEPATH, "/get_population.R"))
source(paste0(FILEPATH, "/get_age_spans.R"))
source(paste0(FILEPATH, "/get_model_results.R"))

# GET ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
acause<-args[1]
cause_id <- args[2]
bundle <- args[3]
loc_id<-args[4]
sex<-args[5]
cod_corr_ver_2020 <- 200
print(args)


draw_cols <- paste0("draw_", 0:999)

# DEFINE PARAMETERS ----------------------------------------------------------------
ages <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235)
date <- gsub("-", "_", Sys.Date())
years <- c(1990, 2010, 2020)

################################################################################################################
##code starts here; get populations and death counts to calculate CSMR 
################################################################################################################
prop_dt <- as.data.table(read.xlsx(paste0(FILEPATH, "/GBD2020_star4_5_cod_data_proportions_predictions.xlsx")))
prop_dt[, c("sex_binary", "sex", "df", "residual.scale")] <- NULL


#Population
pop <- get_population(age_group_id=ages, 
                      location_id=loc_id,
                      sex_id = sex,
                      year_id=years,
                      with_ui=FALSE, 
                      gbd_round_id = 7, 
                      decomp_step = "step3")



#pull death rates
 df <- get_draws(gbd_id_type = 'cause_id',
                 gbd_id = cause_id, 
                 source = 'codcorrect',
                 measure_id = 1,
                 sex_id = sex,
                 location_id = loc_id, 
                 metric_id = 1,
                 gbd_round_id = 7,
                 decomp_step = "iterative",
                 version_id = cod_corr_ver_2020,
                 year_id = years)


#merge pop counts onto main dataframe by loc/sex/age/year_bucket_start
df <- merge(df, pop, by = c('location_id', 'sex_id', 'age_group_id', 'year_id'))


# CALCULATE MORTALITY RATE AS DEATH DRAWS / POP
df[, (draw_cols) := lapply(.SD, function(x) x / population ), .SDcols = draw_cols]

# APPLY PROPORTION TO EXCLUDE NONTOXIC GOITER
for (year in years) {
  prop_dt1 <- subset(prop_dt, location_id ==loc_id & year_id == year & sex_id == sex)
  mean <- unique(prop_dt1$fit)
  sd <- unique(prop_dt1$se.fit)
  prop_scalars<-rnorm(1000,mean, sd)
          
  df1 <- subset(df, year_id == year)
        
for(i in 0:999) {
          draw <- paste0("draw_", i)
          
          df1[, draw := df1[, draw,with=F]*prop_scalars[i+1], with=F]
          
        }
       

# COLLAPSE EACH ROW TO MEAN, UPPER, LOWER
df1[, mean := rowMeans(.SD), .SDcols = draw_cols]
df1$lower <- df1[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
df1$upper <- df1[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
df1 <- df1[, -(draw_cols), with=FALSE]



# FORMAT FOR EPI UPLOAD
# put deaths in cases column, population in sample size column
df1[, seq := NA]
setnames(df1, c('year_id'), c('year_start'))
df1 <- df1[sex_id == 1, sex := 'Male']
df1 <- df1[sex_id == 2, sex := 'Female']
df1[, year_end := year_start]

#Convert the age group ids to starting age years
ages <- get_age_spans()
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235)
ages <- ages[age_group_id %in% age_using,]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
ages[(age_start >=1), `:=` (age_end = age_end -1)]

df1 <- merge(df1, ages, by = "age_group_id", all.x = TRUE)
df1 <- df1[, -c('age_group_id', 'sex_id', 'population', 'envelope', 'pop', 'sex_name', 'metric_id', 'measure_id', 'run_id', 'population')]


df1[, cases := '']
df1[, sample_size := '']
df1[, source_type := 'Facility - inpatient']
df1[, age_demographer := 1]
df1[, measure := 'mtspecific']
df1[, unit_type := 'Person']
df1[, unit_value_as_published := 1]
df1[, representative_name := 'Nationally representative only']
df1[, urbanicity_type := 'Unknown']
df1[, recall_type := 'Not Set']
df1[, extractor := 'hhan5']
df1[, is_outlier := 0]
df1[, underlying_nid := '']
df1[, sampling_type := '']
df1[, recall_type_value := '']
df1[, input_type := '']
df1[, standard_error := '']
df1[, effective_sample_size := '']
df1[, design_effect := '']
df1[, response_rate := '']
df1[, uncertainty_type_value := 95]
df1[, uncertainty_type := 'Confidence interval']


upload_filepath <- paste0(FILEPATH) 
write.csv(df1, upload_filepath, row.names = FALSE)
}
