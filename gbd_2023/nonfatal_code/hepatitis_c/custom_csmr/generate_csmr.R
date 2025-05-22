
## ******************************************************************************
## Purpose: - Pull post-CoDCorrect death counts for by loc/year/sex/age:
##            - acute hep b and c
##            - cirrhosis due to b and c
##            - liver cancer due to b and c
## ******************************************************************************

rm(list= ls())

library(data.table)

source(FILEPATH)

#define parameters
date <- gsub("-", "_", Sys.Date())
virus_index <- OBJECT
cod_corr_ver <- OBJECT
# decomp_step <- 4
bundle_map <- FILEPATH
cause_ids <- as.vector(as.matrix(bundle_map[virus_index, c('cause_1', 'cause_2', 'cause_3')]))


#pull in row id from bash command
args <- commandArgs(trailingOnly = TRUE)
loc <- OBJECT
print(paste0('loc_id: ',loc))
loc <- as.integer(loc)

sex <- OBJECT
print(paste0('sex_id: ',sex))
sex <- as.integer(sex)

gbd_round_id <- OBJECT
decomp_step <- OBJECT

param_map_path <- OBJECT
param_map <- fread(param_map_path)
param_map <- param_map[location_id == loc & sex_id == sex]

#pull death counts
df <- get_draws(gbd_id_type = c('cause_id', 'cause_id', 'cause_id'),
                gbd_id = cause_ids,
                source = 'codcorrect',
                measure_id = 1,
                age_group_id = unique(param_map$age_group_id),
                sex_id = sex,
                location_id = loc,
                metric_id = 1,
                release_id = release_id,
                version_id = cod_corr_ver,
                year_id = unique(param_map$year_id))
df <- df[, virus := bundle_map[virus_index, virus]]

# ADD DEATHS ACROSS VIRUSES
draw_cols <- paste0("draw_", 0:999)
df <- df[, lapply(.SD, sum),
           by = c('location_id', 'sex_id', 'age_group_id', 'virus', 'year_id'),
           .SDcols = draw_cols]


#PULL POP COUNTS
pop <- get_population(age_group_id = unique(param_map$age_group_id), location_id = loc, sex_id = sex, year_id = unique(param_map$year_id), gbd_round_id = gbd_round_id, decomp_step = decomp_step)
pop <- pop[, lapply(.SD, sum),
         by = c('location_id', 'sex_id', 'age_group_id', 'year_id'),
         .SDcols = 'population']

#merge pop counts onto main dataframe by loc/sex/age/year_bucket_start
df <- merge(df, pop, by = c('location_id', 'sex_id', 'age_group_id', 'year_id'), all.x = TRUE)


# CALCULATE MORTALITY RATE AS DEATH DRAWS / POP
df[, (draw_cols) := lapply(.SD, function(x) x / population ), .SDcols = draw_cols]

# COLLAPSE EACH ROW TO MEAN, UPPER, LOWER
df$mean <- rowMeans(df[,6:1005], na.rm = T)
df$lower <- df[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
df$upper <- df[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
df <- df[, -(draw_cols), with=FALSE]



# FORMAT FOR EPI UPLOAD
# put deaths in cases column, population in sample size column
df[, seq := NA]
setnames(df, c('year_id'),
             c('year_start'))
df <- df[sex_id == 1, sex := 'Male']
df <- df[sex_id == 2, sex := 'Female']
df <- df[, -c('sex_id', 'population')]
df[, year_end := year_start]

#Convert the age group ids to starting age years
ages <- get_age_spans()
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235)
ages <- ages[age_group_id %in% age_using,]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))

df <- merge(df, ages, by = "age_group_id", all.x = TRUE)
df <- df[, -c('age_group_id')]


df[, cases := '']
df[, sample_size := '']
df[, source_type := 'Facility - inpatient']
df[, age_demographer := 1]
df[, measure := 'mtspecific']
df[, unit_type := 'Person']
df[, unit_value_as_published := 1]
df[, representative_name := 'Nationally representative only']
df[, urbanicity_type := 'Unknown']
df[, recall_type := 'Not Set']
df[, extractor := USERNAME]
df[, is_outlier := 0]
df[virus == 'b', nid := 341161]
df[virus == 'c', nid := 341173]
df[, underlying_nid := '']
df[, sampling_type := '']
df[, recall_type_value := '']
df[, input_type := '']
df[, standard_error := '']
df[, effective_sample_size := '']
df[, design_effect := '']
df[, response_rate := '']
df[, uncertainty_type_value := 95]
df[, uncertainty_type := 'Confidence interval']


virus_name <- bundle_map[virus_index,virus]
bundle_id_loop <- bundle_map[virus_index, bundle_id]
df[virus == virus_name, bundle_id := bundle_id_loop]
df[virus == virus_name, bundle_name := bundle_map[virus_index, bundle_name]]

