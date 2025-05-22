################################################################################
## DESCRIPTION: Runs save results for all activity related MEs from pipeline ##
## INPUTS: Draws of activity results for specified me  ##
## OUTPUTS: None ##
## AUTHOR: ##
## DATE ##
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
library(mvtnorm)
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
source('FILEPATH')
library(mvtnorm)

## SCRIPT SPECIFIC FUNCTIONS

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--stgpr_run_id", help = "Run ID of desired ST-GPR run",
                    default = 189788, type = "integer")
parser$add_argument("--cycle_year", help = "current GBD cycle",
                    default = "gbd_2020", type = "character")
parser$add_argument("--mark_best", help = "Whether or not to mark results as best",
                    default = 1, type = "integer")

args <- parser$parse_args()
list2env(args, environment()); rm(args)


## LOAD DEMOGRAPHICS FOR QUERY AND VALIDATION
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'activity'))

## LOAD RELEVANT DATA

message(sprintf('Generating standard deviations, from ST-GPR run ID %d', stgpr_run_id))

data_df <- fread('FILEPATH')
source("FILEPATH")
library(dplyr)
age_data <- get_age_metadata(age_group_set_id = 1)
location_data <- get_location_metadata(location_set_id = 22)
data_df <- merge(data_df, select(location_data, location_id, super_region_name), by = "location_id")
data_df <- merge(data_df, select(age_data, age_group_id, age_group_years_start), by = "age_group_id")
data_df <- mutate(data_df, st_dev = se_total_mets * sqrt(sample_size), mean = data, female = sex_id - 1)
data_df <- data.table(data_df)

# Applying all-age, both-sex SR coefficient from gpaq/ipaq surveys
sr_coef <- 1/exp(0.73)
data_df$sr_coef <- sr_coef
setnames(data_df, c("st_dev", "mean"), c("sr_st_dev", "sr_mean"))
if(sum(is.na(data_df$sr_coef)) != 0) stop(message("Something went wrong in the merge."))

data_df[, `:=` (st_dev = sr_st_dev * sr_coef,
                mean = sr_mean * sr_coef)]

data_df[, outlier := (st_dev / mean > 1) | (st_dev / mean < .1)]

## Run mean-sd regressions
sd_mean_mod_mets <- lm(log(st_dev)~log(mean) + age_group_years_start + super_region_name + female, data = data_df[outlier == F])

# gen draws of exp sd
draws_mets <- rmvnorm(n = 1000, sd_mean_mod_mets$coefficients, vcov(sd_mean_mod_mets)) %>% as.data.table 
setnames(draws_mets, 1:2, c('log_beta_0', 'log_beta_1'))
draws_mets[, draw := seq(0, .N-1)]

ggplot(data = data_df[outlier==F], aes(x = log(mean), y = log(st_dev))) + geom_point(aes(color = outlier)) + stat_smooth(data = data_df[outlier == F], method = "lm", se = F, color = 'black') + theme_minimal()

## Save model objects and draws of coefficients
if(!dir.exists(sprintf("FILEPATH", data_dir, stgpr_run_id))) dir.create(sprintf("FILEPATH", data_dir, stgpr_run_id), recursive = T)
saveRDS(sd_mean_mod_mets, sprintf('FILEPATH', data_dir, stgpr_run_id))
write.csv(draws_mets, sprintf('FILEPATH', data_dir, stgpr_run_id), row.names = F)


## Run prediction on desired ST-GPR outputs
task_map <- list.files(sprintf("FILEPATH", data_dir, stgpr_run_id))
coeff_df <- draws_mets
library(stringr)
library(dplyr)
source("FILEPATH")
names(coeff_df) <- str_replace_all(names(coeff_df), c(" " = "", "-" = "", "," = "", "age_group_years_start" = "age_start"))
age_data <- get_age_metadata(age_group_set_id = 1)
loc_map <- get_location_metadata(location_set_id = 22)

mclapply(task_map, function(loc_id){
  
  loc_id = as.integer(str_split(loc_id, "\\.")[[1]][1])
  
  message(sprintf('Generating predictions of SD in METs for %s', loc_map[location_id == loc_id, location_name]))
  
  df <- fread(sprintf('FILEPATH', data_dir, stgpr_run_id, loc_id))
  
  # Convert df to long form
  draw_cols <- paste0("draw_", 0:999)
  df <- melt(df, id.vars = setdiff(names(df), draw_cols)) %>%
    data.table()
  df[, "draw"] <- sapply(str_split(df$variable, "_", simplify = T)[, 2], as.integer)
  df[, "variable"] <- NULL
  
  df <- merge(df, coeff_df, by = 'draw')
  df <- merge(df, select(location_data, location_id, super_region_name), by = "location_id")
  df <- merge(df, select(age_data, age_group_id, age_group_years_start), by = "age_group_id")
  df[, value := exp(log(value)*log_beta_1 + log_beta_0 + (sex_id - 1)*female + age_group_years_start*age_start +
                      (super_region_name == "High-income")*super_region_nameHighincome + 
                      (super_region_name == "Latin America and Caribbean")*super_region_nameLatinAmericaandCaribbean +
                      (super_region_name == "North Africa and Middle East")*super_region_nameNorthAfricaandMiddleEast +
                      (super_region_name == "South Asia")*super_region_nameSouthAsia +
                      (super_region_name == "Southeast Asia, East Asia, and Oceania")*super_region_nameSoutheastAsiaEastAsiaandOceania +
                      (super_region_name == "Sub-Saharan Africa")*super_region_nameSubSaharanAfrica)]
  df[, grep('log_beta|activity|algorithm|super_region_name|age_start|female|age_group_years', names(df), value = T) := NULL][, draw := paste0('draw_', draw)]
  df <- dcast(df, ...~draw, value.var = "value")
  df[, modelable_entity_id := 18704]
  
  setcolorder(df, c('location_id', 'age_group_id', 'sex_id', 'year_id', paste0('draw_', 0:999)))
  
  ## SAVE OUTPUTS
  write.csv(df, sprintf('FILEPATH', data_dir, stgpr_run_id, loc_id), row.names = F)
  
}, mc.cores = 25)

