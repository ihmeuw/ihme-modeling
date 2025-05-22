### ======================= BOILERPLATE ======================= ###
rm(list = ls())

code_root <- "FILEPATH"
data_root <- "FILEPATH"
cause <- "ADDRESS"
library(data.table)

## Define paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--location_id", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
} 

## Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
library(dplyr)
source("/FILEPATH/get_demographics.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_draws.R")
source(paste0(code_root, "/FILEPATH.R")) 

out_dir <- paste0(draws_dir, '/ADDRESS')

## Define constants
release_id <- ADDRESS
version_id <- ADDRESS
  
###================ PULL IN POPULATION ESTIMATES ====================### 
demo <- get_demographics(ADDRESS, release_id = release_id)
years <- demo$year_id
ages <- demo$age_group_id

pop <- get_population(location_id = location_id,
                        year_id = years,
                        age_group_id = ages,
                        sex_id = c(1,2),
                        release_id = release_id)
  
###==================== PULL IN DEATH ESTIMATES ======================### 
## CODEm
#draws <- get_draws(gbd_id_type = "cause_id", gbd_id = ADDRESS, source = "codem", location_id = locs, status = "best", release_id = release_id)

## CodCorrect
draws <- get_draws(gbd_id_type = "cause_id", gbd_id = ADDRESS, source = "codcorrect", location_id = location_id, version_id = version_id, release_id = release_id, year_id = years, measure_id = 1)
cod_df <- draws[ which(draws$measure_id == 1), ]

###=================== CONVERT DEATHS TO CASES =======================### 
  ## Merge
  inc_df <- merge(pop, cod_df, by = c("location_id","year_id","age_group_id","sex_id"))
  
  ## Remove unneeded columns
  inc_df <- subset(inc_df, select = c(paste0("draw_",0:999), 'location_id', 'year_id', 'age_group_id', 'sex_id', 'population'))
  
  ## Incidence = (Deaths + Survivor [assuming 99% CF]) / population 
  ## rnbinom(): estimates the number of survivors based on the number of deaths (assumes 99% case-fatality rate, 1% survivors)
  deaths_to_cases <- function(deaths, p_death) {
    cases <- sapply(deaths, function(x) {
      if (x==0) {
        return(0)
      } else {
        return(x + rnbinom(1, x, p_death))
      }
    })
    return(cases)
  }
  
  inc_df[, paste0("cases_", 0:999) := lapply(0:999, function(x) deaths_to_cases(deaths = get(paste0("draw_", x)), p_death = 0.99))] 

  inc_df[ , paste0("inc_", 0:999) := lapply(0:999, function(x)
    get(paste0("cases_", x)) / population )]
  
###================ CONVERT INCIDENCE TO PREVALENCE ====================### 
## Create copy of df to estimate prevalence
  prev_df <- copy(inc_df)

## Prevalence = Incidence * 2-week duration
  prev_df[ , paste0("prev_", 0:999) := lapply(0:999, function(x)
    get(paste0("inc_", x)) * (2/52))]
  
###============================ WRAP UP ================================### 
  
## Drop unnecessary columns, add measure_id, combine
  inc_df[, paste0('draw_', 0:999) := NULL]
  inc_df[, paste0('cases_', 0:999) := NULL]
  colnames(inc_df) = gsub("inc", "draw", colnames(inc_df))
  inc_df$measure_id <- 6
  
  prev_df[, paste0('draw_',0:999) := NULL] 
  prev_df[, paste0('cases_',0:999) := NULL] 
  prev_df[, paste0('inc_',0:999) := NULL] 
  colnames(prev_df) = gsub("prev", "draw", colnames(prev_df))
  prev_df$measure_id <- 5
  
  final_df <- rbind(prev_df, inc_df)
  
  final_df$modelable_entity_id <- ADDRESS
  final_df$metric_id <- 3
  final_df[, population := NULL]
  
  zeros <- gen_zero_draws(modelable_entity_id = ADDRESS, location_id = location_id, measure_id = c(5,6), metric_id = 3, year_id = years, team = 'epi')
  zeros <- zeros[age_group_id %in% 2:3 ,]
  final_df_zeros <- rbind(final_df, zeros)
  
  final_df_zeros[, (paste0("draw_", 0:999)) := lapply(.SD, function(x) ifelse(x > 1.0, 0.999, x)), .SDcols=paste0("draw_", 0:999)]
  
  write.csv(final_df_zeros, file = (paste0(out_dir, '/', location_id, '.csv')), row.names = F)
  
  cat('\n Writing', location_id) 