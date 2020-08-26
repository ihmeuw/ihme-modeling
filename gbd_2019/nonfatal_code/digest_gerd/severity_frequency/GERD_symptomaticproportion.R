###########################################################
### Author: NAME
### Date: 5/15/2018
### Project: GERD symptom frequency distribution
### Purpose: GBD 2017 Nonfatal Analysis
### Adapted from code for headache frequency and duration from NAME
###########################################################

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  scratch <-"FILEPATH/GERD_sev_freq_distrib"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  scratch <- "FILEPATH/GERD_sev_freq_distrib"
}

##libraries
pacman::p_load(data.table, plyr, openxlsx, readxl, metafor)

##objects
input_path <- paste0(j, "FILEPATH/GERD_reformatted_freqextraction_18May18.xlsx")
# columns assumed: bundle_id	seq	nid	underlying_nid	gbd_component	cause_id	acause	location_id	location_name	year_start	year_end	measure	case_name	case_definition	case_diagnostics	note	parameter	parameter_scale	cat_mean	cat_start	cat_end	age_start	age_end	sex	cases	cases_total	sample_size	value_mean	standard_dev	mean	upper	lower	field_citation_value	underlying_filed_citation_value	is_outlier	note_modeler	questID group_review

output_path <- paste0(h, "FILEPATH/frequency_dayssymptomatic_19May2018.xlsx")
functions_dir <- paste0(j, "FILEPATH_CENTRAL_FXNS_GBD2017")

acause <- "digest_gerd"
bundle_id <- 3059
by_vars <- c("nid", "sex", "age_start", "age_end", "parameter", "case_name")

draws <- paste0("draw_", 0:999)
freq_draws <- paste0("draw_", 0:999, "_Frequency")
date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

##central functions
source(paste0(functions_dir, "get_epi_data.R"))

##user functions
beta_dis <- function(dt){
  #here, you fill the empty cat_mean column with the mean number of days symptomatic for the individuals in the observation, based on the frequency category for that observation, assuming uniform distribution within the category
  dt[, cat_mean := rowMeans(.SD), .SDcols = c("cat_start", "cat_end")]
  dt[, non_cases := cases_total-cases]
  #calculate the "non-cases" for each row as parameter for beta distribution; these are actually the cases that aren't in this frequency category
  beta_rows <- copy(dt)
  beta_rows[, row_count := 1:.N]
  #adds a column to the table that numbers all the rows in the sheet
  beta_cols <- data.table()
  #a new, empty, data table
  for (x in unique(beta_rows$row_count)){
  #for each row in the data table
    row <- beta_rows[row_count == x]
    #make a vector of that row
    beta_draws <- as.data.table(rbeta(n = 1000, shape1 = row$cases, shape2 = row$non_cases, ncp = 0))
    #make a new data table with rows for each of a thousand draws from a beta distribution that uses the case-counts for row x in the data table as shape parameters
    beta_draws[, row_count := x]
    #add a column with the row number to that table of draws
    beta_draws[, draw := paste0("draw_", 0:999)]
    #add a column numbering the draws in the new data table
    beta_draws <- dcast(beta_draws, row_count ~ draw, value.var = "V1")
    #reshape the draw table so draws for x all go wide
    beta_cols <- rbind(beta_cols, beta_draws)
    # append beta_draws (a datatable with a single row in each iteration of this loop) to beta_cols, the data table created empty before the loop started
    # this continues until you've done this for all x in beta_rows, which are all rows of original extraction sheet
  }
  beta_rows <- merge(beta_rows, beta_cols, by = "row_count")
  # merge all the draws (a thousand columns wide) onto your numbered copy of the extraction sheet; these are draws of proportions
  beta_rows[, (draws) := lapply(.SD, function(x) x * cat_mean), .SDcols = draws]
  #for all columns in the list of columns defined above as draws (so draw_0...draw_999), multiply the proportion in the draw column by the mean frequency for that category, to get frequency of symptoms in that category weighted by proportion of total cases that are in that category
  beta_rows[, (draws) := lapply(.SD, function(x) sum(x)), by = by_vars, .SDcols = draws]
  #sum across by_vars to get frequency of symptoms for all cases, summing the frequency of symptoms for each category weighted by proportion of total cases in that category, now draws for all groups defined by by_vars are equal, but all rows for the group remain in the table 
  beta_rows[, c("row_count", "non_cases") := NULL]
  #drop some columns you don't need anymore
  return(beta_rows)
}

reformat <- function(dt){
  
  unique_data <- unique(dt, by = by_vars)
  #drop rows for different frequency categories for same by_var group
  long_dt <- melt(unique_data, measure.vars = draws, variable.name = "draw")
  #reshape to have each draw on its own row
  
  dt <- copy(long_dt)
  sum_vars <- c("nid", "sex", "age_start", "age_end", "case_name")
 
  dt[, mean_freq := mean(value), by = sum_vars]
  dt[, sd_freq := sd(value), by = sum_vars]
  #get mean and sd of the 1000 draws for a single sum_var combination 
  
  dt <- unique(dt, by = sum_vars)
  dt <- dt[, c("draw") := NULL]
  #drop all but one draw for each sum_var combination, since the mean and sd for the group is the same for all draw rows
  #drop the column telling us which draw we kept, because we don't care, we care about the mean and SD summarizing all draws for the sum_var group (which is amount of time symptomatic for a group of cases for which we had exhaustive information on frequency category)
  
  return(dt)
  
}

meta_analysis <- function(dt){
  dt <- copy(dt)
  meta_freq <- rma(yi = dt$mean_freq, sei = dt$sd_freq)
  results <- data.table(meta_mean = as.numeric(meta_freq$beta), meta_se = as.numeric(meta_freq$se), meta_n = nrow(dt))
  return(results)
}

calculate_split <- function(dt){
  results <- copy(dt)
  results[!is.na(meta_mean), prop_sx_days := meta_mean/7]
  results[!is.na(meta_mean), prop_sx_days_se := meta_se/7]
  results[!is.na(meta_mean), prop_sx_days_lower := prop_sx_days-2*prop_sx_days_se]
  results[!is.na(meta_mean), prop_sx_days_upper := prop_sx_days+2*prop_sx_days_se]
  return(results)
}

##doing the things
in_data <- as.data.table(read_excel(input_path))
#get the data as a table
dt <- copy(in_data)
#make a copy
dt <- dt[!is.na(nid),] 
#drop empty rows
dt <- dt[!group_review == 0]
#drop if group-reviewed out
dt[is.na(cases_total), cases_total := sum(cases), by = by_vars]
#calculate total cases if not already in the extraction

weighted_sum_draws <- beta_dis(dt)
formatted <-reformat(weighted_sum_draws) 
results <- meta_analysis(formatted)
results <- calculate_split(results)
write.csv(results, output_path, row.names = F)