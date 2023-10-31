## ******************************************************************************
##
## Purpose: Custom processing of individual-level survey data before collapsing 
##          into aggregated data
## Input:   Directory path containing extracted survey data, directory path where
##          you want to save processed survey data
## Output:  Processed individual-level survey data (microdata)
## Created: 2020-1-15
## Last updated: 2020-1-22
##
## ******************************************************************************

# NEW VERSION of this script
# Read in all the extracted files from the pop/fert directory, check for existence of needed variables,
# calculate variables, save to a processed folder

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

pacman::p_load(data.table, readstata13)

#birth recode version 
input_directory <- 'PATHNAME'
output_directory <- 'PATHNAME'

#loop through every filename in the input directory
directory.list.all <- list.files(input_directory, full.names = F)

for (directory_name in directory.list.all) {
  #loop through every filename in the input directory
  file.list.all <- list.files(paste0(input_directory,directory_name), full.names = F)
  file.list.all <- file.list.all[grepl('.dta',file.list.all)]
  file.list.all <- file.list.all[grepl('DHS',file.list.all) == FALSE]
  file.list.all <- file.list.all[grepl('MIS',file.list.all) == FALSE]

  for (file_name in file.list.all) {

    #read in the downloaded survey
    df <- data.table(read.dta13(file = paste0(input_directory,
                                              directory_name, '/',file_name)))
    
    # Check that necessary columns exist
    required_cols <- c('child_dob_cmc','interview_date_cmc','mother_id','mother_dob_cmc',
                       'survey_module', 'strata', 'psu','hh_id','mother_age_years')
    check <- data.table(required = required_cols, present = 0)
    check[required %in% names(df), present := 1]

    if (sum(check$present) < length(required_cols)) {
      print(paste0(file_name, ' is missing these columns: ',paste(check[present == 0, required])))
      next
    }

    #drop any rows where the interview date isn't filled out (should never happen)
    df <- df[!is.na(interview_date_cmc)]
    df <- df[!is.na(strata)]
    df <- df[!is.na(psu)]
    df <- df[!is.na(hh_id)]
    df <- df[!is.na(mother_id)]
    df <- df[!is.na(mother_dob_cmc)]
    df <- df[!is.na(child_dob_cmc)]
    df <- df[!is.na(mother_age_years)]

    #drop any rows where the date of birth is later than the interview data - error
    df <- df[(interview_date_cmc - child_dob_cmc) >= 0]

    #drop any mothers who have never given birth
    df <- df[!is.na(child_id)]

    # Calculate birth order
    df[, birth_order_seq := frank(child_dob_cmc, ties.method = "random"), by = .(strata, psu, mother_id, hh_id)]

    df[birth_order_seq == 1, not_firstborn := 0]
    df[birth_order_seq > 1, not_firstborn := 1]

    # Drop births greater than 5 years (60 months) before the survey
    df <- df[(interview_date_cmc - child_dob_cmc) < 60]

    # Calculate birth year from the Century-Month-Code
    df[, cv_birth_year := 1900 + floor((child_dob_cmc-1)/12)]

    if (nrow(df) > 0) {
      
      # if ('child_sex' %in% names(df)) {
      #   print(paste0('nid ', df[1,nid], ' has child_sex column with values ', unique(df$child_sex)))
      # }
      
      save.dta13(data = df, file = paste0(output_directory, file_name))
    }

  }
}


#child recode version
input_directory <- "PATHNAME"
output_directory <- "PATHNAME"

#loop through every filename in the input directory
file.list.all <- list.files(paste0(input_directory), full.names = F)
file.list.all <- file.list.all[grepl('.dta',file.list.all)]

for (file_name in file.list.all) {
  
  #read in the downloaded survey
  df <- data.table(read.dta13(file = paste0(input_directory,file_name)))
  
  # Check that necessary columns exist
  required_cols <- c('order_of_birth','int_year','int_month','age_month')
  check <- data.table(required = required_cols, present = 0)
  check[required %in% names(df), present := 1]
  
  if (sum(check$present) < length(required_cols)) {
    print(paste0(file_name, ' is missing these columns: ',paste(check[present == 0, required])))
    next
  }
  
  #drop any rows where the interview date isn't filled out (should never happen)
  df <- df[!is.na(order_of_birth)]
  df <- df[!is.na(int_year)]
  df <- df[!is.na(int_month)]
  df <- df[!is.na(age_month)]
  
  #create new binary variable: firstborn vs not firstborn
  df[order_of_birth != 1, not_firstborn := 1]
  df[order_of_birth == 1, not_firstborn := 0]
  
  #calculate birth year
  df[, interview_date_cmc := 12*((floor(int_year)) - 1900)+int_month]
  df[, child_dob_cmc := interview_date_cmc - age_month]
  df[, cv_birth_year := 1900 + floor((child_dob_cmc-1)/12)]
  
  # Drop births greater than 5 years (60 months) before the survey
  df <- df[(interview_date_cmc - child_dob_cmc) < 60]
  
  # Code maternal age group as well as direct age, in case I decide to use later
  #df[, mother_age_year_start := floor(mother_age_years)]
  
  save.dta13(data = df, file = paste0(output_directory, file_name))
  
}  







#check on a problematic file
dt <- as.data.table(read.dta13(file = paste0(output_directory, file_name)))
dt <- as.data.table(read.dta13(file = 'PATHNAME'))

# OTHER NOTES:

#checking current data
bun_data <- fread("PATHNAME")
bun_data <- bun_data[, .(nid, location_id, location_name, year_start, year_end, sex, age_start, age_end, field_citation_value)]
#bun_data <- bun_data[, .N, by = .(nid, location_id, location_name, field_citation_value)]
#82 surveys in the bundle as of GBD2019

source_list <- fread(paste0("PATHNAME"))
setnames(source_list, 'in bundle already?', 'current_bundle')
setnames(source_list, 'geography', 'location_name')
#305 DHS surveys currently in Cooper (12/5/19)

source_list_new <- merge(source_list, bun_data, by = 'nid', all.x = TRUE)
#30 DHS surveys from Cooper are in the bundle
#275 surveys are not in the bundle

# dt <- get_bundle_data(bundle_id = 6686, decomp_step = 'iterative', export = TRUE)
# validate_input_sheet(bundle_id = 498, 
#                      filepath = "PATHNAME",
#                      error_log_path = "PATHNAME")
# upload_bundle_data(bundle_id = 498, decomp_step = 'iterative', filepath = "PATHNAME")

