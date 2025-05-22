################################################################################
## DESCRIPTION: Applies age and sex pattern to bundle version data
## INPUTS: bundle version data ##
## OUTPUTS: bundle version data with applied age-sex pattern ##
## AUTHOR:
## DATE:
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


# Base filepaths
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- paste0(code_dir, "FILEPATH")

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))

# Set arguments
args <- commandArgs(trailingOnly = TRUE)
data_dir <- args[1]
lit_overweight_file <- args[2]
lit_obese_file <- args[3]
lit_underweight_file <- args[4]
lit_severe_underweight_file <- args[5]

# Load helper datasets
locs <- get_location_metadata(22, decomp_step = "iterative")
ages <- fread(paste0(data_dir, "FILEPATH"))
pops <- readRDS(paste0(data_dir, "FILEPATH")) %>% as.data.table(.) 
pops[age_start == 80, age_end := 125] # Need this for later

## logit conversion calculations from crosswalk functions
logit <- function(p) log(p/(1-p))
inv_logit <- function(mean) 1/(1+exp(-mean))

offset_val <- 1e-05

# Read in tracking sheet
tracking <- fread("FILEPATH/data_tracker.csv")

# Define age-sex split function
age_sex_split <- function(orig_data, as_p_df, tert_map_df){
  
  # Make copies of data
  df <- copy(orig_data)
  as_p <- copy(as_p_df)
  tert_map <- copy(tert_map_df)
  
  # Delete some seqs
  df$crosswalk_parent_seq <- NA
  df$crosswalk_parent_seq <- as.integer(df$crosswalk_parent_seq)
  # Save the seqs coming in to make sure nothing is dropped
  df$seq_orig <- df$seq
  df[,seq := 1:.N]
  seq_in <- unique(df$seq)
  
  # Generate unique ID for easy merging
  df[,split_id := 1:.N]
  
  # Generate sex_id
  if(!"sex_id" %in% names(df)){
    df[sex == "Male", sex_id := 1]
    df[sex == "Female", sex_id := 2]
    df[sex == "Both", sex_id := 3]
  }
  
  # To avoid unneccessary age-sex splitting, if the orig_age_end and orig_age_start are in GBD age bins,
  # assign orig_age_end to age_end instead of age_end being orig_age_end
  if(is.character(df$seq)) df[, seq := as.integer(seq)]
  df[orig_age_end == orig_age_start + 4 & orig_age_start %% 5 == 0, `:=` (age_start = orig_age_start, age_end = orig_age_end + 1, crosswalk_parent_seq = seq)]
  
  # Since standard GBD age groups changed from 1-4 to 2-4 now, just going to assign age_start = 2 and age_end = 5 for old data
  # NEEDS ATTENTION
  df[(orig_age_start == 0 & orig_age_end %in% c(4:5)) | 
       (orig_age_start == 1 & orig_age_end == 4) | 
       (orig_age_start == 2 & orig_age_end == 4), 
     `:=` (age_start = 2, age_end = 5, crosswalk_parent_seq = seq)]
  
  # Since we model 80+ as 1 age group, set all the age_start = 80 and age_end = 125 if orig_age_start >= 80
  # df[orig_age_start >= 80, `:=` (age_start = 80, age_end = 125, crosswalk_parent_seq = seq)]
  df[orig_age_start >= 95, `:=` (age_start = 95, age_end = 125, crosswalk_parent_seq = seq)]
  
  # Since upload required an age group ID, several rows are tagged with age_group_id = 99, so the uploader transformed age_start and age_end to 999
  # Fixing this here so that age_start = orig_age_start and age_end = orig_age_end
  df[age_start == 999 & age_end == 999, `:=` (age_start = orig_age_start, age_end = orig_age_end + 1, crosswalk_parent_seq = seq)]
  
  # Split the data
  
  # 1) Good data: These data are in standard GBD age bins and sex specific
  good_data <- df[sex_id %in% c(1,2)][(age_end == age_start + 5 & age_start %% 5 == 0) | (age_start == 2 & age_end == 5) | (age_start == 95 & age_end == 125)] #age_start == 80 & age_end == 125
  good_data$age_group_id <- NULL
  good_data <- merge(good_data, ages[,.(age_start, age_end, age_group_id)], by = c("age_start", "age_end"), all.x = T)
  if (nrow(good_data[is.na(age_group_id)]) != 0) { 
    stop(message(paste0("There are ", 
                        nrow(good_data[is.na(age_group_id)]),
                        " rows of data with NAs in age group ID even though age start and end are in 5 year age bins")))
  }
  print(paste0("There are ", nrow(good_data), " rows of data in standard GBD age cuts and sex specific."))
  
  # 2) Data that we will not age split since the age_end is less than or equal to 15 
  bad_data <- df[age_end <=15 & (!split_id %in% unique(good_data$split_id))]
  print(paste0("There are ", nrow(bad_data), " rows of data that are not going to be split."))
  
  # 3) Both sex data: These data are in the bundle as both sex
  bs_data <- df[sex_id == 3 & !split_id %in% unique(bad_data$split_id)]
  print(paste0("There are ", nrow(bs_data), " rows of both sex data."))
  
  # 4) Sex specific, non-standard GBD age bins that will be split (holding off on under 5 data for now)
  ns_age_data <- df[!split_id %in% c(unique(good_data$split_id), unique(bs_data$split_id), unique(bad_data$split_id))]
  print(paste0("There are ", nrow(ns_age_data), " rows of data in non-standard GBD age cuts and sex-specific."))
  
  
  if(nrow(good_data) + nrow(bs_data) + nrow(ns_age_data) + nrow(bad_data) != length(seq_in)){
    stop(message("Something went wrong with dividing the data."))
  }
  
  ############################################################
  # Apply age pattern to sex-specific data first
  ############################################################
  
  # Specify columns to keep to keep the processing simpler
  cols <- c("seq", "crosswalk_parent_seq", "location_id", "year_id", "age_start", "age_end", 
            "sex_id", "val", "sample_size", "variance", "age_group_id")
  meta.cols <- setdiff(names(df), cols)
  
  # Metadata and age split data frames
  age_split_metadata <- ns_age_data[, meta.cols, with = F]
  age_split <- ns_age_data[, c("split_id", cols), with = F]
  
  if(nrow(age_split)>0){
    # First things first, set crosswalk_parent_seq to seq and set seq to NULL
    age_split[, crosswalk_parent_seq := seq][, seq := NA]
    
    # Round age groups to the nearest 5 year boundary 
    age_split[, age_start := ifelse(age_start >= 15, round_any(age_start, 5, floor), 15)] # Not relying on any under 15 data to age split
    age_split[, age_end := ifelse(age_start >= 15, round_any(age_end, 5, ceiling), 15)] # Not relying on any under 15 data to age split
    
    # For age end greater than 80, set age_end to 125
    age_split[age_end > 95, age_end := 125]
    
    # Set up rows for splitting
    age_split[age_end != 125, n.age := (age_end - age_start)/5][age_end == 125, n.age := (95 - age_start)/5 + 1] 
    expanded <- rep(age_split$split_id, age_split$n.age) %>% data.table("split_id" = .)
    
    age_split <- merge(expanded, age_split, by = "split_id", all = T)
    age_split[, age.rep := 1:.N - 1, by =.(split_id)]
    age_split[, age_start := age_start + age.rep*5]
    age_split[age_start != 95, age_end := age_start + 5][age_start == 95, age_end := 125]
    
    # Merge on population and calculate sum of population
    age_split[,age_group_id := NULL]
    age_split <- merge(age_split, pops, by = c("location_id", "year_id", "sex_id", "age_start", "age_end"), all.x = T) # will have age_group_id
    if(any(is.na(age_split$population))) stop("Missing populations after merge. Something likely wrong with groups. Double check")
    age_split[, pop_group := sum(population), by = "split_id"]
    
    # Merge on the age-pattern depending on the location's weight tertile
    setnames(as_p, "weight_tert_val_binned", "rel_est")
    age_split <- merge(age_split, tert_map, by = c("location_id", "sex_id"))
    age_split <- merge(age_split, as_p, by = c("age_group_id", "sex_id", "weight_tert"))
    if(any(is.na(age_split$rel_est))) stop("Missing relative estimate after merge. Something likely wrong with groups. Double check")
    
    # Apply age patterns
    age_split[, R := rel_est * population]
    age_split[, R_group := sum(R), by = "split_id"]
    
    age_split[, val := val * (pop_group/population) * (R/R_group)] 
    age_split[val>1, val := 1-offset_val]
    age_split[, sample_size := round(sample_size * population/pop_group)]
    age_split[, variance := val*(1-val)/sample_size]
    
    # # Drop rows based on the quantiles of the new value
    lower_cut <- quantile(age_split$val, probs = 0.01)
    upper_cut <- quantile(age_split$val, probs = 0.99)
    
    age_split <- age_split[val > lower_cut & val < upper_cut]
    # Make column to identify that these rows were age split
    age_split[, cv_age_split := 1]
    
    age_split <- merge(age_split, age_split_metadata, by = "split_id", all.x = T)
    
    # Only keep the necessary columns
    age_split <- age_split[,c(meta.cols, cols, "cv_age_split"), with = F]
    age_split[, cv_sex_split := 0] # Need this column later
  }
  
  ############################################################
  # Apply age-sex pattern to both-sex data now
  ############################################################
  
  # Metadata and sex split data frames
  sex_split_metadata <- bs_data[, meta.cols, with = F]
  sex_split <- bs_data[, c("split_id", cols), with = F]
  
  # First things first, set crosswalk_parent_seq to seq and seq to NA
  sex_split[, crosswalk_parent_seq := seq][, seq := NA]
  
  if(nrow(sex_split)>0){
    # Round age groups to the nearest 5 year boundary 
    sex_split[, age_start := ifelse(age_start >= 15, round_any(age_start, 5, floor), 15)] # Not relying on any under 15 data to age split
    sex_split[, age_end := ifelse(age_start >= 15, round_any(age_end, 5, ceiling), 20)] # Not relying on any under 15 data to age split
    
    # For age end greater than 80, set age_end to 125
    # sex_split[age_end > 80, age_end := 125]
    sex_split[age_end > 95, age_end := 125]
    
    # Set up rows for splitting
    sex_split[age_end != 125, n.age := (age_end - age_start)/5][age_end == 125, n.age := (95 - age_start)/5 + 1] 
    expanded <- rep(sex_split$split_id, sex_split$n.age) %>% data.table("split_id" = .)
    sex_split <- merge(expanded, sex_split, by = "split_id", all = T)
    sex_split[, age.rep := 1:.N - 1, by = "split_id"]
    sex_split[, age_start := age_start + age.rep*5]
    sex_split[age_start != 95, age_end := age_start + 5][age_start == 95, age_end := 125]
    
    
    sex_split[, n.sex := 2]
    sex_split[, sex_split_id := paste0(split_id, "_", age_start)]
    
    expanded <- rep(sex_split$sex_split_id, sex_split$n.sex) %>% data.table("sex_split_id" = .)
    
    sex_split <- merge(expanded, sex_split, by = "sex_split_id", all = T)
    sex_split[, sex_id := 1:.N, by = sex_split_id]
    
    # Merge on population and calculate sum of population
    sex_split[,age_group_id := NULL]
    sex_split <- merge(sex_split, pops, by = c("location_id", "year_id", "sex_id", "age_start", "age_end"), all.x = T) # will have age_group_id
    if(any(is.na(sex_split$population))) stop("Missing populations after merge. Something likely wrong with groups. Double check")
    
    sex_split[, pop_group := sum(population), by = "split_id"]
    sex_split[, pop_bs := sum(population), by = "sex_split_id"] # Determine if this is necessary
    
    # Merge on the age-pattern depending on the location's weight tertile
    
    sex_split <- merge(sex_split, tert_map, by = c("location_id", "sex_id"))
    sex_split <- merge(sex_split, as_p, by = c("age_group_id", "sex_id", "weight_tert"))
    if(any(is.na(age_split$rel_est))) stop("Missing relative estimate after merge. Something likely wrong with groups. Double check")
    
    # Apply age patterns
    sex_split[, R := rel_est * population]
    sex_split[, R_group := sum(R), by = "split_id"]
    sex_split[, R_bs := sum(R), by = "sex_split_id"] # Determine if this is necessary
    
    
    sex_split[, val := val * (pop_group/population) * (R/R_group)] # To do: currently 29 rows where val > 1 for overweight; 0 rows for obese
    sex_split[val>1, val := 1-offset_val]
    sex_split[, sample_size := round(sample_size * population/pop_group)]
    sex_split[, variance := val*(1-val)/sample_size]
    
    # Drop rows based on the quantiles of the new value
    lower_cut <- quantile(sex_split$val, probs = 0.01)
    upper_cut <- quantile(sex_split$val, probs = 0.99)
    
    sex_split <- sex_split[val > lower_cut & val < upper_cut]
    
    # Make column to identify that these rows were age split and sex split
    sex_split[, cv_age_split := 1][, cv_sex_split := 1]
    
    sex_split <- merge(sex_split, sex_split_metadata, by = "split_id", all.x = T)
    
    # Fix the sex column
    sex_split[,sex := ifelse(sex_id == 1, "Male", "Female")]
    
    # Onyl keep the necessary columns
    sex_split <- sex_split[,c(meta.cols, cols, "cv_age_split", "cv_sex_split"), with = F]
  }
  
  ############################################################
  # Combine good data, age split data, and sex split data
  ############################################################
  # First make cv_age_split and cv_sex_split columns in the good data
  good_data[, `:=` (cv_age_split = 0, cv_sex_split = 0)]
  
  # If crosswalk parent seq is filled in, then make seq column NA in good_data
  good_data[!is.na(crosswalk_parent_seq), seq := NA]
  
  # Set the column order for easier rbinding
  out <- rbind(good_data, age_split, sex_split,fill=T)
  
  out[val==0, val:= val+offset_val]
  out[, variance := val*(1-val)/sample_size]
  
  # Check to make sure all seqs are accounted for and that no new seqs were made
  # There will be some NAs
  if(sum(!c(unique(out$seq), unique(bad_data$seq)) %in% c(seq_in, NA)) != 0){
    stop(message("Oops, looks like you created some new seqs."))
  }
  return(out)
}

for( meas in c("ow","ob","uw", "suw")){
  # Read in data
  if(meas=="ow") df <- read.xlsx(lit_overweight_file) %>% as.data.table()
  if(meas=="ob") df <- read.xlsx(lit_obese_file) %>% as.data.table()
  if(meas=="uw") df <- read.xlsx(lit_underweight_file) %>% as.data.table()
  if(meas=="suw") df <- read.xlsx(lit_severe_underweight_file) %>% as.data.table()

  ## remove report lines that are missing both sex and age
  df <- subset(df, !(age_start==999 & age_end==999 & sex=="Both"))
  
  ## remove report lines that are pre-1980
  df <- subset(df, year_id>=1980)
  
  # Find out the duplicated rows
  print(paste0("There are ", sum(duplicated(df[,c("age_group_id","sex","nid","year_start","location_name","location_id")])), 
               " rows of duplicate data in the overweight files. Removing them should change the number of rows from ",
               nrow(df), " to ", nrow(df)-sum(duplicated(df)), "."))
 
   # Read in age-sex patterns
  # Hard coding age and sex patterns from GBD 2020 for now until new age-sex pattern is developed
  as <- fread(paste0("FILEPATH/tert_",meas,"_as_pattern.csv"))
  # Read in mapping of locations by tertiles
  tert <- fread(paste0("FILEPATH/tert_",meas,"_mapping.csv"))
  
  # Apply the age-sex pattern
  as_split <- age_sex_split(df, as, tert)
  ##Final cleaning
  df_comb[, measure := "proportion"]
  df_comb[val>1, val := 1]
  df_comb[variance<0, variance := 0.02] ## around median
  df_comb[is.infinite(variance) | is.na(variance), variance := 0.2] ## on higher range
  df_comb <- subset(df_comb, !is.na(val))
  
  # Save them
  if(meas=="ow") measure <- "prev_overweight"
  if(meas=="ob") measure <- "prop_obese"
  if(meas=="uw") measure <- "prev_underweight"
  if(meas=="suw")measure <- "prop_severe_underweight"
  
  if(!dir.exists(paste0(data_dir, measure, "/FILEPATH"))){
    dir.create(paste0(data_dir, measure, "/FILEPATH"))
    save_dir <- paste0(data_dir, measure, "/FILEPATH/")
  } else {
    save_dir <- paste0(data_dir, measure, "/FILEPATH/")
  }
  
  fwrite(df_comb, paste0(save_dir, "lit_age_sex_split_", Sys.Date(), ".csv"))
}


