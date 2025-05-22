################################################################################
## NTDs Dengue
## Alternative to code in step_4_sequela_split_child.py
## Description: apply proportional sequela splits based on coeffs and apply duration 
## for prevalence, save NF

################################################################################
rm(list = ls())
user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH")
#install.packages("BayesianTools", lib=path)
#library(BayesianTools, lib.loc=path)
library(dplyr)

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")

### functions
# Create vector n=1000 of beta distribution
custom_beta <- function(u, s) {
  a <- u * (u - u^2 - s^2) / s^2
  b <- a * (1 - u) / u
  dist <- rbeta(1000, a, b)
  return(dist)
}
# Correct Af/Bf probabilities to be a even split
apply_prob_correction <- function(p_Af, p_Bf) {
  correction <- p_Af + p_Bf
  p_Af <- p_Af / correction
  p_Bf <- p_Bf / correction
  return(list(p_Af, p_Bf))
}
# Apply Probabilities to Base Draws
apply_prob <- function(dt, prob) {
  # Ensure dt is a data.table
  # setDT(dt)
  final_dt <- copy(dt)
  # dt <- subset(dt, location_id == 4736 & year_id ==2020 & age_group_id ==10)
  prob <- copy(prob)
  # Identify columns that contain 'draw'
  draw_cols <- grep("draw", names(final_dt), value = TRUE)
  
  # Apply the probability adjustment to each 'draw' column
  for (col in draw_cols) {
    #col <- draw_cols[1]
    i <- as.numeric(gsub("draw_", "", col))+1
    final_dt[, (col) := get(col) * prob[i]]
  }
  
  
  return(final_dt)
}
apply_duration <- function(df, duration) {
  # Copy the dataframe
  prev <- copy(df)
  
  # Identify columns that contain 'draw'
  draw_cols <- grep("draw", names(df), value = TRUE)
  
  # Multiply draw columns by duration
  #prev[draw_cols] <- prev[draw_cols] * duration
  for (col in draw_cols) {
    prev[, (col) := get(col) * duration]
  }
  
  # Set measure_id to 5
  prev$measure_id <- 5
  
  # Append prev to df
  df_final <- rbind(df, prev)
  
  return(df_final)
}
# apply correction to prev >1
correct_prev_1 <- function(df) {
  # Copy the dataframe
  prev <- copy(df)
  # Identify columns that contain 'draw'
  draw_cols <- grep("draw", names(df), value = TRUE)
  
  for (col in draw_cols) {
    
    prev[get(col) > 1 & measure_id ==5, (col) := 1]
  }
  return(prev)
}
################################################################################
# paths and vars
release_id <- ADDRESS
date <- Sys.Date()
stgpr_id <- ADDRESS
inc_model_id <- ADDRESS
yearsgbd <- c(ADDRESS)
cw_id <- ADDRESS
run_date <- "ADDRESS"
desc <- "ADDRESS"

data_root <- paste0("FILEPATH")
# saving out seq 
output_dir <- paste0(data_root,'FILEPATH')

dir.create(paste0(output_dir,"FILEPATH"), recursive = T, showWarnings = FALSE)
dir.create(paste0(output_dir,"FILEPATH"), recursive = T, showWarnings = FALSE)
dir.create(paste0(output_dir,"FILEPATH"), recursive = T, showWarnings = FALSE)

################################################################################
# pull locations
d_locs<-get_location_metadata(release_id =release_id,location_set_id = ADDRESS)
d_locs<-d_locs[d_locs$is_estimate==1,]

# all locations
location_list<-unique(d_locs$location_id)

# pull final results to begin sequela processing
df <- get_draws("modelable_entity_id", ADDRESS, "ADDRESS", location_id=location_list, 
               release_id = relesase_id, version_id=inc_model_id, year_id =yearsgbd)

# Drop the column "model_version_id"
df$model_version_id <- NULL

# Add a new column "metric_id" with all values set to ADDRESS
df$metric_id <- ADDRESS
df2 <- copy(df)

# Apply each meid params
p_Af <- custom_beta(u=0.945, s=0.074)
p_Bf <- custom_beta(u=0.055, s=0.00765)
p_Cf <- custom_beta(u=0.084, s=0.02)

# apply probability correction
correction_result <- apply_prob_correction(p_Af, p_Bf)
p_Af <- correction_result[[1]]
p_Bf <- correction_result[[2]]

# Apply probability
df_Af <- apply_prob(df2, p_Af)
df_Bf <- apply_prob(df2, p_Bf)
df_Cf <- apply_prob(df2, p_Cf)

## Adjust incidence to prevalence w/ durations
# Source of durations: Whitehead et al, doi: 10.1038/nrmicro1690
df_Af <- apply_duration(df_Af, 6/365)
df_Bf <- apply_duration(df_Bf, 14/365)
df_Cf <- apply_duration(df_Cf, 0.5)

# set me_id
df_Af$modelable_entity_id <- Af
df_Bf$modelable_entity_id <- Bf
df_Cf$modelable_entity_id <- Cf

# apply prev correction
df_Af <- correct_prev_1(df_Af)
df_Bf <- correct_prev_1(df_Bf)
df_Cf <- correct_prev_1(df_Cf)

################################################################################
## write out results
# Saves (ADDRESS is prevalence only)
unique_d_locations <- unique(location_list)
for(i in unique_d_locations){
  # subset to single loc and spit out
  upload_file<- subset(df_Af, location_id==i)
  fwrite(upload_file,(paste0(output_dir, "FILEPATH")))
}

for(i in unique_d_locations){
  # subset to single loc and spit out
  upload_file<- subset(df_Bf, location_id==i)
  fwrite(upload_file,(paste0(output_dir, "FILEPATH")))
}

for(i in unique_d_locations){
  # subset to single loc and spit out
  upload_file<- subset(df_Cf, location_id==i & measure_id ==ADDRESS)
  fwrite(upload_file,(paste0(output_dir, "FILEPATH")))
}

################################################################################
# ### Save

save_results_epi(input_dir =paste0(output_dir, "FILEPATH"),
                 input_file_pattern = "FILEPATH",
                 modelable_entity_id = ADDRESS,
                 description = desc,
                 measure_id = ADDRESS,
                 release_id = release_id,
                 bundle_id=ADDRESS,
                 crosswalk_version_id=cw_id,
                 year_id = yearsgbd,
                 mark_best = TRUE
)


save_results_epi(input_dir =paste0(output_dir, "FILEPATH"),
                 input_file_pattern = "FILEPATH",
                 modelable_entity_id = ADDRESS,
                 description = desc,
                 measure_id = ADDRESS,
                 release_id = release_id,
                 bundle_id=ADDRESS,
                 crosswalk_version_id=cw_id,
                 year_id = yearsgbd,
                 mark_best = TRUE
)


save_results_epi(input_dir =paste0(output_dir, "FILEPATH"),
                 input_file_pattern = "FILEPATH",
                 modelable_entity_id = ADDRESS,
                 description = desc,
                 measure_id = ADDRESS,
                 release_id = release_id,
                 bundle_id=ADDRESS,
                 crosswalk_version_id=cw_id,
                 year_id = yearsgbd,
                 mark_best = TRUE
)
################################################################################