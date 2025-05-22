# Purpose: Third script to convert draw-level all-age/sex LF prevalence estimates from MBG model to age/sex-specific estimates for GBD.

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
user <- Sys.info()[["user"]]
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"
options(max.print=999999)

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- "FILEPATH"
  draws_dir <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir <- "FILEPATH"
}

# 1. Setup libraries, functions, parameters, filepaths, etc. ---------------------------------------------------------------------
# load libraries
library(argparse)

# source functions
source("FILEPATH")

# directories
params_dir <- "FILEPATH"
run_file <- fread("FILEPATH")
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
crosswalks_dir <- "FILEPATH"
inf_draws_dir <- "FILEPATH"

# set parameters
draws <- 1000
release_id <- ADDRESS

# create inverse logit functon to convert from logit to prevalence space
inv.logit <- function(x) {
  exp(x)/(1+exp(x))
}

# parse arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = "168", type = "character")
args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path)

loc  <- params[task_id, location_id]
meid <- params[task_id, meid]

# 2. Adjust all-age/sex data to age/sex-specific estimates and write-out results by location_id ------------------------------------------
# age constraints
## if start age is over 64 (64.5 in original focal 3 script) then just copy prevalence of next lowest age group forward
age_threshold <- 64
## find start age with which to copy prevalence forward past the age threshold (i.e., the highest start age below 64)
age_carry <- 60

# loop function over locations
study_dems <- readRDS("FILEPATH")
yrs   <- study_dems$year_id
ages <- study_dems$age_group_id
sexes <- study_dems$sex_id
yn <- length(yrs)
exp_rows <- length(yrs) * length(ages) * length(sexes) * 1

xwalk_age_pattern_collapsed <- fread("FILEPATH")
pop <- fread("FILEPATH")

# loop through each location to: age & sex-split data, apply age constraints & add the 95+ age group, format columns, & write-out location-specific data as a .csv 
data <- readRDS("FILEPATH")
df_combo <- subset(data, location_id == loc)

# update draw column names to begin with V1 (currently required for split_location_year function)
colnames(df_combo)[3:1002] <- paste0("draw_", 1:1000)

all_years <- data.frame()
for (yn in 1:length(yrs)) {
    
  y <- yrs[yn]
  print(paste0("Running year ", y, " for location ", loc))
    
  r <- split_location_year(all_age_data = df_combo, 
                            pop_data = pop, 
                            age_pattern = xwalk_age_pattern_collapsed, 
                            loc_id = loc,
                            yr = y, 
                            n_draws = draws)
    
  # apply age constraints within each location-year (i.e., only a single set of age groups)
  ## extract row that matches the age group to carry forward and flatten age groups past the upper age threshold
  carry <- r[r$age_group_years_start == age_carry, .SD, .SDcols = paste0("draw_", 1:1000)]
    
  ## age_group_id 235 was not included in crosswalk, so duplicate row here so it will have the upper age threshold carried forward
  add_95 <- data.frame(
      age_group_id = 235,
      age_group_years_start = 95,
      age_group_years_end = 999,
      location_id = loc,
      year = y,
      sex_id = 3,
      population = "")
    
  add_95[8:1007] <- carry
  colnames(add_95) <- names(r)
  r <- rbind(r, add_95)
    
  ## for each row in the location-year (NOTE: here, rows only differ by age grouping)
  for (i in 1:nrow(r)){
      # select row
      row <- r[i,]
      # if the row's start age is great than the upper age threshold
      if (row$age_group_years_start > age_threshold){
        # set aside row's meta-data (location info, age info, year, etc.)
        row_meta <- row[,c(1:7)]
        # bind the draws from from upper age threshold to the row's meta-data
        # replace the new row with the original row
        r[i,] <- cbind(row_meta, carry)
      }
    }
    
  ## if age is under 1 year, assume a prevalence of 0.00001
  r[r$age_group_id %in% c(2,3,4,388,389), .SD := 0.00001, .SDcols = paste0("draw_", 1:1000)]
    
  # sex-splitting -- assuming an equivilent prevalence between males and females
  r$sex_id <- 1
    
  r_f <- copy(r)
  r_f$sex_id <- 2
    
  r_sex_split <- rbind(r,r_f)
    
  if (".SD" %in% colnames(r_sex_split)){
  r_sex_split <- subset(r_sex_split, select = -c(.SD)) 
  }
    
  all_years <- rbind(all_years, r_sex_split)
    
}

# 3. Make sure elimination years are maintained at zero -------------------------------------------
## hard code elimination values for togo, egypt, cambodia, thailand, sri lanka - achieved before 2017
draw.cols <- paste0("draw_", 0:999)
all_years <- setDT(all_years)
all_years[, id := .I]

elim_locs_2010 <- c(141,10,18,17)
elim_locs_2015 <- c(218)

if (loc %in% elim_locs_2010) {
  ### replace prevalence estimates for location_id loc and year_id yr-current with zeros 
  all_years[location_id==loc & year_id >= 2010, (draw.cols):= 0, by=id] 
  # drop id column
  all_years <- subset(all_years, select=-c(id)) 
}

if (loc %in% elim_locs_2015) {
  ### replace prevalence estimates for location_id loc and year_id yr-current with zeros 
  all_years[location_id==loc & year_id >= 2015, (draw.cols):= 0, by=id] 
  # drop id column
  all_years <- subset(all_years, select=-c(id)) 
}

# 4. Format data, export draw files, and save infection prevalence estimates -------------------------------------------
colnames(all_years)[8:1007] <- paste0("draw_", 0:999)
names(all_years)[names(all_years) == "year"] <- "year_id"
all_years$measure_id <- 5
all_years$metric_id <- 3
all_years$modelable_entity_id <- ADDRESS
all_years <- all_years[, c(1008:1010,1:1007)]

# write-out prevalence draws processed for GBD, by location_id
write.csv(all_years, "FILEPATH", row.names = FALSE)
