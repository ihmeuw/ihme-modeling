###################################################################
## Purpose: Scale Amount Consumed Estimates to Supply-Side Envelope
###################################################################

# Source libraries
source(FILEPATH)
library(tidyverse)
library(data.table)

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
release <- ifelse(!is.na(args[1]), args[1], 16) # take location_id
max_year <- args[2]
output_path <- ifelse(!is.na(args[3]), args[3], FILEPATH)
amt_id <- ifelse(!is.na(args[4]), args[4], 217797) # amount_smoked run ID 138929 148856
current_smok_id <- ifelse(!is.na(args[5]), args[5], 217768) # current smoking prevalence run ID 138941, 190167
supply_path <- ifelse(!is.na(args[6]), args[6], FILEPATH)
param_location <- args[7]

if (F){
  release <- 16
  max_year <- 2024
  output_path <- FILEPATH
  supply_path <- FILEPATH
  amt_id <- 217797
  current_smok_id <- 217769
  param_location <- FILEPATH
  task_id <- 563
}

param_map <- fread(param_location)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(task_id)
f <- param_map[task_id, location_id]
l <- paste0(f)

# supply side: 1) st-gpr, 2) forecast with holt method, 3) split subnational level using daily smoking

# create the output directory:
dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/"), showWarnings = F)

locs <- get_location_metadata(22, release_id = release)

# Set some useful objects
idvars <- c("location_id", "year_id", "age_group_id", "sex_id")
drawvars <- c(paste0("draw_", 0:999))

# Step 1: Generate age-sex pattern

# Pull survey-data derived estimates of amount consumed
asp <- fread(paste0(FILEPATH, amt_id, "/draws_temp_0/", l, ".csv"))
files <- list.files(paste0(FILEPATH, amt_id, "/draws_temp_0/"))

# Pull smoking prevalence draws
prevalence <- fread(paste0(FILEPATH, current_smok_id, "/draws_temp_0/", l, ".csv"))

# Pull populations
pops <- get_population(age_group_id = c(7:20, 30, 31, 32, 235), sex_id = c(1, 2), location_id = l, year_id = c(1960:max_year), release_id = release)[, run_id:=NULL]

# Merge and make a number of smokers df
num_smokers <- merge(prevalence, pops, by = idvars)
num_smokers <- num_smokers[, (drawvars):=lapply(.SD, function(x) x*population), .SDcols=drawvars]

# Now scale up to produce number of cigarettes 
asp <- melt(asp, id.vars = idvars, measure.vars = drawvars)
setnames(asp, "value", "cigperday")

num_smokers <- melt(num_smokers[, population:=NULL], id.vars = idvars, measure.vars = drawvars)
setnames(num_smokers, "value", "num_smokers")

asp <- merge(asp, num_smokers, by = c(idvars, "variable"))
asp <- asp[, cigperday:=cigperday*num_smokers] # get the number of cigarettes smoked by all smokers per day (versus just the number of cigarettes smoked by each smoker)

# Finally, make the proportion
asp <- asp[, prop:=cigperday/sum(cigperday), by = c("location_id", "year_id", "variable")]
# For each location and year, calculate the proportion of cigarettes smoked by each age/sex out of the total number of cigarettes smoked for that loc/year

# Step 2: Split out total envelope of supply-side cig
# Read in supply side data

# Determine the level of the location, and thus figure out what the parent ID is
level_loc <- locs[location_id == l, level]
if (level_loc > 3){
  # country is 4th location after comma split
  # get the country name:
  parent_country <- strsplit(locs[location_id == l, path_to_top_parent], ",")[[1]][4] # Changed from ", " to "," to accommodate how path_to_top_parent works. 2/14/2025
  supply <- fread(paste0(supply_path, parent_country, ".csv"))
  supply <- supply[location_id==l]
} else{
  supply <- fread(paste0(supply_path, l, ".csv"))
  supply <- supply[location_id==l]
}


# Make pop 15+ to calculate total number of cigarettes consumed
pops_above15 <- pops[age_group_id>=7]
pops_above15 <- pops_above15[, population:=sum(population), by = c("location_id", "year_id")] # total population for each loc/year (because supply side estimate are all-age and all-sex)
pops_above15 <- unique(pops_above15[, c("age_group_id", "sex_id"):=NULL])

# Calculate total number of cigarettes
supply <- merge(supply, pops_above15, by = c("location_id", "year_id"))
supply <- supply[, (drawvars):=lapply(.SD, function(x) x*population), .SDcols=drawvars] # multiply each tobacco consumption draw by population to get the total number of cigarettes smoked for each loc/year according to the supply side estimates
supply <- melt(supply, id.vars = c("location_id", "year_id"), measure.vars = drawvars)
setnames(supply, "value", "cig_envelope") # cig envelope is the number of cigarettes smoked per loc/year according to the supply side estimates


# Split out the cigarettes
out <- merge(asp, supply, by = c("location_id", "year_id", "variable"))
out <- out[, amt_consumed:=((cig_envelope*prop)/num_smokers)/365]
# total num cigs for each loc/year * proportion of those cigs smoked by each age/sex for that loc/year / num smokers for each loc/year/age/sex -> number of cigs smoked by age/sex/loc/year divided by num smokers -> number cigs per smoker for each loc/year/age/sex
# divided by 365
out <- out[, orig:=cigperday/num_smokers]

# Save summaries
summaries <- copy(out)
setnames(summaries, "variable", "draw")
summaries <- melt(summaries, id.vars = c("draw", "location_id", "year_id", "age_group_id", "sex_id"), measure.vars = c("num_smokers", "prop", "cig_envelope", "amt_consumed", "orig"))
var = "value"
summaries <- summaries[, c("mean", "lower", "upper"):=as.list(c(mean(get(var)), quantile(get(var), c(0.025, 0.975), na.rm=T) )), by=c(idvars, "variable")]
summaries <- unique(summaries[, .(location_id, year_id, age_group_id, sex_id, mean, lower, upper, variable)])

dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/summaries/"), showWarnings = F)
write.csv(summaries, paste0(FILEPATH, "/scaled_supply_side_draws/summaries/", l, ".csv"), na = "", row.names = F)

# Save draws
out <- out[, .(location_id, year_id, age_group_id, sex_id, amt_consumed, variable)]
out <- dcast(out, location_id + year_id + age_group_id + sex_id ~ variable, value.var = "amt_consumed")

dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/draws/"), showWarnings = F)
write.csv(out, paste0(FILEPATH, "/scaled_supply_side_draws/draws/", l, ".csv"), na = "", row.names = F)
message("SAVED SUCCESSFULLY!")
