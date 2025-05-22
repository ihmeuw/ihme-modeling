## HEADER #################################################################
# Purpose: The purpose of this script is to read in the draws from ST-GPR and use them to calculate the ratio between chew and non chew, and then calculate mean/standard deviation/variance
#          

## SET-UP #################################################################
# Load packages, and install if missing ========================================================================
library(data.table)
library(ggplot2)

# Load files/arguments and source code =========================================================================
args <- commandArgs(trailingOnly = TRUE)
run_chew <- ifelse(!is.na(args[1]),args[1], "ID") # take chewing run ID
run_non_chew <- ifelse(!is.na(args[2]),args[2], "ID") # take non_chewing run ID
date <- ifelse(!is.na(args[3]),args[3], "no_date")
draws <- ifelse(!is.na(args[4]), args[4], 1000)
out_root <- ifelse(!is.na(args[5]), args[5], "FILEPATH")
array <- ifelse(!is.na(args[6]), args[6], TRUE)
param_location <- ifelse(!is.na(args[7]), args[7], "none_provided")

## SCRIPT ##################################################################
if(array == FALSE) {
  c.data <- commandArgs(trailingOnly = T)[5]
  print(c.data)
}
if (array == TRUE) {
  param_map <- fread(param_location)
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  print(task_id)
  f <- param_map[task_id, location_id]
  loc <- paste0(f)
  print(loc)
}

# we will eventually need to do this at the draw level so that we can get the correct uncertainty

stgpr_output <- "FILEPATH"

draws_chew <- fread(paste0("FILEPATH","/draws_temp_0/",loc,".csv"))
draws_non_chew <- fread(paste0("FILEPATH","/draws_temp_0/",loc,".csv"))

draws_chew[,model := "chew"]
draws_non_chew[,model := "non_chew"]

total_draws <- rbind(draws_chew,draws_non_chew)

if(draws == 100){
  # sum
  sum_draws <- paste0("sum_",c(0:99))
  vars <- paste0("draw_",c(0:99))
} else {
  # sum
  sum_draws <- paste0("sum_",c(0:999))
  vars <- paste0("draw_",c(0:999))
}
# sum draws across models
total_draws[ , (sum_draws) :=  lapply(.SD, function(x) sum(x)), .SDcols=vars, by=c("location_id", "year_id", "age_group_id", "sex_id")]

# subset to only the chewing data:
total_draws <- total_draws[model == "chew"]

# create the ratio
keep_sum <- c("location_id","age_group_id","sex_id","year_id",sum_draws)
total_draws_sum <- total_draws[,(keep_sum),with=F]
total_draws_sum$version <- "sum"
# change colnames
setnames(total_draws_sum, old = sum_draws, new = vars)

keep_raw <- c("location_id","age_group_id","sex_id","year_id",vars)
total_draws_raw <- total_draws[,(keep_raw),with=F]
total_draws_raw$version <- "raw"

total_draws_long <- rbind(total_draws_sum,total_draws_raw)

# then melt:
total_draws_melt <- melt.data.table(total_draws_long,measure.vars = vars)
# then dcast
total_draws_wide <- dcast.data.table(total_draws_melt, location_id + age_group_id + sex_id + year_id + variable ~ version, value.var = "value")

# then divide!
total_draws_wide[,ratio := raw / sum]

# take mean and standard deviation:
total_draws_wide[, mean_ratio := mean(ratio), by=c("location_id","year_id","sex_id","age_group_id")]
total_draws_wide[, se_ratio := sd(ratio), by=c("location_id","year_id","sex_id","age_group_id")]

total_draws_wide_avg <- unique(total_draws_wide[,.(location_id,year_id,age_group_id,sex_id,mean_ratio,se_ratio)])

write.csv(total_draws_wide_avg,paste0("FILEPATH",".csv"))


