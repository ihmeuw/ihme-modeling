## the purpose of this script is to read in the draws from ST-GPR and use them to calculate the ratio between chew and non chew,
## and then calculate mean/standard deviation/variance

# load libraries
library(data.table)
library(ggplot2)

# read in the relevant run IDs from ST-GPR
args <- commandArgs(trailingOnly = TRUE)
run_chew <- ifelse(!is.na(args[1]),args[1], 'RUNID') # chewing tobacco model run ID
run_non_chew <- ifelse(!is.na(args[2]),args[2], 'RUNID') # non-chewing tobacco model run ID
# location of interest (this script is run in parallel for each location)
loc <- ifelse(!is.na(args[3]),args[3], 'LOCATIONID') 

date <- gsub(" |:","-",Sys.time())

# location where you want to write your outputs
out_root <- 'FILEPATH'

# read in the draws for the chewing tobacco model for the location of interest
draws_chew <- fread('FILEPATH')
# read in the draws for the non-chewing tobacco model for the location of interest
draws_non_chew <- fread('FILEPATH')

draws_chew[,model := "chew"]
draws_non_chew[,model := "non_chew"]

total_draws <- rbind(draws_chew,draws_non_chew)


# sum
sum_draws <- paste0("sum_",c(0:999))
vars <- paste0("draw_",c(0:999))
# sum draws across models
total_draws[ , (sum_draws) :=  lapply(.SD, function(x) sum(x)), .SDcols=vars, by=c("location_id", "year_id", "age_group_id", "sex_id")]

# subset to only the chewing data:
total_draws <- total_draws[model == "chew"]

# create the ratio
# create a dataset with just the summed draws
keep_sum <- c("location_id","age_group_id","sex_id","year_id",sum_draws)
total_draws_sum <- total_draws[,(keep_sum),with=F]
total_draws_sum$version <- "sum"
# change colnames
setnames(total_draws_sum, old = sum_draws, new = vars)

# create a dataset with just the raw chewing tobacco draws
keep_raw <- c("location_id","age_group_id","sex_id","year_id",vars)
total_draws_raw <- total_draws[,(keep_raw),with=F]
total_draws_raw$version <- "raw"

# combine draws length-wise
total_draws_long <- rbind(total_draws_sum,total_draws_raw)

# then melt:
total_draws_melt <- melt.data.table(total_draws_long,measure.vars = vars)
# then dcast
total_draws_wide <- dcast.data.table(total_draws_melt, location_id + age_group_id + sex_id + year_id + variable ~ version, value.var = "value")

# then divide to create the ratio
total_draws_wide[,ratio := raw / sum]

# take mean and standard deviation over the draws of ratios:
total_draws_wide[, mean_ratio := mean(ratio), by=c("location_id","year_id","sex_id","age_group_id")]
total_draws_wide[, se_ratio := sd(ratio), by=c("location_id","year_id","sex_id","age_group_id")]

total_draws_wide_avg <- unique(total_draws_wide[,.(location_id,year_id,age_group_id,sex_id,mean_ratio,se_ratio)])

# save one file per location. These are read in by the next script, which adjusts the smokeless tobacco data points using the ratios.
write.csv(total_draws_wide_avg,paste0(out_root,loc,".csv"))
  


