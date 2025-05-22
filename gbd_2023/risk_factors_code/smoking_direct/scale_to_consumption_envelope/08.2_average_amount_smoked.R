## Purpose: Average the scaled supply side values with the amount smoked ST-GPR model to get a final measure of cigarettes per smoker per day

###################################################################

# Source libraries
source(FILEPATH)
library(tidyverse)
library(data.table)
# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
output_path <- ifelse(!is.na(args[1]), args[1], FILEPATH)
amt_id <- ifelse(!is.na(args[2]),args[2], 217797) # amount_smoked run ID
param_location <- args[3]

param_map <- fread(param_location)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(task_id)
f <- param_map[task_id, location_id]
l <- paste0(f)

message(l)
message(output_path)
message(amt_id)

# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:999))


# pull survey-level amount smoked
asp <- fread(paste0(FILEPATH, amt_id, "/draws_temp_0/", l, ".csv"))

# pull in the scaled supply side estimates
scaled_draws <- fread(FILEPATH, "/scaled_supply_side_draws/draws/", l, ".csv"))

# melt amount smoked and scaled values
asp <- melt.data.table(asp, measure.vars = drawvars, value.name = "amt_smoked")
scaled_draws <- melt.data.table(scaled_draws, measure.vars = drawvars, value.name = "supply")


all_amt <- merge(asp, scaled_draws, by=c("location_id","year_id","sex_id","age_group_id","variable"))


# try ordering the draws:
if (F){
  all_amt_smoked <- all_amt[,c("location_id","year_id","sex_id","age_group_id","amt_smoked")]
  all_amt_supply <- all_amt[,c("location_id","year_id","sex_id","age_group_id","supply")]
  
  
  setorderv(all_amt_smoked,c("location_id","year_id","sex_id","age_group_id","amt_smoked"),1)
  setorderv(all_amt_supply,c("location_id","year_id","sex_id","age_group_id","supply"),1)
  
  all_amt_supply <- all_amt_supply[,.(supply)]
  
  # then bind back together:
  all_amt <- cbind(all_amt_smoked,all_amt_supply)
}

# average the draws
all_amt[, average_amt := (amt_smoked + supply)/2]

if(F){
  ggplot(all_amt[year_id==1960 & age_group_id == 15 & sex_id == 1],aes(average_amt))+geom_histogram()
  mean(all_amt[year_id==1960 & age_group_id == 15 & sex_id == 1, supply])
  mean(all_amt[year_id==1960 & age_group_id == 15 & sex_id == 1, amt_smoked])
}


# write out draws
all_amt <- all_amt[,.(location_id, year_id, age_group_id, sex_id, average_amt, variable)]
all_amt <- dcast(all_amt, location_id + year_id + age_group_id + sex_id ~ variable, value.var = "average_amt")
dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/averaged/"), showWarnings = F)
dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/averaged/draws/"), showWarnings = F)
write.csv(all_amt, paste0(FILEPATH, "/scaled_supply_side_draws/averaged/draws/", l, ".csv"), na = "", row.names = F)

vars <- paste0("draw_", seq(0, 999, 1))
all_amt <- all_amt[, mean:=rowMeans(.SD), .SD=vars]
all_amt <- all_amt[, lower:=apply(.SD, 1, quantile, c(.025), na.rm=T), .SDcols=vars]
all_amt <- all_amt[, upper:=apply(.SD, 1, quantile, c(.975), na.rm=T), .SDcols=vars]
avg_files_unique <- unique(all_amt[,.(location_id, sex_id, age_group_id, year_id, mean, lower, upper)])

dir.create(paste0(FILEPATH, "/scaled_supply_side_draws/averaged/summaries/"), showWarnings = F)
write.csv(avg_files_unique,paste0(FILEPATH, "/scaled_supply_side_draws/averaged/summaries/", l, ".csv"), row.names=F)
message("SAVED SUCCESSFULLY!")