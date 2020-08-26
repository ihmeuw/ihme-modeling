# this script adjusts the smokeless tobacco data points (post age and sex splitting using the ratios of chewing tobacco divded by the all SLT envelope)

# load libraries
library(data.table)
library(ggplot2)

date <- gsub(" |:","-",Sys.time())

#######
# bring in the run IDs:
run_slt <- 'RUNID' # all smokeless tobacco ST-GPR model run ID
run_chew <- 'RUNID' # chewing tobacco ST-GPR model run ID
run_non_chew <- 'RUNID' # non-chewing tobacco ST-GPR model run ID

# read in the file with the ratios and their variances
input_dir <- 'FILEPATH'
# the files are per location, so they must be combined
files <- list.files(input_dir,full.names = T)
files <- lapply(files,fread)
all_ratios <- rbindlist(files)
all_ratios$V1 <- NULL
# change name to clarify that this is standard deviation
setnames(all_ratios, old="se_ratio",new="sd_ratio")

# then load the final estimates and input data:
# the model_load() function is a central function
# first, for the all smokeless tobacco model/data
slt_final <- model_load(run_slt,obj="raked") # final estimated from ST-GPR
slt_data <- model_load(run_slt,obj="data") # data that went into the model (i.e. post age and sex splitting)
slt_final$model <- "slt"
slt_data$model <- "slt"

# second, for chewing tobacco
chew_final <- model_load(run_chew,obj="raked")
chew_data <- model_load(run_chew,obj="data")
chew_final$model <- "chew"
chew_data$model <- "chew"

# third, for non-chewing tobacco
non_chew_final <- model_load(run_non_chew,obj="raked")
non_chew_data <- model_load(run_non_chew,obj="data")
non_chew_final$model <- "non_chew"
non_chew_data$model <- "non_chew"

### within each location, squeeze the data under the slt envelope:
all_slt_final <- rbind(slt_final,chew_final,non_chew_final)
all_slt_data <- rbind(slt_data,chew_data,non_chew_data)

# then multiply this chew ratio by slt to generate new chewing points:
slt_only <- all_slt_data[model == "slt"]
slt_only <- merge(slt_only,all_ratios,by=c("location_id","year_id","sex_id","age_group_id"))
slt_only[,data := data * mean_ratio] # adjustment step
slt_only[,variance := variance + (sd_ratio*sd_ratio)] # increase variance for adjusted data points
slt_only[,model := "slt_adjusted"] # re-name the model
slt_only[,c("mean_ratio","sd_ratio") := NULL]

# combine the adjusted all SLT data and the chewing tobacco data. This is the dataset that will be used in the final model.
all_slt <- rbind(slt_only,chew_data)

# take out the smokeless tobacco point if a chewing tobacco point is already present
all_slt[,count := .N, by=c("location_id","nid","year_id","age_group_id","year_id","sex_id")]
all_slt[count == 2 & model == "slt_adjusted", data := NA]
all_slt <- all_slt[!is.na(data)]
all_slt$count <- NULL

# write final dataset that goes into ST-GPR
write.csv(all_slt,'FILEPATH')
