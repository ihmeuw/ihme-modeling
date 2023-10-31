#Script to adjust results of Post-MI IHD model to avoid double-counting angina and HF
# rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))
# source("/FILEPATH/")

library(R.utils)
jpath <- "/FILEPATH/"
suppressMessages(sourceDirectory(paste0("/FILEPATH/")))
date <-  gsub("-", "_", Sys.Date())
demographics <- get_demographics(gbd_team="epi")
ages <- unlist(demographics$age_group_id, use.names=F)

#Arguments
# argue <- commandArgs(trailingOnly = T)
args <- commandArgs(trailingOnly = TRUE)
parameters_filepath <- args[1]
decomp_step <- args[2]

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)

parameters <- fread(parameters_filepath)

location <- parameters[task_id, location_id]
print(location)
# location <- as.numeric(argue[1])
#decomp_step <- as.character(argue[2])

#Directories
outdir <- paste0("/FILEPATH/", decomp_step,"_", date,"/")
dir.create(file.path(outdir), showWarnings = FALSE)

filedir <- paste0(jpath, "FILEPATH/")

#Load file with angina proportions
angina <- read.csv(paste0(filedir, "angina_prop_postMI_gbd2020.csv"), header=T, stringsAsFactors=F)
angina$angina_prop <- with(angina, ifelse(is.na(angina_prop)==T, 0, angina_prop))
angina <- unique(angina[,c("age_group_id", "angina_prop")])

#Load draws from Post-MI dismod model
post.mi <- data.frame(get_draws("modelable_entity_id", 15755, location_id=location, source="epi", status="best", measure_id=5, age_group_id=ages, gbd_round_id=7, decomp_step='iterative'))
names(post.mi) <- gsub("draw_", "ihd.draw_", names(post.mi))

#Load draws from heart failure model
hf <- data.frame(get_draws("modelable_entity_id", 9567, location_id=location, source="epi", status="best", measure_id=5, age_group_id=ages, gbd_round_id=7, decomp_step='iterative'))
names(hf) <- gsub("draw_", "hf.draw_", names(hf))

#Merge and adjust to remove angina and heart failure
post.mi <- merge(post.mi, angina, by="age_group_id", all.x=T)
post.mi[,c(grep("ihd.draw_", names(post.mi)))] <- lapply(post.mi[,c(grep("ihd.draw_", names(post.mi)))], function(x, y) x-(x*y), y=post.mi$angina_prop)
post.mi <- merge(post.mi, hf, by=c("location_id", "age_group_id", "sex_id", "year_id"))
post.mi[,c(grep("ihd.draw_", names(post.mi)))] <- post.mi[,c(grep("ihd.draw_", names(post.mi)))] - post.mi[,c(grep("hf.draw_", names(post.mi)))]
post.mi[,c(grep("ihd.draw_", names(post.mi)))] <- lapply(post.mi[,c(grep("ihd.draw_", names(post.mi)))], function(x) (abs(x)+x)/2)

#Subset to necessary variables for save_results_epi
post.mi <- post.mi[,c("age_group_id", "sex_id", "location_id", "year_id", grep("ihd.draw_", names(post.mi), value=T))]
names(post.mi) <- gsub("ihd.draw_", "draw_", names(post.mi))
write.csv(post.mi, paste0(outdir, location, ".csv"), row.names=F)
