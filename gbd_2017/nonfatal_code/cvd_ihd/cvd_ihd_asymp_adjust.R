#Script to adjust results of Post-MI IHD model to avoid double-counting angina and HF
.libPaths("FILEPATH")
library(R.utils)

suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))

demographics <- get_demographics(gbd_team="epi")
ages <- unlist(demographics$age_group_id, use.names=F)

#Arguments
argue <- commandArgs(trailingOnly = T)
location <- as.numeric(argue[1])

#Directories
outdir <- "FILEPATH"
filedir <- paste0(jpath, "FILEPATH")

#Load file with angina proportions
angina <- read.csv(paste0(filedir, "angina_prop_postMI.csv"), header=T, stringsAsFactors=F)
angina$angina_prop <- with(angina, ifelse(is.na(angina_prop)==T, 0, angina_prop))
angina <- unique(angina[,c("age_group_id", "angina_prop")])

#Load draws from Post-MI dismod model
post.mi <- data.frame(get_draws("modelable_entity_id", 15755, location_id=location, source="epi", status="best", measure_id=5, age_group_id=ages))
names(post.mi) <- gsub("draw_", "ihd.draw_", names(post.mi))

#Load draws from heart failure model
hf <- data.frame(get_draws("modelable_entity_id", 9567, location_id=location, source="epi", status="best", measure_id=5, age_group_id=ages))
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
