#Script to split CSMR in to acute and chronic proportions
# .libPaths("/FILEPATH/")
#source("/FILEPATH/")

#library(R.methodsS3, lib='/FILEPATH')
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_outputs.R")
source("/FILEPATH/interpolate.R")
source("/FILEPATH/get_population.R")
#library(R.oo, lib='/FILEPATH/')
#library(R.utils, lib='/FILEPATH/')


#suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))


args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]
print(parameters_filepath)
date <- args[2]
decomp_step <- args[3]
#print(decomp_step)

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)

parameters <- fread(parameters_filepath) #for reading table data. 

location <- parameters[task_id, locs] 
print(paste0("Spliting CSMR for location ", location))

#Directories
#Set working directory
folder <- "/FILEPATH/"
dir.create(folder, showWarnings = FALSE)
outdir <- paste0(folder, "iterative", "/") 
dir.create(outdir, showWarnings = FALSE)
setwd(outdir)

#Vectors
me_id <- 2570
cause_id <- 493
ages <- c(8:20, 30, 31, 32, 235)
years <- c(seq(1990,2015,5),2017) #c(seq(1990,2019,1))#c(seq(1990,2015,5),2017) #c(1990,2000,2017) #c(seq(1990, 2010, 5), 2017) 
sex_ids <- c(1,2)

####Load population file

pop <- readRDS(paste0("/FILEPATH/pop_", date,".rds"))

####Pull draws and make calculations
#ME draws - pull global only MI to IHD ratio draws.
me.draws <- data.frame(get_draws("modelable_entity_id", me_id, location_id=1, year_id=years, source="epi",
                                  measure_id=18, age_group_id=ages, status="best", decomp_step='iterative', gbd_round_id=7, downsample = T, n_draws = 1000))
names(me.draws) <- gsub("draw_", "me.draw_", names(me.draws))
me.draws$location_id <- NULL

#CoDCorrect draws - pull location-specific information
cod.draws <- data.frame(get_draws("cause_id", cause_id, location_id=location, year_id=years, source="codcorrect",
                                 age_group_id=ages,version_id = 186, measure_id=1, gbd_round_id = 7, decomp_step='step2',downsample=T, n_draws=1000))
names(cod.draws) <- gsub("draw_", "cod.draw_", names(cod.draws))

#Merge and generate acute CSMR .
acute <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
acute[,c(grep("cod.draw_", names(acute)))] <- lapply(acute[,c(grep("cod.draw_", names(acute)))], function(x, y) x/y, y=acute$population) #transforms deaths from number to rate space

acute <- merge(acute, me.draws, by=c("year_id", "sex_id", "age_group_id"), all.x=T)

#STEP 1 Take ratio and multiply by csmr
acute[,c(grep("me.draw_", names(acute)))] <- acute[,c(grep("cod.draw_", names(acute)))] * acute[,c(grep("me.draw_", names(acute)))]      #multiply ratio by rate-space deaths


acute$mean <- rowMeans(acute[,c(grep("me.draw_", names(acute)))])
acute$sample_size <- rowMeans(acute[,c(grep("cod.draw_", names(acute)))])

#Merge and generate chronic CSMR
#STEP2
chronic <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
#STEP3
#chronic[,c(grep("cod.draw_", names(chronic)))] <- lapply(chronic[,c(grep("cod.draw_", names(chronic)))], function(x,y) x/y, y=chronic$population) #transforms deaths from number to rate-space (already done)
chronic <- merge(chronic, me.draws, by=c("year_id", "sex_id", "age_group_id"), all.x=T)
#STEP4
chronic[,c(grep("me.draw_", names(chronic)))] <- lapply(chronic[,c(grep("me.draw_", names(chronic)))], function(x) 1-x) # generates IHD:MI (inverse) ratio
#STEP5
chronic[,c(grep("me.draw_", names(chronic)))] <- chronic[,c(grep("cod.draw_", names(chronic)))] * chronic[,c(grep("me.draw_", names(chronic)))]
#STEP6
chronic[,c(grep("cod.draw_", names(chronic)))] <- lapply(chronic[,c(grep("cod.draw_", names(chronic)))], function(x,y) x*y, y=chronic$population)

chronic$mean <- rowMeans(chronic[,c(grep("me.draw_", names(chronic)))])
chronic$sample_size <- rowMeans(chronic[,c(grep("cod.draw_", names(chronic)))])

#store_original_results <- data.frame(acute[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")],chronic[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")])
#store_new_results <- data.frame(acute[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")],chronic[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")])
#Save rds files
saveRDS(acute[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")], file=paste0(outdir, "acute_", location, ".rds"))
saveRDS(chronic[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")], file=paste0(outdir, "chronic_", location, ".rds"))



