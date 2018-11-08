#Script to split CSMR in to acute and chronic proportions
.libPaths("FILEPATH")
library(R.utils)

suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))


#Arguments
argue <- commandArgs(trailingOnly = T)
location <- as.numeric(argue[1])
date <- argue[2]
#date <- gsub("-", "_", Sys.Date())

#Directories
outdir <- "FILEPATH"

#Vectors
me_id <- 2570
cause_id <- 493
ages <- c(8:20, 30, 31, 32, 235)
years <- c(seq(1990, 2010, 5), 2017)
version <- 86

#Location information, if want to switch away from using the global
locs <- data.frame(get_location_metadata(location_set_id=35))
regions <- unlist(subset(locs, location_type=="region", select="location_id"), use.names=F)
superregions <- unlist(subset(locs, location_type=="superregion", select="location_id"), use.names=F)

####Load population file 
#Population
pop <- readRDS(paste0("FILEPATH/pop_", date,".rds"))

####Pull draws and make calculations
#ME draws - pull global only
me.draws <- data.frame(get_draws("modelable_entity_id", me_id, location_id=1, year_id=years, source="epi", measure_id=18, age_group_id=ages, status="best"))
names(me.draws) <- gsub("draw_", "me.draw_", names(me.draws))
me.draws$location_id <- NULL

#CoDCorrect draws - pull location-specific information
cod.draws <- data.frame(get_draws("cause_id", cause_id, location_id=location, year_id=years, source="codcorrect", age_group_id=ages, version_id=version, measure_id=1))
names(cod.draws) <- gsub("draw_", "cod.draw_", names(cod.draws))

#Merge and generate acute CSMR
acute <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
acute[,c(grep("cod.draw_", names(acute)))] <- lapply(acute[,c(grep("cod.draw_", names(acute)))], function(x, y) x/y, y=acute$population) #transforms deaths from number to rate space

acute <- merge(acute, me.draws, by=c("year_id", "sex_id", "age_group_id"), all.x=T)

acute[,c(grep("me.draw_", names(acute)))] <- acute[,c(grep("cod.draw_", names(acute)))] * acute[,c(grep("me.draw_", names(acute)))] #multiply ratio by rate-space deaths
acute[,c(grep("cod.draw_", names(acute)))] <- lapply(acute[,c(grep("cod.draw_", names(acute)))], function(x, y) x*y, y=acute$population) #transforms deaths from number to rate space

acute$mean <- rowMeans(acute[,c(grep("me.draw_", names(acute)))]) 
acute$sample_size <- rowMeans(acute[,c(grep("cod.draw_", names(acute)))])

#Merge and generate chronic CSMR
chronic <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
chronic[,c(grep("cod.draw_", names(chronic)))] <- lapply(chronic[,c(grep("cod.draw_", names(chronic)))], function(x,y) x/y, y=chronic$population)

chronic <- merge(chronic, me.draws, by=c("year_id", "sex_id", "age_group_id"), all.x=T)
chronic[,c(grep("me.draw_", names(chronic)))] <- lapply(chronic[,c(grep("me.draw_", names(chronic)))], function(x) 1-x) # generates IHD:MI (inverse) ratio
chronic[,c(grep("me.draw_", names(chronic)))] <- chronic[,c(grep("cod.draw_", names(chronic)))] * chronic[,c(grep("me.draw_", names(chronic)))]
chronic[,c(grep("cod.draw_", names(chronic)))] <- lapply(chronic[,c(grep("cod.draw_", names(chronic)))], function(x,y) x*y, y=chronic$population)

chronic$mean <- rowMeans(chronic[,c(grep("me.draw_", names(chronic)))])
chronic$sample_size <- rowMeans(chronic[,c(grep("cod.draw_", names(chronic)))])

#Save rds files
saveRDS(acute[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")], file=paste0(outdir, "acute_", location, ".rds"))
saveRDS(chronic[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "sample_size")], file=paste0(outdir, "chronic_", location, ".rds"))


