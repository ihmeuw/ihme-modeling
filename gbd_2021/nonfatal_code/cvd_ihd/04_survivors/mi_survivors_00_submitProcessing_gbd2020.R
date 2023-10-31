#set variables for parallelization
rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))


source("/FILEPATH/")
library(R.methodsS3, lib='/FILEPATH/')
source('/FILEPATH/get_demographics.R')
source('/FILEPATH/get_population.R')
source('/FILEPATH/get_ids.R')
date <- gsub("-", "_", Sys.Date())

suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))

share_path <- "/FILEPATH/"
code.dir <- paste0(share_path, "FILEPATH/")
scriptname <- paste0("mi_survivors_01_processing_2019_10_04.R")
decomp_step = "step4"


if (decomp_step=="step1"){
  demographics <- get_demographics(gbd_team="epi", gbd_round_id=6) # because CoDCorrect deaths not yet available for round 6
} else {
  demographics <- get_demographics(gbd_team="epi", gbd_round_id=7)
}
locations <- unlist(demographics$location_id, use.names=F)



loc_meta <- expand.grid(locs=locations)

write.csv(loc_meta, file = '/FILEPATH/loc_meta.csv')

#Pull population already ran - no need to re-run
# pop <- data.frame(get_population(age_group_id=ages, location_id=locations, year_id=years, sex_id=c(1,2),
#                                  status="best", decomp_step=decomp_step))
# saveRDS(pop, file=paste0("/FILEPATH/pop_", date,".rds"))
# eh <- readRDS("/FILEPATH/pop_DATE.rds")

threads <- 3
memory <- "500M"
time <- "00:12:00" 
location_filepath <- '/FILEPATH/loc_meta.csv'
njobs <- nrow(loc_meta)

args <- paste(location_filepath)#paste(locations[i])
rscript <- paste0("-s ", code.dir, paste0("mi_survivors_01_processing_2019_10_04.R")) #input date 
rshell <- "/FILEPATH" 

errors <- " -o /FILEPATH/ -e /FILEPATH/"

sys.sub <- paste0("qsub -P proj_cvd ", errors , " -cwd -N surv -l fthread=", threads,
                  " -l m_mem_free=", memory, " -l h_rt=", time, " -q all.q -l archive=TRUE", ' -t 1:',njobs )
command <- paste(sys.sub, rshell, rscript, args, decomp_step)
print(command)

system(command)

