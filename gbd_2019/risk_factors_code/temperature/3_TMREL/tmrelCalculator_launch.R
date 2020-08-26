#clear memory
rm(list=ls())

library("data.table")
library("dplyr")
library("psych")
library("ggplot2")

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")


#define args:
tmrel_min <- 6.6
tmrel_max <- 34.6

user <- USERNAME


# set up directories
indir <- FILEPATH
outdir <- FILEPATH
dir.create(outdir, recursive = T)



### GET LIST OF INCLUDED CAUSES ###
causeList  <- c("inj_drowning", "lri", "cvd_htn","inj_suicide","cvd_stroke","cvd_ihd","resp_copd","ckd","inj_homicide","inj_mech","inj_trans_road","diabetes")


### GET CAUSE AND LOCATION META-DATA ###
causeMeta <- get_cause_metadata(cause_set_id=4, gbd_round_id=6)
causeMeta <- causeMeta %>% filter(acause %in% causeList & level==3) %>% dplyr::select(cause_id, cause_name, acause)

cause_list <- paste(causeMeta$cause_id, collapse = ",")
acause_list <- paste(causeMeta$acause, collapse = ",")


locMeta <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level<=3, location_id]
loc_ids <- loc_ids[order(loc_ids)]



# set up run environment
project <- "proj_paf_temp"
sge.output.dir <- paste0(" -o FILEPATH", user, "/output -e FILEPATH", user, "/errors ")
r.shell <- FILEPATH
mem <- "150G"
slots <- 8
save.script <- paste0(FILEPATH, "/tmrelCalculator.R")




for (loc_id in loc_ids) {

  args <- paste(loc_id, cause_list, acause_list, outdir, indir, tmrel_min, tmrel_max)
  jname <- paste("tmrel", loc_id, sep = "_")
  print(paste(jname))

  if (file.exists(paste0(outdir, "/tmrel_", loc_id, "_summaries.csv"))==F) {

    # Create submission call
    sys.sub <- paste0("qsub -l archive -l m_mem_free=", mem, " -l fthread=", slots, " -P ", project, " -q all.q", sge.output.dir, " -N ", jname)

    # Run
    system(paste(sys.sub, r.shell, save.script, args))
    Sys.sleep(0.01)
  }
}


missing <- loc_ids
pause <- 10

while (length(missing)>0) {
  for (loc_id in missing) {
    if (file.exists(paste0(outdir, "/tmrel_", loc_id, "_summaries.csv"))==T) missing <- setdiff(missing, loc_id)
  }

  if (length(missing)>0) {
    print(paste0(length(missing), " locations incomplete. Will check again in ", pause, " minutes."))
    for (t in 1:pause) {
      cat(".")
      Sys.sleep(60)
    }
  }
}



cat("All locations complete.")


