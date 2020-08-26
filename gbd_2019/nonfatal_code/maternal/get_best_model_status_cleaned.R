### This script pulls best model information, used in order to verify all mes were uploaded to properly
  ### manually change all of the me_ids to match the full set of expected uploads
  ### don't know if there is an automated way to do this -- as the uploads are specific to researcher and core code

### set environment
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('rstudioapi')

source(paste0(j, "FILEPATH/get_best_model_versions.R"))

code_dir <- dirname(rstudioapi::getSourceEditorContext()$path)


me_ids <- c(16535,	3644,	3620,	3629,	3635,	3641,	1553,	3623,	3626,	10485,	1536,	2625,	2627,	1554,	2624,	1537,	1542,	1546)

final <- data.frame()
for(me in me_ids){
  print(paste0("pulling ", me))
  data <- get_best_model_versions(entity = 'modelable_entity', 
                                  ids = me, 
                                  gbd_round_id = 6,
                                  decomp_step = 'step1')
  !
  datalist <- list(final, data)
  final <- rbindlist(datalist, use.names = TRUE, fill = TRUE, idcol = FALSE)
}

write.csv(final, paste0(h, "FILEPATH/all_best_models.csv"), row.names = FALSE)
