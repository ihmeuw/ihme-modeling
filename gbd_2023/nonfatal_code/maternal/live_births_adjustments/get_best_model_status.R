### This script pulls best model information, used in order to verify all mes were uploaded to properly

library('rstudioapi')
source(FILEPATH)

code_dir <- dirname(rstudioapi::getSourceEditorContext()$path)

me_ids <- c(16535,	3644,	3620,	3629,	3635,	3641,	1553,	3623,	3626,	10485,	1536,	2625,	2627,	1554,	2624,	1537,	1542,	1546)

final <- data.frame()
for(me in me_ids){
  print(paste0("pulling ", me))
  data <- get_best_model_versions(entity = 'modelable_entity', 
                                  ids = me, 
                                  release_id = release_id)
  !
    datalist <- list(final, data)
  final <- rbindlist(datalist, use.names = TRUE, fill = TRUE, idcol = FALSE)
}

write.csv(final, paste0(FILEPATH), row.names = FALSE)
