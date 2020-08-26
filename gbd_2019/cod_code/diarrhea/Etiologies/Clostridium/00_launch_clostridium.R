####################################################################################
## This file launches Clostridium difficile PAF calculation, parallel by location_id ##
####################################################################################
source("/filepath/get_location_metadata.R")

locations <- get_location_metadata(location_set_id=9)

###############################################
# loop over locations, submit a job
for(l in locations) {
  args <- paste(l)
  # store qsub command
  qsub = paste0('qsub -e /filepath/errors/ -o /filepath/output/ -cwd -N cdifficile_paf_', l, ' -P ihme_general ',
                ' -l fthread=1 -l m_mem_free=1G -q all.q -l archive=TRUE ',
                '/filepath/r_shell.sh /filepath/01_clostridium_codcorrect.R ', args)

  # submit job
  system(qsub)
}
