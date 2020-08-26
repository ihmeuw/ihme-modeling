####################################################################################
## This file launches rotavirus burden calculation, parallel by location_id       ##
####################################################################################
library(plyr)
source("FILEPATH")

locations <- get_location_metadata(location_set_id=9)
locations <- locations$location_id[locations$is_estimate==1]

###############################################
# loop over locations, submit a job
## Right now, the PAF calculation performs the
## script for each etiology

for(l in locations) {
  args <- paste(l)
  # store qsub command
  qsub = paste0('qsub -e ADDRESS -o ADDRESS -cwd -N rv_burden_', l, ' -P ihme_general ',
                # new arguments
                '-l fthread=1 -l m_mem_free=1G -q all.q -l archive=TRUE ',
                ' FILEPATH FILEPATH ', # Change this line of code if you move the code elsewhere.
                args)
  
  # submit job
  system(qsub)
  print(paste("Submitted job", which(locations==l), "of",length(locations)))
}
