####################################################################################
## This file launches diarrhea etiology PAF calculation, parallel by location_id ##
####################################################################################
source("/filepath/get_location_metadata.R")

locations <- get_location_metadata(location_set_id=9)
locations <- locations$location_id[locations$is_estimate==1]

# Creates a draw matrix of the results from MR-BRT (in logit space) #
  eti_info <- read.csv("filepath")

  scalar <- read.csv("filepath")
  scalar <- subset(scalar, crosswalk_name=="cv_inpatient")
  scalar <- scalar[, c("Y_mean","Y_mean_lo","Y_mean_hi","etiology","rei_name")]
  scalar$se <- (scalar$Y_mean - scalar$Y_mean_lo) / qnorm(0.975)
  scalar <- join(scalar, eti_info[,c("rei_name","modelable_entity_id","rei_id")], by="rei_name")

  for(i in 0:999){
    draw <- rnorm(n = length(scalar$rei_id), mean = scalar$Y_mean, sd = scalar$se)
    scalar[,paste0("scalar_",i)] <- draw
  }
  write.csv(scalar, "filepath", row.names=F)

###############################################
# loop over locations, submit a job
## the PAF calculation performs the script for each etiology
for(l in locations) {
  args <- paste(l)
  # store qsub command
  qsub = paste0('qsub -e /filepath/errors/ -o /filepath/output/ -cwd -N diarrhea_paf_', l, ' -P ihme_general ',
                # new arguments
                '-l fthread=1 -l m_mem_free=1G -q all.q -l archive=TRUE ',
                # this has a different shell script required to use the "msm" package, pay attention to the "commandArgs()[]"
                ' /filepath/r_shell_singularity.sh /filepath/paf_calculation_2019_rotavirus.R ',
                args)

  # submit job
  system(qsub)
}
