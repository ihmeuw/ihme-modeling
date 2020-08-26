############################################################################
## Intimate Partner Violence, HIV PAF calculation, save_results script
## AUTHOR: 
## DATE: June 2018
## PROJECT: Intimate partner violence, GBD risk factors
## PURPOSE: save paf results to the database
## NOTE: only works on the cluster
############################################################################

## get arguments
print(commandArgs())
print(paste("4 ", commandArgs()[4]))
print(paste("5 ", commandArgs()[5]))
print(paste("6 ", commandArgs()[6]))
print(paste("7 ", commandArgs()[7]))
print(paste("8 ", commandArgs()[8]))
print(paste("9 ", commandArgs()[9]))
print(paste("10 ", commandArgs()[10]))
print(paste("11 ", commandArgs()[11]))
spectrum.name <- commandArgs()[8]
main_dir <- commandArgs()[9]
ver <- commandArgs()[10]
decomp <- 'step4'
date <- "2019_10_22"
ver <- "2019_10_22"

print(paste(spectrum.name,main_dir,ver," "))

# source shared functions
shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_location_metadata.R'))
source(paste0(shared_functions_dir,'/get_best_model_versions.R'))
source(paste0(shared_functions_dir,'/save_results_risk.R'))

# create description
ipv_mvid <- get_best_model_versions(entity='modelable_entity', ids=2452, status='best',decomp_step=decomp)$model_version_id
description <- paste('HIV model', spectrum.name,'& IPV exposure model',ipv_mvid, "& new hiv due to sex (24812)",sep=" ")

# check locations all present
loc_table <- get_location_metadata(35, decomp_step = 'step4', gbd_round_id = 6)
loc_table <- loc_table[most_detailed==1,]
locations <- unique(loc_table$location_id)
missing <- c()
files <- list.files(paste0(main_dir,'/paf_draws_',ver),full.names=F)
for(loc in locations){
  if(!paste0(loc,"_2.csv")%in%files) missing <- c(missing, loc)
}
print("missing these locations:")
print(missing)

# save results
save_results_risk(
  modelable_entity_id=8824,
  description=description,
  input_dir=paste0(main_dir,'/paf_draws_',ver),
  input_file_pattern="{location_id}_{sex_id}.csv",
  risk_type='paf',
  mark_best=TRUE,
  sex_id=2,
  year_id=c(1980:2019),
  decomp_step=decomp
)
